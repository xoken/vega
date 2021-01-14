{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.MessageHandler where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, concurrently_)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.Trans.Control
import Data.Function ((&))
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import Network.Socket as NS
import Network.Xoken.Block
import Network.Xoken.Constants
import Network.Xoken.Network.Common
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Network.Message
import Network.Xoken.Node.DB
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.MessageReader
import Network.Xoken.Node.P2P.MessageSender
import Network.Xoken.Node.P2P.Process.Block
import Network.Xoken.Node.P2P.Process.Headers
import Network.Xoken.Node.P2P.Process.Tx
import Network.Xoken.Node.P2P.Socket
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.Version
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Transaction
import Streamly as S
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Random
import Xoken.NodeConfig as NC

handleIncomingMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
handleIncomingMessages pr = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ msg $ "reading from: " ++ show (bpAddress pr)
    rlk <- liftIO $ newMVar Nothing
    res <-
        LE.try $
        LA.concurrently_
            (peerBlockSync pr) -- issue GetData msgs
            (S.drain $
             aheadly $
             S.repeatM (readNextMessage' pr rlk) & -- read next msgs
             S.mapM (messageHandler pr) & -- handle read msgs
             S.mapM (logMessage pr) & -- log msgs & collect stats
             S.maxBuffer 2 &
             S.maxThreads 2)
    case res of
        Right a -> return ()
        Left (e :: SomeException) -> do
            err lg $ msg $ (val "[ERROR] Closing peer connection ") +++ (show e)
            case (bpSocket pr) of
                Just sock -> liftIO $ NS.close sock
                Nothing -> return ()
            liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
            return ()

logMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> MessageCommand -> m (Bool)
logMessage peer mg = do
    lg <- getLogger
    -- liftIO $ atomically $ modifyTVar' (bpIngressMsgCount peer) (\z -> z + 1)
    debug lg $ LG.msg $ "DONE! processed: " ++ show mg
    return (True)

messageHandler ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => BitcoinPeer
    -> (Maybe Message, Maybe IngressStreamState)
    -> m (MessageCommand)
messageHandler peer (mm, ingss) = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    db <- getDB
    case mm of
        Just msg -> do
            liftIO $ print $ "MSG: " ++ (show $ msgType msg)
            case msg of
                MHeaders hdrs -> do
                    liftIO $ takeMVar (headersWriteLock bp2pEnv)
                    res <- LE.try $ processHeaders hdrs
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left InvalidBlocksException -> do
                            err lg $ LG.msg $ (val "[ERROR] Closing peer connection, Checkpoint verification failed")
                            case (bpSocket peer) of
                                Just sock -> liftIO $ NS.close sock
                                Nothing -> return ()
                            liftIO $
                                atomically $ do
                                    modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                    modifyTVar' (blacklistedPeers bp2pEnv) (M.insert (bpAddress peer) peer)
                            liftIO $ putMVar (headersWriteLock bp2pEnv) True
                            throw InvalidBlocksException
                        Left KeyValueDBInsertException -> do
                            err lg $ LG.msg $ LG.val ("[ERROR] Insert failed. KeyValueDBInsertException")
                            liftIO $ putMVar (headersWriteLock bp2pEnv) True
                            throw KeyValueDBInsertException
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            liftIO $ putMVar (headersWriteLock bp2pEnv) True
                            throw e
                    liftIO $ putMVar (headersWriteLock bp2pEnv) True
                    return $ msgType msg
                MInv inv -> do
                    mapM_
                        (\x
                             --liftIO $ print $ "INVTYPE: " ++ (show $ invType x)
                          -> do
                             case (invType x) of
                                 InvBlock -> do
                                     let bhash = invHash x
                                     debug lg $ LG.msg ("INV - new Block: " ++ (show bhash))
                                     unsynced <- blocksUnsynced
                                     if unsynced <= (3 :: Int32)
                                         then do
                                             newCandidateBlock $ BlockHash bhash
                                             processCompactBlockGetData peer $ invHash x
                                         else liftIO $ putMVar (bestBlockUpdated bp2pEnv) True -- will trigger a GetHeaders to peers
                                 InvTx -> do
                                     indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
                                     debug lg $ LG.msg ("INV - new Tx: " ++ (show $ TxHash $ invHash x))
                                     if indexUnconfirmedTx == True
                                         then do
                                             debug lg $
                                                 LG.msg
                                                     ("[dag] InvTx (indexUnconfirmedTx True): " ++
                                                      (show $ TxHash $ invHash x))
                                             processTxGetData peer $ invHash x
                                         else do
                                             debug lg $
                                                 LG.msg
                                                     ("[dag] InvTx (indexUnconfirmedTx False): " ++
                                                      (show $ TxHash $ invHash x))
                                             return ()
                                 InvCompactBlock -> do
                                     let bhash = invHash x
                                     debug lg $ LG.msg ("INV - Compact Block: " ++ (show bhash))
                                     newCandidateBlock $ BlockHash bhash
                                     processCompactBlockGetData peer $ invHash x
                                 otherwise -> return ())
                        (invList inv)
                    return $ msgType msg
                MAddr addrs -> do
                    mapM_
                        (\(t, x) -> do
                             bp <- setupPeerConnection $ naAddress x
                             LA.async $
                                 (case bp of
                                      Just p -> handleIncomingMessages p
                                      Nothing -> return ()))
                        (addrList addrs)
                    return $ msgType msg
                MConfTx txns -> do
                    case ingss of
                        Just iss -> do
                            debug lg $ LG.msg $ ("Processing Tx-batch, size :" ++ (show $ L.length txns))
                            processTxBatch txns iss
                        Nothing -> do
                            err lg $ LG.msg $ val ("[???] Unconfirmed Tx ")
                    return $ msgType msg
                MTx tx -> do
                    debug lg $ LG.msg $ "[dag] Processing Unconf Tx (MTx)" ++ show (txHash tx)
                    res <- LE.try $ zRPCDispatchUnconfirmedTxValidate processUnconfTransaction tx
                    case res of
                        Right (depTxHashes) -> do
                            candBlks <- liftIO $ TSH.toList (candidateBlocks bp2pEnv)
                            let candBlkHashes = fmap fst candBlks
                            addTxCandidateBlocks (txHash tx) candBlkHashes depTxHashes
                        Left TxIDNotFoundException -> do
                            return ()
                            --throw TxIDNotFoundException
                        Left KeyValueDBInsertException -> do
                            err lg $ LG.msg $ val "[ERROR] KeyValueDBInsertException"
                            throw KeyValueDBInsertException
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            throw e
                    return $ msgType msg
                MBlock blk
                    -- debug lg $ LG.msg $ LG.val ("DEBUG receiving block ")
                 -> do
                    res <- LE.try $ processBlock blk
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            throw e
                    return $ msgType msg
                MCompactBlock cmpct -> do
                    res <- LE.try $ processCompactBlock cmpct peer
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            throw e
                    return $ msgType msg
                MBlockTxns blockTxns -> do
                    res <- LE.try $ processBlockTransactions blockTxns
                    case res of
                        Right () -> return ()
                        Left BlockHashNotFoundException -> return ()
                        Left EmptyHeadersMessageException -> return ()
                        Left e -> do
                            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
                            throw e
                    return $ msgType msg
                MPing ping -> do
                    bp2pEnv <- getBitcoinP2P
                    let net = bitcoinNetwork $ nodeConfig bp2pEnv
                    case bpSocket peer of
                        Just sock -> do
                            liftIO $ encodeAndSendMessage (bpWriteMsgLock peer) sock net (MPong $ Pong (pingNonce ping))
                            return $ msgType msg
                        Nothing -> return $ msgType msg
                MGetData (GetData gd) -> do
                    mapM_
                        (\(InvVector invt invh) -> do
                             debug lg $ LG.msg $ "recieved getdata: " ++ show (invt, invh)
                             case invt of
                                 InvBlock -> do
                                     cmptblkm <- liftIO $ TSH.lookup (compactBlocks bp2pEnv) (BlockHash invh)
                                     case cmptblkm of
                                         Just (cmptblk, _) -> do
                                             liftIO $ threadDelay (1000000 * 5)
                                             debug lg $ LG.msg $ "sent CompactBlock: " ++ (show $ BlockHash invh)
                                             sendCmptBlock cmptblk peer
                                         Nothing -> return ()
                                 InvCompactBlock -> do
                                     cmptblkm <- liftIO $ TSH.lookup (compactBlocks bp2pEnv) (BlockHash invh)
                                     case cmptblkm of
                                         Just (cmptblk, _) -> do
                                             liftIO $ threadDelay (1000000 * 5)
                                             debug lg $ LG.msg $ "sent CompactBlock: " ++ (show $ BlockHash invh)
                                             sendCmptBlock cmptblk peer
                                         Nothing -> return ()
                                 InvTx -> do
                                     return ())
                        gd
                    return $ msgType msg
                MGetHeaders (GetHeaders ver bl bh)
                    -- TODO: use blocklocator to send more than one header
                 -> do
                    cmptblkm <- liftIO $ TSH.lookup (compactBlocks bp2pEnv) bh
                    let ret =
                            case cmptblkm of
                                Just (cmptblk, _) ->
                                    [ ( cbHeader cmptblk
                                      , VarInt $ fromIntegral $ cbShortIDsLength cmptblk + cbPrefilledTxnLength cmptblk)
                                    ]
                                Nothing -> []
                    sendRequestMessages peer $ MHeaders $ Headers ret
                    return $ msgType msg
                MSendCompact _ -> do
                    liftIO $ writeIORef (bpSendcmpt peer) True
                    return $ msgType msg
                MGetBlockTxns gbt@(GetBlockTxns bh ln bi) -> do
                    cmptblkm <- liftIO $ TSH.lookup (compactBlocks bp2pEnv) bh
                    (btl, bt) <-
                        case cmptblkm of
                            Just (cmptblk, txhs) -> do
                                liftIO $ print $ "MGetBlockTxns: txs:" ++ show txhs ++ " GetBlockTxns:" ++ show gbt
                                return (ln, fmap (\i -> txhs !! (fromIntegral $ i - 1)) bi)
                            Nothing -> do
                                liftIO $
                                    print $ "MGetBlockTxns: candidateBlock doesn't exist; GetBlockTxns:" ++ show gbt
                                return (0, [])
                    txs' <- mapM getTx bt
                    let txs = catMaybes txs'
                        btls = fromIntegral $ L.length txs
                    sendBlockTxn (BlockTxns bh btls txs) peer
                    return $ msgType msg
                _ -> do
                    liftIO $ print $ "Got message: " ++ show msg
                    return $ msgType msg
        Nothing -> do
            err lg $ LG.msg $ val "Error, invalid message"
            throw InvalidMessageTypeException

doVersionHandshake ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> SockAddr
    -> m Bool
doVersionHandshake net sock sa = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    g <- liftIO newStdGen
    now <- round <$> liftIO getPOSIXTime
    let ip = bitcoinNodeListenIP $ nodeConfig p2pEnv
        port = toInteger $ bitcoinNodeListenPort $ nodeConfig p2pEnv
    myaddr <- liftIO $ head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just ip) (Just $ show port)
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
    mv <- liftIO $ newMVar ()
    liftIO $ encodeAndSendMessage mv sock net $ MVersion ver
    (hs1, _) <- readNextMessage net sock Nothing
    case hs1 of
        Just (MVersion __) -> do
            (hs2, _) <- readNextMessage net sock Nothing
            case hs2 of
                Just MVerAck -> do
                    liftIO $ encodeAndSendMessage mv sock net MVerAck
                    debug lg $ msg ("Version handshake complete: " ++ show sa)
                    return True
                __ -> do
                    err lg $ msg $ val "Error, unexpected message (2) during handshake"
                    return False
        __ -> do
            err lg $ msg $ val "Error, unexpected message (1) during handshake"
            return False

peerBlockSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> m ()
peerBlockSync peer =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        trace lg $ LG.msg $ ("peer block sync : " ++ (show peer))
        !tm <- liftIO $ getCurrentTime
        let tracker = statsTracker peer
        fw <- liftIO $ readIORef $ ptBlockFetchWindow tracker
        recvtm <- liftIO $ readIORef $ ptLastTxRecvTime tracker
        sendtm <- liftIO $ readIORef $ ptLastGetDataSent tracker
        let staleTime = fromInteger $ fromIntegral (unresponsivePeerConnTimeoutSecs $ nodeConfig bp2pEnv)
        case recvtm of
            Just rt -> do
                if (fw == 0) && (diffUTCTime tm rt < staleTime)
                    then do
                        msg <- produceGetDataMessage peer
                        res <- LE.try $ sendRequestMessages peer msg
                        case res of
                            Right () -> do
                                debug lg $ LG.msg $ val "updating state."
                                liftIO $ writeIORef (ptLastGetDataSent tracker) $ Just tm
                                liftIO $ modifyIORef' (ptBlockFetchWindow tracker) (\z -> z + 1)
                            Left (e :: SomeException) -> do
                                err lg $ LG.msg ("[ERROR] peerBlockSync " ++ show e)
                                throw e
                    else if (diffUTCTime tm rt > staleTime)
                             then do
                                 debug lg $ LG.msg ("Removing unresponsive peer. (1)" ++ show peer)
                                 case bpSocket peer of
                                     Just sock -> liftIO $ NS.close $ sock
                                     Nothing -> return ()
                                 liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                 throw UnresponsivePeerException
                             else return ()
            Nothing -> do
                case sendtm of
                    Just st -> do
                        if (diffUTCTime tm st > staleTime)
                            then do
                                debug lg $ LG.msg ("Removing unresponsive peer. (2)" ++ show peer)
                                case bpSocket peer of
                                    Just sock -> liftIO $ NS.close $ sock
                                    Nothing -> return ()
                                liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress peer))
                                throw UnresponsivePeerException
                            else return ()
                    Nothing -> do
                        if (fw == 0)
                            then do
                                msg <- produceGetDataMessage peer
                                res <- LE.try $ sendRequestMessages peer msg
                                case res of
                                    Right () -> do
                                        debug lg $ LG.msg $ val "updating state."
                                        liftIO $ writeIORef (ptLastGetDataSent tracker) $ Just tm
                                        liftIO $ modifyIORef' (ptBlockFetchWindow tracker) (\z -> z + 1)
                                    Left (e :: SomeException) -> do
                                        err lg $ LG.msg ("[ERROR] peerBlockSync " ++ show e)
                                        throw e
                            else return ()
        liftIO $ threadDelay (10000) -- 0.01 sec
        return ()

setupPeerConnection :: (HasXokenNodeEnv env m, MonadIO m) => SockAddr -> m (Maybe BitcoinPeer)
setupPeerConnection saddr = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    blockedpr <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
    allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    let connPeers =
            L.foldl'
                (\c x ->
                     if bpConnected (snd x)
                         then c + 1
                         else c)
                0
                (M.toList allpr)
    if connPeers > (maxBitcoinPeerCount $ nodeConfig bp2pEnv)
        then return Nothing
        else do
            let toConn =
                    case M.lookup saddr allpr of
                        Just pr -> not $ bpConnected pr
                        Nothing -> True
                isBlacklisted = M.member saddr blockedpr
            if toConn == False
                then do
                    debug lg $ msg ("Peer already connected, ignoring.. " ++ show saddr)
                    return Nothing
                else if isBlacklisted
                         then do
                             debug lg $ msg ("Peer blacklisted, ignoring.. " ++ show saddr)
                             return Nothing
                         else do
                             res <- LE.try $ liftIO $ createSocketFromSockAddr saddr
                             case res of
                                 Right (sock) -> do
                                     wl <- liftIO $ newMVar ()
                                     {- UNUSED? 
                                     ss <- liftIO $ newTVarIO Nothing
                                     imc <- liftIO $ newTVarIO 0
                                     rc <- liftIO $ newTVarIO Nothing
                                     st <- liftIO $ newTVarIO Nothing
                                     fw <- liftIO $ newTVarIO 0
                                     -}
                                     trk <- liftIO $ getNewTracker
                                     bfq <- liftIO $ newEmptyMVar
                                     sc <- liftIO $ newIORef False
                                     case sock of
                                         Just sx -> do
                                             debug lg $ LG.msg ("Discovered Net-Address: " ++ (show $ saddr))
                                             fl <- doVersionHandshake net sx $ saddr
                                             let bp = BitcoinPeer (saddr) sock wl fl Nothing 99999 trk bfq sc
                                             liftIO $
                                                 atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.insert (saddr) bp)
                                             return $ Just bp
                                         Nothing -> return (Nothing)
                                 Left (SocketConnectException addr) -> do
                                     warn lg $ msg ("SocketConnectException: " ++ show addr)
                                     return Nothing
