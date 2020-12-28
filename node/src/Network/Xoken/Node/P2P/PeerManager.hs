{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.PeerManager
    ( createSocket
    , setupSeedPeerConnection
    , mineBlockFromCandidate
    ) where

import qualified Codec.Serialise as CBOR
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async, cancel, concurrently_, mapConcurrently_, race, wait, waitAnyCatch, withAsync)
import qualified Control.Concurrent.MSem as MS
import qualified Control.Concurrent.MSemN as MSN
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TBQueue
import Control.Concurrent.STM.TQueue
import Control.Concurrent.STM.TSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import Control.Monad.Trans.Control
import Crypto.MAC.SipHash as SH
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Char
import Data.Default
import Data.Function ((&))
import Data.Functor.Identity
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize as DS
import Data.String.Conversions
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.Bolt as BT
import GHC.Natural
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Node.WorkerDispatcher
import Network.Xoken.Node.WorkerListener
import Network.Xoken.Transaction
import Network.Xoken.Util
import StmContainers.Map as SM
import StmContainers.Set as SS
import Streamly as S
import Streamly.Prelude ((|:), drain, each, nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig as NC

createSocket :: AddrInfo -> IO (Maybe Socket)
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO (Maybe Socket)
createSocketWithOptions options addr = do
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    res <- try $ connect sock (addrAddress addr)
    case res of
        Right () -> return $ Just sock
        Left (e :: IOException) -> do
            liftIO $ Network.Socket.close sock
            throw $ SocketConnectException (addrAddress addr)

createSocketFromSockAddr :: SockAddr -> IO (Maybe Socket)
createSocketFromSockAddr saddr = do
    ss <-
        case saddr of
            SockAddrInet pn ha -> socket AF_INET Stream defaultProtocol
            SockAddrInet6 pn6 _ ha6 _ -> socket AF_INET6 Stream defaultProtocol
    rs <- try $ connect ss saddr
    case rs of
        Right () -> return $ Just ss
        Left (e :: IOException) -> do
            liftIO $ Network.Socket.close ss
            throw $ SocketConnectException (saddr)

setupSeedPeerConnection :: (HasXokenNodeEnv env m, MonadIO m) => m ()
setupSeedPeerConnection =
    forever $ do
        bp2pEnv <- getBitcoinP2P
        lg <- getLogger
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
            seeds = getSeeds net
            hints = defaultHints {addrSocketType = Stream}
            port = getDefaultPort net
        debug lg $ msg $ show seeds
        --let sd = map (\x -> Just (x :: HostName)) seeds
        !addrs <- liftIO $ mapConcurrently (\x -> head <$> getAddrInfo (Just hints) (Just x) (Just (show port))) seeds
        mapM_
            (\y -> do
                 debug lg $ msg ("Peer.. " ++ show (addrAddress y))
                 LA.async $
                     (do blockedpr <- liftIO $ readTVarIO (blacklistedPeers bp2pEnv)
                         allpr <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                             -- this can be optimized
                         let connPeers =
                                 L.foldl'
                                     (\c x ->
                                          if bpConnected (snd x)
                                              then c + 1
                                              else c)
                                     0
                                     (M.toList allpr)
                         if connPeers > (maxBitcoinPeerCount $ nodeConfig bp2pEnv)
                             then liftIO $ threadDelay (10 * 1000000)
                             else do
                                 let toConn =
                                         case M.lookup (addrAddress y) allpr of
                                             Just pr ->
                                                 if bpConnected pr
                                                     then False
                                                     else True
                                             Nothing -> True
                                     isBlacklisted = M.member (addrAddress y) blockedpr
                                 if toConn == False
                                     then do
                                         debug lg $
                                             msg ("Seed peer already connected, ignoring.. " ++ show (addrAddress y))
                                     else if isBlacklisted
                                              then do
                                                  debug lg $
                                                      msg ("Seed peer blacklisted, ignoring.. " ++ show (addrAddress y))
                                              else do
                                                  wl <- liftIO $ newMVar ()
                                                  ss <- liftIO $ newTVarIO Nothing
                                                  imc <- liftIO $ newTVarIO 0
                                                  rc <- liftIO $ newTVarIO Nothing
                                                  st <- liftIO $ newTVarIO Nothing
                                                  fw <- liftIO $ newTVarIO 0
                                                  res <- LE.try $ liftIO $ createSocket y
                                                  trk <- liftIO $ getNewTracker
                                                  bfq <- liftIO $ newEmptyMVar
                                                  sc <- liftIO $ newIORef False
                                                  case res of
                                                      Right (sock) -> do
                                                          case sock of
                                                              Just sx -> do
                                                                  fl <- doVersionHandshake net sx $ addrAddress y
                                                                  let bp =
                                                                          BitcoinPeer
                                                                              (addrAddress y)
                                                                              sock
                                                                              wl
                                                                              fl
                                                                              Nothing
                                                                              99999
                                                                              trk
                                                                              bfq
                                                                              sc
                                                                  liftIO $
                                                                      atomically $
                                                                      modifyTVar'
                                                                          (bitcoinPeers bp2pEnv)
                                                                          (M.insert (addrAddress y) bp)
                                                                  liftIO $ print "Sending sendcmpt.."
                                                                  sendcmpt bp
                                                                  handleIncomingMessages bp
                                                              Nothing -> return ()
                                                      Left (SocketConnectException addr) ->
                                                          warn lg $ msg ("SocketConnectException: " ++ show addr)))
            (addrs)
        liftIO $ threadDelay (30 * 1000000)

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
                        Just pr ->
                            if bpConnected pr
                                then False
                                else True
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
                                     ss <- liftIO $ newTVarIO Nothing
                                     imc <- liftIO $ newTVarIO 0
                                     rc <- liftIO $ newTVarIO Nothing
                                     st <- liftIO $ newTVarIO Nothing
                                     fw <- liftIO $ newTVarIO 0
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

resilientRead ::
       (HasLogger m, MonadBaseControl IO m, MonadIO m) => Socket -> BlockIngestState -> m (([Tx], LC.ByteString), Int64)
resilientRead sock !blin = do
    lg <- getLogger
    let chunkSize = 100 * 1000 -- 100 KB
        !delta =
            if binTxPayloadLeft blin > chunkSize
                then chunkSize - ((LC.length $ binUnspentBytes blin))
                else (binTxPayloadLeft blin) - (LC.length $ binUnspentBytes blin)
    trace lg $ msg (" | Tx payload left " ++ show (binTxPayloadLeft blin))
    trace lg $ msg (" | Bytes prev unspent " ++ show (LC.length $ binUnspentBytes blin))
    trace lg $ msg (" | Bytes to read " ++ show delta)
    nbyt <- recvAll sock delta
    let !txbyt = (binUnspentBytes blin) `LC.append` (nbyt)
    case runGetLazyState (getConfirmedTxBatch) txbyt of
        Left e -> do
            trace lg $ msg $ "1st attempt|" ++ show e
            let chunkSizeFB = 10 * 1000 * 1000 -- 10 MB
                !deltaNew =
                    if binTxPayloadLeft blin > chunkSizeFB
                        then chunkSizeFB - ((LC.length $ binUnspentBytes blin) + delta)
                        else (binTxPayloadLeft blin) - ((LC.length $ binUnspentBytes blin) + delta)
            nbyt2 <- recvAll sock deltaNew
            let !txbyt2 = txbyt `LC.append` (nbyt2)
            case runGetLazyState (getConfirmedTxBatch) txbyt2 of
                Left e -> do
                    err lg $ msg $ "2nd attempt|" ++ show e
                    throw ConfirmedTxParseException
                Right res -> do
                    return (res, LC.length txbyt2)
        Right res -> return (res, LC.length txbyt)

readNextMessage ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> Maybe IngressStreamState
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe' <- getDB
    case ingss of
        Just iss -> do
            let blin = issBlockIngest iss
            ((txns, unused), txbytLen) <- resilientRead sock blin
            debug lg $ msg ("Confirmed-Tx: " ++ (show (L.length txns)) ++ " unused: " ++ show (LC.length unused))
            qe <-
                case (issBlockInfo iss) of
                    Just bf ->
                        if (binTxIngested blin == 0) -- very first Tx
                            then do
                                liftIO $ do
                                    vala <- TSH.lookup (blockTxProcessingLeftMap p2pEnv) (biBlockHash $ bf)
                                    case vala of
                                        Just v -> return ()
                                        Nothing -> do
                                            ar <- TSH.new 1
                                            TSH.insert
                                                (blockTxProcessingLeftMap p2pEnv)
                                                (biBlockHash $ bf)
                                                (ar, (binTxTotalCount blin))
                                qq <- liftIO $ atomically $ newTQueue
                                        -- wait for TMT threads alloc
                                liftIO $ MS.wait (maxTMTBuilderThreadLock p2pEnv)
                                liftIO $ TSH.insert (merkleQueueMap p2pEnv) (biBlockHash $ bf) qq
                                -- LA.async $
                                --    merkleTreeBuilder qq (biBlockHash $ bf) (computeTreeHeight $ binTxTotalCount blin)
                                return qq
                            else do
                                valx <- liftIO $ TSH.lookup (merkleQueueMap p2pEnv) (biBlockHash $ bf)
                                case valx of
                                    Just q -> return q
                                    Nothing -> throw MerkleQueueNotFoundException
                    Nothing -> throw MessageParsingException
            let isLastBatch = ((binTxTotalCount blin) == ((L.length txns) + binTxIngested blin))
            let txct = L.length txns
            liftIO $
                atomically $
                mapM_
                    (\(tx, ct) -> do
                         let isLast =
                                 if isLastBatch
                                     then txct == ct
                                     else False
                         writeTQueue qe ((txHash tx), isLast))
                    (zip txns [1 ..])
            let bio =
                    BlockIngestState
                        { binUnspentBytes = unused
                        , binTxPayloadLeft = binTxPayloadLeft blin - (txbytLen - LC.length unused)
                        , binTxTotalCount = binTxTotalCount blin
                        , binTxIngested = (L.length txns) + binTxIngested blin
                        , binBlockSize = binBlockSize blin
                        , binChecksum = binChecksum blin
                        }
            return (Just $ MConfTx txns, Just $ IngressStreamState bio (issBlockInfo iss))
        Nothing -> do
            hdr <- recvAll sock 24
            case (decodeLazy hdr) of
                Left e -> do
                    err lg $ msg ("Error decoding incoming message header: " ++ e)
                    throw MessageParsingException
                Right (MessageHeader headMagic cmd len cks) -> do
                    if headMagic == getNetworkMagic net
                        then do
                            if cmd == MCBlock
                                then do
                                    byts <- recvAll sock (88) -- 80 byte Block header + VarInt (max 8 bytes) Tx count
                                    case runGetLazyState (getDeflatedBlock) (byts) of
                                        Left e -> do
                                            err lg $ msg ("Error, unexpected message header: " ++ e)
                                            throw MessageParsingException
                                        Right (blk, unused) -> do
                                            case blk of
                                                Just b -> do
                                                    trace lg $
                                                        msg
                                                            ("DefBlock: " ++
                                                             show blk ++ " unused: " ++ show (LC.length unused))
                                                    let bi =
                                                            BlockIngestState
                                                                { binUnspentBytes = unused
                                                                , binTxPayloadLeft =
                                                                      fromIntegral (len) - (88 - LC.length unused)
                                                                , binTxTotalCount = fromIntegral $ txnCount b
                                                                , binTxIngested = 0
                                                                , binBlockSize = fromIntegral $ len
                                                                , binChecksum = cks
                                                                }
                                                    return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                                Nothing -> throw DeflatedBlockParseException
                                else do
                                    byts <-
                                        if len == 0
                                            then return hdr
                                            else do
                                                b <- recvAll sock (fromIntegral len)
                                                return (hdr `BSL.append` b)
                                    case runGetLazy (getMessage net) byts of
                                        Left e -> do
                                            debug lg $
                                                msg ("Message parse error' : " ++ (show e) ++ "; cmd: " ++ (show cmd))
                                            throw MessageParsingException
                                        Right mg -> do
                                            debug lg $ msg ("Message recv' : " ++ (show $ msgType mg))
                                            return (Just mg, Nothing)
                        else do
                            err lg $ msg ("Error, network magic mismatch!: " ++ (show headMagic))
                            throw NetworkMagicMismatchException

--
doVersionHandshake ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> SockAddr
    -> m (Bool)
doVersionHandshake net sock sa = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    g <- liftIO $ newStdGen
    now <- round <$> liftIO getPOSIXTime
    let ip = bitcoinNodeListenIP $ nodeConfig p2pEnv
        port = toInteger $ bitcoinNodeListenPort $ nodeConfig p2pEnv
    myaddr <- liftIO $ head <$> getAddrInfo (Just defaultHints {addrSocketType = Stream}) (Just ip) (Just $ show port)
    let nonce = fst (random g :: (Word64, StdGen))
        ad = NetworkAddress 0 $ addrAddress myaddr
        bb = 1 :: Word32 -- ### TODO: getBestBlock ###
        rmt = NetworkAddress 0 sa
        ver = buildVersion net nonce bb ad rmt now
        em = runPut . putMessage net $ (MVersion ver)
    mv <- liftIO $ (newMVar ())
    liftIO $ sendEncMessage mv sock (BSL.fromStrict em)
    (hs1, _) <- readNextMessage net sock Nothing
    case hs1 of
        Just (MVersion __) -> do
            (hs2, _) <- readNextMessage net sock Nothing
            case hs2 of
                Just MVerAck -> do
                    let em2 = runPut . putMessage net $ (MVerAck)
                    liftIO $ sendEncMessage mv sock (BSL.fromStrict em2)
                    debug lg $ msg ("Version handshake complete: " ++ show sa)
                    return True
                __ -> do
                    err lg $ msg $ val "Error, unexpected message (2) during handshake"
                    return False
        __ -> do
            err lg $ msg $ val "Error, unexpected message (1) during handshake"
            return False

messageHandler ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => BitcoinPeer
    -> (Maybe Message, Maybe IngressStreamState)
    -> m (MessageCommand)
messageHandler peer (mm, ingss) = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    db <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = rocksDB db
        cf = rocksCF db
    case mm of
        Just msg -> do
            liftIO $ print $ "MSG: " ++ (show $ msgType msg)
            case (msg) of
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
                                Just sock -> liftIO $ Network.Socket.close sock
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
                                     unsynced <- checkBlocksFullySynced_ net
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
                    let em = runPut . putMessage net $ (MPong $ Pong (pingNonce ping))
                    case (bpSocket peer) of
                        Just sock -> do
                            liftIO $ sendEncMessage (bpWriteMsgLock peer) sock (BSL.fromStrict em)
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
                    txs' <- mapM (txFromHash conn cf) bt
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

txFromHash conn cf txh = do
    cftx <- liftIO $ TSH.lookup cf ("tx")
    case cftx of
        Just cftx' -> do
            tx' <- getDBCF conn cftx' (txh)
            return tx'
        Nothing -> do
            return Nothing -- ideally should be unreachable

processTxBatch :: (HasXokenNodeEnv env m, MonadIO m) => [Tx] -> IngressStreamState -> m ()
processTxBatch txns iss = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let bi = issBlockIngest iss
    let binfo = issBlockInfo iss
    case binfo of
        Just bf -> do
            valx <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (biBlockHash bf)
            skip <-
                case valx of
                    Just lfa -> do
                        y <- liftIO $ TSH.lookup (fst lfa) (txHash $ head txns)
                        case y of
                            Just c -> return True
                            Nothing -> return False
                    Nothing -> return False
            if skip
                then do
                    debug lg $
                        LG.msg $
                        ("Tx already processed, block: " ++
                         (show $ biBlockHash bf) ++ ", tx-index: " ++ show (binTxIngested bi))
                else do
                    S.drain $
                        aheadly $
                        (do let start = (binTxIngested bi) - (L.length txns)
                                end = (binTxIngested bi) - 1
                            S.fromList $ zip [start .. end] [0 ..]) &
                        S.mapM
                            (\(cidx, idx) -> do
                                 if (idx >= (L.length txns))
                                     then debug lg $ LG.msg $ (" (error) Tx__index: " ++ show idx ++ show bf)
                                     else debug lg $ LG.msg $ ("Tx__index: " ++ show idx)
                                 return ((txns !! idx), bf, cidx)) &
                        S.mapM (processTxStream) &
                        S.maxBuffer (maxTxProcessingBuffer $ nodeConfig bp2pEnv) &
                        S.maxThreads (maxTxProcessingThreads $ nodeConfig bp2pEnv)
                    valy <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (biBlockHash bf)
                    case valy of
                        Just lefta -> liftIO $ TSH.insert (fst lefta) (txHash $ head txns) (L.length txns)
                        Nothing -> return ()
                    return ()
        Nothing -> throw InvalidStreamStateException

--
-- 
processTxStream :: (HasXokenNodeEnv env m, MonadIO m) => (Tx, BlockInfo, Int) -> m ()
processTxStream (tx, binfo, txIndex) = do
    let bhash = biBlockHash binfo
        bheight = biBlockHeight binfo
    lg <- getLogger
    res <- LE.try $ zRPCDispatchTxValidate processConfTransaction tx bhash bheight (fromIntegral txIndex)
    case res of
        Right () -> return ()
        Left TxIDNotFoundException -> do
            throw TxIDNotFoundException
        Left KeyValueDBInsertException -> do
            err lg $ LG.msg $ val "[ERROR] KeyValueDBInsertException"
            throw KeyValueDBInsertException
        Left e -> do
            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
            throw e

readNextMessage' ::
       (HasXokenNodeEnv env m, MonadIO m)
    => BitcoinPeer
    -> MVar (Maybe IngressStreamState)
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer readLock = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        tracker = statsTracker peer
    case bpSocket peer of
        Just sock -> do
            prIss <- liftIO $ takeMVar $ readLock
            let prevIngressState =
                    case prIss of
                        Just pis ->
                            if (binTxTotalCount $ issBlockIngest pis) == (binTxIngested $ issBlockIngest pis)
                                then Nothing
                                else prIss
                        Nothing -> Nothing
            (msg, ingressState) <- readNextMessage net sock prevIngressState
            case ingressState of
                Just iss -> do
                    let ingst = issBlockIngest iss
                    case msg of
                        Just (MBlock blk) -- setup state
                         -> do
                            let hh = headerHash $ defBlockHeader blk
                            mht <- liftIO $ TSH.lookup (blockSyncStatusMap bp2pEnv) (hh)
                            case (mht) of
                                Just x -> return ()
                                Nothing -> do
                                    err lg $ LG.msg $ ("InvalidBlockSyncStatusMapException - " ++ show hh)
                                    throw InvalidBlockSyncStatusMapException
                            let iz = Just (IngressStreamState ingst (Just $ BlockInfo hh (snd $ fromJust mht)))
                            liftIO $ putMVar readLock iz
                        Just (MConfTx ctx) -> do
                            case issBlockInfo iss of
                                Just bi -> do
                                    tm <- liftIO $ getCurrentTime
                                    liftIO $ writeIORef (ptLastTxRecvTime tracker) $ Just tm
                                    if binTxTotalCount ingst == binTxIngested ingst
                                        then do
                                            liftIO $ modifyIORef' (ptBlockFetchWindow tracker) (\z -> z - 1)
                                            liftIO $
                                                TSH.insert
                                                    (blockSyncStatusMap bp2pEnv)
                                                    (biBlockHash bi)
                                                    (BlockReceiveComplete tm, biBlockHeight bi)
                                            debug lg $
                                                LG.msg $ ("putMVar readLock Nothing - " ++ (show $ bpAddress peer))
                                            liftIO $ putMVar readLock Nothing
                                        else do
                                            liftIO $
                                                TSH.insert
                                                    (blockSyncStatusMap bp2pEnv)
                                                    (biBlockHash bi)
                                                    ( RecentTxReceiveTime (tm, binTxIngested ingst)
                                                    , biBlockHeight bi -- track receive progress
                                                     )
                                            liftIO $ putMVar readLock ingressState
                                Nothing -> throw InvalidBlockInfoException
                        otherwise -> throw $ UnexpectedDuringBlockProcException "_1_"
                Nothing -> do
                    liftIO $ putMVar readLock ingressState
                    return ()
            return (msg, ingressState)
        Nothing -> throw PeerSocketNotConnectedException

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
        Right (a) -> return ()
        Left (e :: SomeException) -> do
            err lg $ msg $ (val "[ERROR] Closing peer connection ") +++ (show e)
            case (bpSocket pr) of
                Just sock -> liftIO $ Network.Socket.close sock
                Nothing -> return ()
            liftIO $ atomically $ modifyTVar' (bitcoinPeers bp2pEnv) (M.delete (bpAddress pr))
            return ()

logMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> MessageCommand -> m (Bool)
logMessage peer mg = do
    lg <- getLogger
    -- liftIO $ atomically $ modifyTVar' (bpIngressMsgCount peer) (\z -> z + 1)
    debug lg $ LG.msg $ "DONE! processed: " ++ show mg
    return (True)

--
--
broadcastToPeers :: (HasXokenNodeEnv env m, MonadIO m) => Message -> m ()
broadcastToPeers msg = do
    liftIO $ putStrLn $ "Broadcasting " ++ show (msgType msg) ++ " to peers"
    bp2pEnv <- getBitcoinP2P
    peerMap <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    mapM_
        (\bp ->
             if bpConnected bp
                 then sendRequestMessages bp msg
                 else return ())
        peerMap
    liftIO $ putStrLn $ "Broadcasted " ++ show (msgType msg) ++ " to peers"

sendcmpt :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
sendcmpt bp = sendRequestMessages bp $ MSendCompact $ SendCompact 0 1

sendCmptBlock :: (HasXokenNodeEnv env m, MonadIO m) => CompactBlock -> BitcoinPeer -> m ()
sendCmptBlock cmpt bp = sendRequestMessages bp $ MCompactBlock cmpt

sendBlockTxn :: (HasXokenNodeEnv env m, MonadIO m) => BlockTxns -> BitcoinPeer -> m ()
sendBlockTxn blktxn bp = sendRequestMessages bp $ MBlockTxns blktxn

sendInv :: (HasXokenNodeEnv env m, MonadIO m) => Inv -> BitcoinPeer -> m ()
sendInv inv bp = sendRequestMessages bp $ MInv inv

mineBlockFromCandidate :: (HasXokenNodeEnv env m, MonadIO m) => m (Maybe CompactBlock)
mineBlockFromCandidate = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = rocksDB dbe
    bbn <- fetchBestBlock
    let (bhash,ht) = (headerHash $ nodeHeader bbn, nodeHeight bbn)
    dag <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) bhash
    case dag of
        Nothing -> return Nothing
        Just dag' -> do
            top <- liftIO $ DAG.getPrimaryTopologicalSorted dag'
            ct <- liftIO getPOSIXTime
            if L.null top
                then do
                    liftIO $ print $ "Couldn't mine cmptblk (dag empty over): " ++ show bhash
                    return Nothing
                else do
                    let cbase =
                            makeCoinbaseTx
                                (fromIntegral $ ht + 1)
                                (fromJust $ stringToAddr net "msedZ3WsLDsd97f2cPo26J5xU5ZjBgTExz")
                                1250000000
                        txhashes = (txHash cbase : top)
                    --    TxHash hh = txHash $ cbase
                    --let TxHash hh' = head top
                    --tx' <- liftIO $ TSH.lookup (unconfTxCache bp2pEnv) (TxHash hh')
                    --case tx' of
                    --    Nothing -> do
                    --        liftIO $ print $ "Mined cmptblk (tx not in cache): " ++ show (TxHash hh')
                    --        return Nothing
                    --    Just tx -> do
                    let bh =
                            BlockHeader
                                0x20000000
                                bhash
                                (buildMerkleRoot txhashes)
                                (fromIntegral $ floor ct)
                                0x207fffff
                                (1) -- BlockHeader
                        (bhsh@(BlockHash bhsh'), nn) = generateHeaderHash net bh
                        sidl = fromIntegral $ L.length $ top -- shortIds length
                        skey = getCompactBlockSipKey bh $ fromIntegral nn
                        pfl = 1
                        --cbase'' = fromJust cbase'
                        --ourtxin = head $ txIn cbase
                        --newtxin' = fmap (\ti -> ti {scriptInput = scriptInput ourtxin}) $ txIn cbase''
                        --newcb = cbase'' {txIn = newtxin'}
                        pftx = [PrefilledTx 0 $ cbase]
                        (cbsid:sids) = map (\txid -> txHashToShortId' txid skey) $ txhashes -- shortIds
                        cb = CompactBlock (bh {bhNonce = nn}) (fromIntegral nn) sidl sids pfl pftx
                    liftIO $ TSH.insert (compactBlocks bp2pEnv) bhsh (cb, top)
                    liftIO $
                        print $
                        "Mined cmptblk " ++
                        show bhsh ++
                        " over " ++
                        show bhash ++
                        " with work " ++
                        (show $ headerWork bh) ++
                        " and coinbase tx: " ++
                        (show $ runPutLazy $ putLazyByteString $ DS.encodeLazy cbase)
                                                                -- ++ " and prev coinbase tx: " ++ (show $ runPutLazy $ putLazyByteString $ DS.encodeLazy cbase')
                         ++
                        " hash: " ++ (show $ head $ txhashes) ++ " sid: " ++ (show $ cbsid)
                    --peerMap <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
                    --mapM_ (\bp -> if bpConnected bp then processCompactBlock cb bp else return ()) peerMap
                    newCandidateBlock bhsh
                    broadcastToPeers $ MInv $ Inv [InvVector InvBlock bhsh']
                    mapM_ (\x -> updateZtxiUtxo x bhsh $ fromIntegral $ ht + 1) top
                    return $ Just $ cb

generateHeaderHash :: Network -> BlockHeader -> (BlockHash, Word32)
generateHeaderHash net hdr = if isValidPOW net hdr
                                then (headerHash hdr, bhNonce hdr)
                                else generateHeaderHash net (hdr {bhNonce = (bhNonce hdr + 1)})

updateZtxiUtxo :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> BlockHash -> Word32 -> m ()
updateZtxiUtxo txh bh ht = do
    count <- zRPCDispatchUpdateOutpoint (OutPoint txh 0) bh ht
    if count <= 0
        then return ()
        else do
            let inds = [1 .. (count - 1)]
            LA.mapConcurrently_ (\i -> zRPCDispatchUpdateOutpoint (OutPoint txh i) bh ht) inds

{-
updateZtxiUtxo' :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> BlockHash -> Word32 -> m ()
updateZtxiUtxo' txh bh ht = updateZtxiUtxoOutpoints (OutPoint txh 0) bh ht

updateZtxiUtxoOutpoints :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> Word32 -> m ()
updateZtxiUtxoOutpoints op@(OutPoint txh ind) bh ht = do
    upd <- zRPCDispatchUpdateOutpoint op bh ht
    if upd == (-1)
        then return ()
        else do
            liftIO $ print $ "updateZtxiUtxoOutpoints: " ++ show (op,bh,ht)
            updateZtxiUtxoOutpoints (op {outPointIndex = ind + 1}) bh ht
-}
