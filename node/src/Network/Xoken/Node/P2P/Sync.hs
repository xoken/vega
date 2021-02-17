{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.P2P.Sync where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, mapConcurrently_)
import qualified Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Reader
import Control.Monad.STM
import qualified Data.ByteString as B
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Time.Calendar
import Data.Time.Clock
import qualified Network.Socket as NS
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Network.Message
import Network.Xoken.Node.DB
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.Message.Handler
import Network.Xoken.Node.P2P.Message.Sender
import Network.Xoken.Node.P2P.Socket
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Dispatcher
import System.Logger as LG
import System.Random (randomRIO)
import Xoken.NodeConfig as NC

runEgressChainSync :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runEgressChainSync = do
    lg <- getLogger
    res1 <- LE.try $ forever $ produceGetHeadersMessage >>= sendGetHeaderMessages
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg $ "[ERROR] runEgressChainSync " ++ show e

runPeerSync :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runPeerSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (bpConnected . snd) (M.toList allPeers)
        if L.length connPeers < maxBitcoinPeerCount (nodeConfig bp2pEnv)
            then liftIO $
                 mapConcurrently_
                     (\(_, pr) ->
                          case bpSocket pr of
                              Just s -> do
                                  debug lg $ LG.msg ("sending GetAddr to " ++ show pr)
                                  res <- liftIO $ try $ encodeAndSendMessage (bpWriteMsgLock pr) s net MGetAddr
                                  case res of
                                      Right () -> liftIO $ threadDelay (60 * 1000000)
                                      Left (e :: SomeException) -> err lg $ LG.msg ("[ERROR] runPeerSync " ++ show e)
                              Nothing -> err lg $ LG.msg $ val "Error sending, no connections available")
                     connPeers
            else liftIO $ threadDelay (60 * 1000000)

getBatchSizeMainnet :: Int32 -> Int32 -> [Int32]
getBatchSizeMainnet peerCount n
    | n < 200000 =
        if peerCount > 8
            then [1 .. 4]
            else [1 .. 2]
    | n >= 200000 && n < 540000 =
        if peerCount > 4
            then [1 .. 2]
            else [1]
    | otherwise = [1]

getBatchSizeTestnet :: Int32 -> Int32 -> [Int32]
getBatchSizeTestnet peerCount n
    | peerCount > 4 = [1 .. 2]
    | otherwise = [1]

getBatchSize :: Network -> Int32 -> Int32 -> [Int32]
getBatchSize net peerCount n
    | getNetworkName net == "bsvtest" = getBatchSizeTestnet peerCount n
    | getNetworkName net == "regtest" = [1]
    | otherwise = getBatchSizeMainnet peerCount n

runBlockCacheQueue :: (HasXokenNodeEnv env m, MonadIO m) => m ()
runBlockCacheQueue =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe <- getDB
        !tm <- liftIO getCurrentTime
        trace lg $ LG.msg $ val "runBlockCacheQueue loop..."
        let nc = nodeConfig bp2pEnv
            net = bitcoinNetwork nc
            rkdb = rocksDB dbe
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        syt' <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let syt = L.sortBy (\(_, (_, h)) (_, (_, h')) -> compare h h') syt'
            sysz = fromIntegral $ L.length syt
        fullySynced <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
        -- check any retries
        retn1 <-
            do debug lg $ LG.msg $ val "Checking for retries"
               let unsent = L.filter (\x -> (fst $ snd x) == RequestQueued) syt
               let sent =
                       L.filter
                           (\x ->
                                case fst $ snd x of
                                    RequestSent _ -> True
                                    otherwise -> False)
                           syt
               let recvNotStarted =
                       L.filter
                           (\(_, ((RequestSent t), _)) ->
                                (diffUTCTime tm t > (fromIntegral $ getDataResponseTimeout nc)))
                           sent
               let receiveInProgress =
                       L.filter
                           (\x ->
                                case fst $ snd x of
                                    RecentTxReceiveTime _ -> True
                                    otherwise -> False)
                           syt
               let recvTimedOut =
                       L.filter
                           (\(_, ((RecentTxReceiveTime (t, c)), _)) ->
                                (diffUTCTime tm t > (fromIntegral $ recentTxReceiveTimeout nc)))
                           receiveInProgress
               let recvComplete =
                       L.filter
                           (\x ->
                                case fst $ snd x of
                                    BlockReceiveComplete _ -> True
                                    otherwise -> False)
                           syt
               let processingIncomplete =
                       L.filter
                           (\(_, ((BlockReceiveComplete t), _)) ->
                                (diffUTCTime tm t > (fromIntegral $ blockProcessingTimeout nc)))
                           recvComplete
                -- all blocks received, empty the cache, cache-miss gracefully
               if L.length processingIncomplete > 0
                   then return $ mkBlkInf $ getHead processingIncomplete
                   else if L.length recvTimedOut > 0
                            then return $ mkBlkInf $ getHead recvTimedOut
                            else if L.length recvNotStarted > 0
                                     then return $ mkBlkInf $ getHead recvNotStarted
                                     else if L.length unsent > 0
                                              then return $ mkBlkInf $ getHead unsent
                                              else return Nothing
        -- reload cache
        retn <-
            case retn1 of
                Just x -> return $ Just x
                Nothing ->
                    if sysz <= (blocksFetchWindow $ nodeConfig bp2pEnv)
                        then do
                            (bhash, ht') <- fetchBestSyncedBlock
                            hmem <- liftIO $ readTVarIO (blockTree bp2pEnv)
                            let fetchMore = (blocksFetchWindow $ nodeConfig bp2pEnv) - sysz
                            let ht = fromIntegral ht'
                                bc = (blocksFetchWindow $ nodeConfig bp2pEnv) - sysz -- last $ getBatchSize net (fromIntegral $ L.length connPeers) ht
                                bh = ht + bc
                    --let !bks = map (\x -> ht + x) cacheInd
                                parc = bc - 1
                                ans = getAncestor hmem (fromIntegral bh) (memoryBestHeader hmem)
                                op =
                                    case ans of
                                        Nothing -> []
                                        Just an -> L.reverse (an : getParents hmem (fromIntegral parc) an)
                    --liftIO $ print (bhash,ht,bc,bh,parc,ans,op)
                            if L.null op
                                then do
                                    trace lg $ LG.msg $ val "Synced fully!"
                                    return Nothing
                                else do --if L.length op == fromIntegral bc
                                    debug lg $ LG.msg $ val "Reloading cache."
                                    let !p = fmap (\x -> (headerHash $ nodeHeader x, (RequestQueued, nodeHeight x))) op
                                    if L.null p
                                        then do
                                            trace lg $ LG.msg $ val "Nothing to add."
                                            return Nothing
                                        else do
                                            debug lg $ LG.msg $ val "Adding to cache."
                                            mapM_ (\(k, v) -> liftIO $ TSH.insert (blockSyncStatusMap bp2pEnv) k v) p
                                            let e = head p
                                            return (Just $ BlockInfo (fst e) (snd $ snd e))
                        else do
                            debug lg $ LG.msg $ val "Still loading block headers, try again!"
                            return Nothing
                    --
        mapM_
            (\(bsh, (_, ht)) -> do
                    valx <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (bsh)
                    case valx of
                        Just xv -> do
                            siza <- liftIO $ TSH.toList (fst xv)
                            trace lg $ LG.msg $ ("bsh: " ++ (show bsh) ++ " " ++ (show siza) ++ (show $ snd xv))
                            when ((sum $ snd $ unzip siza) == snd xv) $
                                liftIO $
                                TSH.insert (blockSyncStatusMap bp2pEnv) (bsh) (BlockProcessingComplete, ht)
                        Nothing -> trace lg $ LG.msg $ ("bsh did-not-find : " ++ show bsh))
            syt
        --
        trace lg $ LG.msg $ ("blockSyncStatusMap size: " ++ (show sysz))
        trace lg $ LG.msg $ ("blockSyncStatusMap (list): " ++ (show syt))
        syncList <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let sortedList = L.sortOn (snd . snd) syncList
        trace lg $ LG.msg $ ("sorted blockSyncStatusMap (list): " ++ (show sortedList))
        let compl = L.takeWhile (\x -> (fst $ snd x) == BlockProcessingComplete) sortedList
        if not $ L.null compl
            then do
                            let !(lhash, (_, lht)) = last $ compl
                            debug lg $ LG.msg $ ("marking best synced " ++ show (blockHashToHex $ lhash))
                            markBestSyncedBlock (blockHashToHex $ lhash) (fromIntegral $ lht)
                            updatePredecessors
                            --
                            lp <- getDefault rkdb ("last-pruned" :: B.ByteString)
                            let (lpht, lphs) =
                                    fromMaybe (0, headerHash $ getGenesisHeader net) lp :: (BlockHeight, BlockHash)
                            debug lg $
                                LG.msg $
                                ("Last pruned: " ++
                                 show (lpht, blockHashToHex $ lphs) ++
                                 "; for best synced: " ++ show (lht, blockHashToHex lhash))
                            if lht - lpht <= (fromIntegral $ pruneLag nc)
                                then do
                                    debug lg $
                                        LG.msg $
                                        ("Last pruned too close. Skipping pruning. " ++ show (lht, lpht, lht - lpht))
                                    return ()
                                else do
                                    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
                                    let bnm = getBlockHeaderMemory lhash hm
                                    case bnm of
                                        Nothing -> return ()
                                        Just bn -> do
                                            let anc =
                                                    fmap (\x -> (nodeHeight x, headerHash $ nodeHeader x)) $
                                                    drop (fromIntegral $ (pruneLag nc) - 1) $
                                                    getParents hm (fromIntegral $ lht - lpht - 1) bn
                                            debug lg $ LG.msg $ ("Pruning " ++ show ((fmap blockHashToHex) <$> anc))
                                            debug lg $ LG.msg $ ("Marking Last Pruned: " ++ show (head anc))
                                            putDefault rkdb ("last-pruned" :: B.ByteString) (head anc)
                                            _ <- LA.async $ zRPCDispatchBlocksTxsOutputs $ fmap snd anc -- (fst $ unzip syt)
                                            return ()
                            --
                            mapM_
                                (\(k, _) -> do
                                     liftIO $ TSH.delete (blockSyncStatusMap bp2pEnv) k
                                     liftIO $ TSH.delete (blockTxProcessingLeftMap bp2pEnv) k)
                                compl
            else return ()
                             
        case retn of
            Just bbi -> do
                latest <- liftIO $ newIORef True
                sortedPeers <- liftIO $ sortPeersRandom (map snd connPeers)
                mapM_
                    (\pr -> do
                         ltst <- liftIO $ readIORef latest
                         when ltst $ do
                             trace lg $ LG.msg $ "try putting mvar.. " ++ show bbi
                             fl <- liftIO $ tryPutMVar (blockFetchQueue pr) bbi
                             when fl $ do
                                 trace lg $ LG.msg $ "done putting mvar.. " ++ show bbi
                                 !tm <- liftIO getCurrentTime
                                 liftIO $
                                     TSH.insert
                                         (blockSyncStatusMap bp2pEnv)
                                         (biBlockHash bbi)
                                         (RequestSent tm, biBlockHeight bbi)
                                 liftIO $ writeIORef latest False)
                    sortedPeers
            Nothing -> trace lg $ LG.msg $ "nothing yet" ++ ""
        --
        liftIO $ threadDelay 100000 -- 0.1 sec
        return ()
  where
    getHead l = head $ L.sortOn (snd . snd) l
    mkBlkInf h = Just $ BlockInfo (fst h) (snd $ snd h)


sortPeersRandom :: [BitcoinPeer] -> IO [BitcoinPeer]
sortPeersRandom peers = do
    mark <- randomRIO (0, L.length peers - 1)
    let parts = L.splitAt mark peers
    return $ snd parts ++ fst parts

sortPeers :: [BitcoinPeer] -> IO [BitcoinPeer]
sortPeers peers = do
    let longlongago = UTCTime (ModifiedJulianDay 1) 1
    ts <-
        mapM
            (\p -> do
                 lstr <- liftIO $ readIORef $ ptLastTxRecvTime $ statsTracker p
                 case lstr of
                     Just lr -> return lr
                     Nothing -> return longlongago)
            peers
    return $ map snd $ L.sortBy (\(a, _) (b, _) -> compare a b) (zip ts peers)

setupSeedPeerConnection :: (HasXokenNodeEnv env m, MonadIO m) => m ()
setupSeedPeerConnection =
    forever $ do
        bp2pEnv <- getBitcoinP2P
        lg <- getLogger
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
            seeds = getSeeds net
            hints = NS.defaultHints {NS.addrSocketType = NS.Stream}
            port = getDefaultPort net
        debug lg $ msg $ show seeds
        --let sd = map (\x -> Just (x :: HostName)) seeds
        !addrs <-
            liftIO $ mapConcurrently (\x -> head <$> NS.getAddrInfo (Just hints) (Just x) (Just (show port))) seeds
        mapM_
            (\y -> do
                 debug lg $ msg ("Peer.. " ++ show (NS.addrAddress y))
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
                                         case M.lookup (NS.addrAddress y) allpr of
                                             Just pr ->
                                                 if bpConnected pr
                                                     then False
                                                     else True
                                             Nothing -> True
                                     isBlacklisted = M.member (NS.addrAddress y) blockedpr
                                 if toConn == False
                                     then do
                                         debug lg $
                                             msg ("Seed peer already connected, ignoring.. " ++ show (NS.addrAddress y))
                                     else if isBlacklisted
                                              then do
                                                  debug lg $
                                                      msg
                                                          ("Seed peer blacklisted, ignoring.. " ++
                                                           show (NS.addrAddress y))
                                              else do
                                                  wl <- liftIO $ newMVar ()
                                                  {- UNUSED?
                                                  ss <- liftIO $ newTVarIO Nothing
                                                  imc <- liftIO $ newTVarIO 0
                                                  rc <- liftIO $ newTVarIO Nothing
                                                  st <- liftIO $ newTVarIO Nothing
                                                  fw <- liftIO $ newTVarIO 0
                                                  -}
                                                  res <- LE.try $ liftIO $ createSocket y
                                                  trk <- liftIO $ getNewTracker
                                                  bfq <- liftIO $ newEmptyMVar
                                                  sc <- liftIO $ newIORef False
                                                  case res of
                                                      Right (sock) -> do
                                                          case sock of
                                                              Just sx -> do
                                                                  fl <- doVersionHandshake net sx $ NS.addrAddress y
                                                                  let bp =
                                                                          BitcoinPeer
                                                                              (NS.addrAddress y)
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
                                                                          (M.insert (NS.addrAddress y) bp)
                                                                  liftIO $ print "Sending sendcmpt.."
                                                                  sendcmpt bp
                                                                  handleIncomingMessages bp
                                                              Nothing -> return ()
                                                      Left (SocketConnectException addr) ->
                                                          warn lg $ msg ("SocketConnectException: " ++ show addr)))
            (addrs)
        liftIO $ threadDelay (30 * 1000000)
--
-- Get ZUT from outpoint
-- getZUTFromOutpoint ::
--        (HasXokenNodeEnv env m, MonadIO m)
--     => Int
--     -> TSH.TSHashTable (TxHash, Word32) (MVar (Text, Text, Int64))
--     -> Logger
--     -> Network
--     -> OutPoint
--     -> Int
--     -> m (ZtxiUtxo)
-- getZUTFromOutpoint conn txSync lg net outPoint maxWait = do
--     bp2pEnv <- getBitcoinP2P
--     dbe <- getDB
--     let rkdb = rocksDB dbe
--         cfs = rocksCF dbe
--     lg <- getLogger
--     cf <- liftIO $ TSH.lookup cfs ("outputs")
--     res <- try $ deleteDBCF rkdb (fromJust cf) (outPointHash outPoint, opIndex)
--     case res of
--         Right results -> do
--             debug lg $
--                 LG.msg $ "Tx not found: " ++ (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
--             valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint, outPointIndex outPoint)
--             event <-
--                 case valx of
--                     Just evt -> return evt
--                     Nothing -> newEmptyMVar
--             liftIO $ TSH.insert txSync (outPointHash outPoint, outPointIndex outPoint) event
--             ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (maxWait * 1000000))
--             case ores of
--                 Right () -> do
--                     liftIO $ TSH.delete txSync (outPointHash outPoint, outPointIndex outPoint)
--                     throw TxIDNotFoundException
--                 Left res -> do
--                     debug lg $ LG.msg $ "event received _available_: " ++ (show $ txHashToHex $ outPointHash outPoint)
--                     liftIO $ TSH.delete txSync (outPointHash outPoint, outPointIndex outPoint)
--                     return res
--         Left (e :: SomeException) -> do
--             err lg $ LG.msg $ "Error: getSatsValueFromOutpoint: " ++ show e
--             throw e
{-
dagEpochSwitcher :: (HasXokenNodeEnv env m, MonadIO m) => m ()
dagEpochSwitcher = do
    bp2pEnv <- getBitcoinP2P
    ep <- liftIO $ readTVarIO (persistOutputsDagEpoch bp2pEnv)
    let dag =
            if ep
                then persistOutputsDagOdd bp2pEnv
                else persistOutputsDagEven bp2pEnv
    LA.async $ (atomically $ LT.toList $ listT $ dag, dag) `persistDagIntoDBWith` (txZtxiUtxoTable bp2pEnv)
    liftIO $ atomically $ modifyTVar' (persistOutputsDagEpoch bp2pEnv) not
persistDagIntoDBWith ::
       MonadIO m
    => (IO [((TxShortHash, Int16), Bool)], SM.Map (TxShortHash, Int16) Bool)
    -> SM.Map (TxShortHash, Int16) ZtxiUtxo
    -> m ()
persistDagIntoDBWith (dagList, dag) zu = do
    x <-
        return $
        (fmap . fmap)
            (\((tsh, h), bool) -> do
                 case bool of
                     True -> do
                         val <- atomically $ SM.lookup (tsh, h) zu
                         case val of
                             Nothing -> return ()
                             Just v -> KV.insert (LC.toStrict $ A.encode (tsh, h)) (LC.toStrict $ A.encode v)
                     False -> do
                         KV.delete $ LC.toStrict $ A.encode (tsh, h)
                 atomically $ SM.delete (tsh, h) zu
                 atomically $ SM.delete (tsh, h) dag)
            (dagList)
    return ()
-}
--
--
