{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.P2P.BlockSync
    ( processBlock
    , processConfTransaction
    , peerBlockSync
    , checkBlocksFullySynced
    , runPeerSync
    , runBlockCacheQueue
    , sendRequestMessages
    , zRPCDispatchTxValidate
    ) where

import Codec.Serialise
import qualified Codec.Serialise as CBOR
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently, mapConcurrently_, race_)
import qualified Control.Concurrent.Async.Lifted as LA (async, concurrently_, mapConcurrently, mapConcurrently_, wait)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import Control.Monad.Trans.Control
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.IORef
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize
import Data.Serialize as S
import qualified Data.Set as DS
import qualified Data.Store as DSE
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.Bolt as BT
import qualified Database.RocksDB as R
import qualified ListT as LT
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service.Chain
import Network.Xoken.Node.WorkerDispatcher
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import StmContainers.Set as SS
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Printf
import Xoken
import Xoken.NodeConfig as NC

produceGetDataMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> m (Message)
produceGetDataMessage peer = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg $ "Block - produceGetDataMessage - called." ++ (show peer)
    bl <- liftIO $ takeMVar (blockFetchQueue peer)
    trace lg $ LG.msg $ "took mvar.. " ++ (show bl) ++ (show peer)
    let gd = GetData $ [InvVector InvBlock $ getBlockHash $ biBlockHash bl]
    debug lg $ LG.msg $ "GetData req: " ++ show gd
    return (MGetData gd)

sendRequestMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> Message -> m ()
sendRequestMessages pr msg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    debug lg $ LG.msg $ val "Block - sendRequestMessages - called."
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    case fromException e of
                        Just (t :: AsyncCancelled) -> throw e
                        otherwise -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

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

runPeerSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runPeerSync =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        let net = bitcoinNetwork $ nodeConfig bp2pEnv
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        if L.length connPeers < (maxBitcoinPeerCount $ nodeConfig bp2pEnv)
            then do
                liftIO $
                    mapConcurrently_
                        (\(_, pr) ->
                             case (bpSocket pr) of
                                 Just s -> do
                                     let em = runPut . putMessage net $ (MGetAddr)
                                     debug lg $ LG.msg ("sending GetAddr to " ++ show pr)
                                     res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
                                     case res of
                                         Right () -> liftIO $ threadDelay (60 * 1000000)
                                         Left (e :: SomeException) -> err lg $ LG.msg ("[ERROR] runPeerSync " ++ show e)
                                 Nothing -> err lg $ LG.msg $ val "Error sending, no connections available")
                        (connPeers)
            else liftIO $ threadDelay (60 * 1000000)

markBestSyncedBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Text -> Int32 -> m ()
markBestSyncedBlock hash height = do
    rkdb <- rocksDB <$> getDB
    R.put rkdb "best_synced_hash" $ DTE.encodeUtf8 hash
    R.put rkdb "best_synced_height" $ C.pack $ show height
    liftIO $ print "MARKED BEST SYNCED INTO ROCKS DB"

checkBlocksFullySynced :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Network -> m Bool
checkBlocksFullySynced net = do
    rkdb <- rocksDB <$> getDB
    bestBlock <- fetchBestBlock rkdb net
    bestSynced <- fetchBestSyncedBlock rkdb net
    return $ bestBlock == bestSynced

getBatchSize :: Int32 -> Int32 -> [Int32]
getBatchSize peerCount n
    | n < 200000 =
        if peerCount > 8
            then [1 .. 8]
            else [1 .. peerCount]
    | n >= 200000 && n < 500000 =
        if peerCount > 4
            then [1 .. 2]
            else [1]
    | n >= 500000 && n < 640000 =
        if peerCount > 4
            then [1 .. 2]
            else [1]
    | otherwise = [1]

runBlockCacheQueue :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runBlockCacheQueue =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe <- getDB
        !tm <- liftIO $ getCurrentTime
        trace lg $ LG.msg $ val "runBlockCacheQueue loop..."
        let nc = nodeConfig bp2pEnv
            net = bitcoinNetwork $ nc
            rkdb = rocksDB dbe
        allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
        let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
        syt' <- liftIO $ TSH.toList (blockSyncStatusMap bp2pEnv)
        let syt = L.sortBy (\(_, (_, h)) (_, (_, h')) -> compare h h') syt'
            sysz = fromIntegral $ L.length syt
        -- reload cache
        retn <-
            if sysz == 0
                then do
                    (hash, ht) <- fetchBestSyncedBlock rkdb net
                    let cacheInd = getBatchSize (fromIntegral $ L.length connPeers) ht
                    let !bks = map (\x -> ht + x) cacheInd
                    res <-
                        liftIO $
                        try $
                        mapM
                            (\(kb, k) -> do
                                 x <- R.get rkdb kb
                                 return $
                                     (\v ->
                                          case DSE.decode v :: Either DSE.PeekException T.Text of
                                              Left _ -> Nothing
                                              Right v' -> Just (k, v')) <$>
                                     x)
                            ((\k -> (DSE.encode k, k)) <$> bks)
                    case res of
                        Left (e :: SomeException) -> do
                            err lg $ LG.msg ("Error: runBlockCacheQueue: " ++ show e)
                            throw e
                        Right (op') -> do
                            let op = catMaybes $ fmap join op'
                            if L.length op == 0
                                then do
                                    debug lg $ LG.msg $ val "Synced fully!"
                                    return (Nothing)
                                else if L.length op == (fromIntegral $ last cacheInd)
                                         then do
                                             debug lg $ LG.msg $ val "Reloading cache."
                                             let !p =
                                                     catMaybes $
                                                     map
                                                         (\x ->
                                                              case (hexToBlockHash $ snd x) of
                                                                  Just h ->
                                                                      Just (h, (RequestQueued, fromIntegral $ fst x))
                                                                  Nothing -> Nothing)
                                                         (op)
                                             mapM (\(k, v) -> liftIO $ TSH.insert (blockSyncStatusMap bp2pEnv) k v) p
                                             let e = p !! 0
                                             return (Just $ BlockInfo (fst e) (snd $ snd e))
                                         else do
                                             debug lg $ LG.msg $ val "Still loading block headers, try again!"
                                             return (Nothing)
                else do
                    mapM
                        (\(bsh, (_, ht)) -> do
                             q <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (bsh)
                             case q of
                                 Nothing -> trace lg $ LG.msg $ ("bsh did-not-find : " ++ show bsh)
                                 Just (vvv, www) -> do
                                     eee <- liftIO $ TSH.toList vvv
                                     trace lg $ LG.msg $ ("bsh: " ++ (show bsh) ++ " " ++ (show eee) ++ (show www)))
                        syt
                    --
                    mapM
                        (\(bsh, (_, ht)) -> do
                             valx <- liftIO $ TSH.lookup (blockTxProcessingLeftMap bp2pEnv) (bsh)
                             case valx of
                                 Just xv -> do
                                     siza <- liftIO $ TSH.toList (fst xv)
                                     if ((sum $ snd $ unzip siza) == snd xv)
                                         then do
                                             liftIO $
                                                 TSH.insert
                                                     (blockSyncStatusMap bp2pEnv)
                                                     (bsh)
                                                     (BlockProcessingComplete, ht)
                                         else return ()
                                 Nothing -> return ())
                        (syt)
                    --
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
                    trace lg $ LG.msg $ ("blockSyncStatusMap size: " ++ (show sysz))
                    trace lg $ LG.msg $ ("blockSyncStatusMap (list): " ++ (show syt))
                    if L.length sent == 0 &&
                       L.length unsent == 0 && L.length receiveInProgress == 0 && L.length recvComplete == 0
                        then do
                            let !lelm = last $ L.sortOn (snd . snd) (syt)
                            debug lg $ LG.msg $ ("marking best synced " ++ show (blockHashToHex $ fst $ lelm))
                            markBestSyncedBlock (blockHashToHex $ fst $ lelm) (fromIntegral $ snd $ snd $ lelm)
                            mapM
                                (\(k, _) -> do
                                     liftIO $ TSH.delete (blockSyncStatusMap bp2pEnv) k
                                     liftIO $ TSH.delete (blockTxProcessingLeftMap bp2pEnv) k)
                                syt
                            return Nothing
                        else do
                            if L.length processingIncomplete > 0
                                then return $ mkBlkInf $ getHead processingIncomplete
                                else if L.length recvTimedOut > 0
                                         then return $ mkBlkInf $ getHead recvTimedOut
                                         else if L.length recvNotStarted > 0
                                                  then return $ mkBlkInf $ getHead recvNotStarted
                                                  else if L.length unsent > 0
                                                           then return $ mkBlkInf $ getHead unsent
                                                           else return Nothing
        case retn of
            Just bbi -> do
                latest <- liftIO $ newIORef True
                sortedPeers <- liftIO $ sortPeers (snd $ unzip connPeers)
                mapM_
                    (\pr -> do
                         ltst <- liftIO $ readIORef latest
                         if ltst
                             then do
                                 trace lg $ LG.msg $ "try putting mvar.. " ++ (show bbi)
                                 fl <- liftIO $ tryPutMVar (blockFetchQueue pr) bbi
                                 if fl
                                     then do
                                         trace lg $ LG.msg $ "done putting mvar.. " ++ (show bbi)
                                         !tm <- liftIO $ getCurrentTime
                                         liftIO $
                                             TSH.insert
                                                 (blockSyncStatusMap bp2pEnv)
                                                 (biBlockHash bbi)
                                                 (RequestSent tm, biBlockHeight bbi)
                                         liftIO $ writeIORef latest False
                                     else return ()
                             else return ())
                    (sortedPeers)
            Nothing -> do
                trace lg $ LG.msg $ "nothing yet" ++ ""
        --
        liftIO $ threadDelay (10000) -- 0.01 sec
        return ()
  where
    getHead l = head $ L.sortOn (snd . snd) (l)
    mkBlkInf h = Just $ BlockInfo (fst h) (snd $ snd h)

sortPeers :: [BitcoinPeer] -> IO ([BitcoinPeer])
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
    return $ snd $ unzip $ L.sortBy (\(a, _) (b, _) -> compare b a) (zip ts peers)

fetchBestSyncedBlock :: (HasLogger m, MonadIO m) => R.DB -> Network -> m ((BlockHash, Int32))
fetchBestSyncedBlock rkdb net = do
    lg <- getLogger
    hash <- liftIO $ R.get rkdb "best_synced_hash"
    height <- liftIO $ R.get rkdb "best_synced_height"
    case (hash, height) of
        (Just hs, Just ht) -> do
            liftIO $
                print $
                "FETCHED BEST SYNCED FROM ROCKS DB: " ++
                (T.unpack $ DTE.decodeUtf8 hs) ++ " " ++ (T.unpack . DTE.decodeUtf8 $ ht)
            case hexToBlockHash $ DTE.decodeUtf8 hs of
                Nothing -> throw InvalidBlockHashException
                Just hs' -> return (hs', read . T.unpack . DTE.decodeUtf8 $ ht :: Int32)
        _ -> do
            debug lg $ LG.msg $ val "Bestblock is genesis."
            return ((headerHash $ getGenesisHeader net), 0)

insertTxIdOutputs :: (HasLogger m, MonadIO m) => R.DB -> R.ColumnFamily -> (TxHash, Word32) -> TxOut -> m ()
insertTxIdOutputs conn cf (txid, outputIndex) txOut = do
    lg <- getLogger
    res <- liftIO $ try $ putDBCF conn cf (txid, outputIndex) txOut
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into " ++ (show cf) ++ ": " ++ show e
            throw KeyValueDBInsertException

processConfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> BlockHash -> Word32 -> Word32 -> m ()
processConfTransaction tx bhash blkht txind = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = rocksDB $ dbe'
        cf = rocksCF dbe'
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx)
    xxx <- liftIO $ TSH.toList $ txZtxiUtxoTable bp2pEnv
    debug lg $ LG.msg $ "blockheight: " ++ (show blkht) ++ "sizaaa: " ++ show (L.length xxx)
    let inputs = zip (txIn tx) [0 :: Word32 ..]
    let outputs = zip (txOut tx) [0 :: Word32 ..]
    --
    let outpoints =
            map (\(b, _) -> ((outPointHash $ prevOutput b), fromIntegral $ outPointIndex $ prevOutput b)) (inputs)
    LA.mapConcurrently_
        (\(o, oindx) -> do
             debug lg $
                 LG.msg $
                 " insert ZUT record : " ++
                 (show (txHashToHex $ txHash tx)) ++ " - " ++ (show $ (getTxShortHash (txHash tx) 20, oindx))
             liftIO $
                 TSH.insert
                     (txZtxiUtxoTable bp2pEnv)
                     ((txHash tx), oindx)
                     (ZtxiUtxo
                          (txHash tx)
                          (oindx)
                          [bhash] -- if already present then ADD to the existing list of BlockHashes
                          (fromIntegral blkht)
                          outpoints
                          []
                          (fromIntegral $ outValue o)))
        (outputs)
    --
    inputValsOutpoints <-
        LA.mapConcurrently
            (\(b, indx) -> do
                 let shortHash = getTxShortHash (outPointHash $ prevOutput b) 20
                 let opindx = fromIntegral $ outPointIndex $ prevOutput b
                 if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                     then do
                         let sval = fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32)
                         return (sval, (shortHash, opindx))
                     else do
                         zz <- LE.try $ zRPCDispatchGetOutpoint (prevOutput b) bhash
                         case zz of
                             Right val -> do
                                 return (val, (shortHash, opindx))
                             Left (e :: SomeException) -> do
                                 err lg $
                                     LG.msg $
                                     "Error: [pCT calling gSVFO] WHILE Processing TxID " ++
                                     show (txHashToHex $ txHash tx) ++
                                     ", getting value for dependent input (TxID,Index): (" ++
                                     show (txHashToHex $ outPointHash (prevOutput b)) ++
                                     ", " ++ show (outPointIndex $ prevOutput b) ++ ")" ++ (show e)
                                 throw e)
            (inputs)
    LA.mapConcurrently_
        (\(b, indx) -> do
             let opt = OutPoint (outPointHash $ prevOutput b) (outPointIndex $ prevOutput b)
             predBlkHash <- getChainIndexByHeight $ fromIntegral blkht - 10
             case predBlkHash of
                 Just pbh -> zRPCDispatchTraceOutputs opt pbh -- TODO: use appropriate Stale marker blockhash
                 Nothing -> do
                     if blkht > 11
                         then throw InvalidBlockHashException
                         else return []
             return ())
        (inputs)
    --
    let ipSum = foldl (+) 0 $ (\(val, _) -> val) <$> inputValsOutpoints
        opSum = foldl (+) 0 $ (\o -> outValue o) <$> (txOut tx)
    if (ipSum - opSum) < 0
        then do
            debug lg $ LG.msg $ " ZUT value mismatch " ++ (show ipSum) ++ " " ++ (show opSum)
            throw InvalidTxSatsValueException
        else return ()
    -- signal 'done' event for tx's that were processed out of sequence 
    --
    txSyncMap <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txHash tx)
    case (txSyncMap) of
        Just ev -> liftIO $ EV.signal $ ev
        Nothing -> return ()
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx) ++ ": end of processing signaled"
    cf' <- liftIO $ TSH.lookup cf (getEpochTxOutCF epoch)
    case cf' of
        Just cf'' -> do
            mapM_
                (\(txOut, oind) -> do
                     debug lg $ LG.msg $ "inserting output " ++ show txOut
                     insertTxIdOutputs conn cf'' (txHash tx, oind) (txOut)
                     x <- (getDBCF conn cf'' (txHash tx, oind))
                     liftIO $ print (x :: Maybe TxOut))
                outputs
        Nothing -> return () -- ideally should be unreachable

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
processBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg ("processing deflated Block! " ++ show dblk)
    -- liftIO $ signalQSem (blockFetchBalance bp2pEnv)
    return ()
