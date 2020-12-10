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
    , processCompactBlock
    , processBlockTransactions
    , processConfTransaction
    , peerBlockSync
    , fetchBestSyncedBlock
    , markBestSyncedBlock
    , checkBlocksFullySynced
    , checkBlocksFullySynced_
    , runPeerSync
    , runBlockCacheQueue
    , sendRequestMessages
    , zRPCDispatchTxValidate
    , processCompactBlockGetData
    , newCandidateBlock
    , newCandidateBlockChainTip
    ) where

import Codec.Serialise
import qualified Codec.Serialise as CBOR
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently, mapConcurrently_, race_)
import qualified Control.Concurrent.Async.Lifted as LA
    ( async
    , concurrently_
    , mapConcurrently
    , mapConcurrently_
    , race
    , wait
    )
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
import Crypto.MAC.SipHash as SH
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashMap.Strict as HM
import qualified Data.HashTable.IO as H
import Data.IORef
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import qualified Data.Sequence as SQ
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
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service.Chain
import Network.Xoken.Node.WorkerDispatcher
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import StmContainers.Set as SS
import Streamly as S
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
    -- liftIO $ print "MARKED BEST SYNCED INTO ROCKS DB"

checkBlocksFullySynced :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Network -> m Bool
checkBlocksFullySynced net = do
    rkdb <- rocksDB <$> getDB
    bestBlock <- fetchBestBlock rkdb net
    bestSynced <- fetchBestSyncedBlock rkdb net
    return $ bestBlock == bestSynced

checkBlocksFullySynced_ :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Network -> m Int32
checkBlocksFullySynced_ net = do
    rkdb <- rocksDB <$> getDB
    bestBlock <- fetchBestBlock rkdb net
    bestSynced <- fetchBestSyncedBlock rkdb net
    return $ (snd bestBlock) - (snd bestSynced)

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
    | (getNetworkName net == "bsvtest") = getBatchSizeTestnet peerCount n
    | (getNetworkName net == "regtest") = [1]
    | otherwise = getBatchSizeMainnet peerCount n

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
                    let cacheInd = getBatchSize net (fromIntegral $ L.length connPeers) ht
                    let !bks = map (\x -> ht + x) cacheInd
                    res <-
                        liftIO $
                        try $
                        mapM
                            (\k -> do
                                 x <- getDB' rkdb k -- Maybe (Text, BlockHeader)
                                 return $
                                     case x :: Maybe (Text, BlockHeader) of
                                         Nothing -> Nothing
                                         Just (x', _) -> Just (k, x'))
                            (bks)
                    case res of
                        Left (e :: SomeException) -> do
                            err lg $ LG.msg ("Error: runBlockCacheQueue: " ++ show e)
                            throw e
                        Right (op' :: [Maybe (Int32, Text)]) -> do
                            let op = catMaybes $ op'
                            if L.length op == 0
                                then do
                                    trace lg $ LG.msg $ val "Synced fully!"
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
                            --
                            _ <- LA.async $ zRPCDispatchBlocksTxsOutputs (fst $ unzip syt)
                            --
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
        (Just hs, Just ht)
            -- liftIO $
            --     print $
            --     "FETCHED BEST SYNCED FROM ROCKS DB: " ++
            --     (T.unpack $ DTE.decodeUtf8 hs) ++ " " ++ (T.unpack . DTE.decodeUtf8 $ ht)
         -> do
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
            err lg $ LG.msg $ "[dag] Error: INSERTing into " ++ (show cf) ++ ": " ++ show e
            throw KeyValueDBInsertException

processConfTransaction ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> BlockHash -> Word32 -> Word32 -> m ([OutPoint])
processConfTransaction tx bhash blkht txind = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = rocksDB $ dbe'
        cf = rocksCF dbe'
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx)
    debug lg $ LG.msg $ "[dag] processing Tx " ++ show (txHash tx)
    let inputs = zip (txIn tx) [0 :: Word32 ..]
    let outputs = zip (txOut tx) [0 :: Word32 ..]
    --
    let outpoints =
            map (\(b, _) -> ((outPointHash $ prevOutput b), fromIntegral $ outPointIndex $ prevOutput b)) (inputs)
    --
    inputValsOutpoints <-
        mapM
            (\(b, indx) -> do
                 let shortHash = getTxShortHash (outPointHash $ prevOutput b) 20
                 let opindx = fromIntegral $ outPointIndex $ prevOutput b
                 if (outPointHash nullOutPoint) == (outPointHash $ prevOutput b)
                     then do
                         let sval = fromIntegral $ computeSubsidy net $ (fromIntegral blkht :: Word32)
                         return (sval, (shortHash, opindx))
                     else do
                         zz <- LE.try $ zRPCDispatchGetOutpoint (prevOutput b) $ Just bhash
                         case zz of
                             Right (val, _) -> do
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
    -- insert UTXO/s
    cf' <- liftIO $ TSH.lookup cf ("outputs")
    ovs <-
        mapM
            (\(opt, oindex) -> do
                 debug lg $ LG.msg $ "Inserting UTXO : " ++ show (txHash tx, oindex)
                 let zut =
                         ZtxiUtxo
                             (txHash tx)
                             (oindex)
                             [bhash] -- if already present then ADD to the existing list of BlockHashes
                             (fromIntegral blkht)
                             outpoints
                             []
                             (fromIntegral $ outValue opt)
                 res <- liftIO $ try $ putDBCF conn (fromJust cf') (txHash tx, oindex) zut
                 case res of
                     Right _ -> return (zut)
                     Left (e :: SomeException) -> do
                         err lg $ LG.msg $ "Error: INSERTing into " ++ (show cf') ++ ": " ++ show e
                         throw KeyValueDBInsertException)
            outputs
    --
    --
    -- mapM_
    --     (\(b, indx) -> do
    --          let opt = OutPoint (outPointHash $ prevOutput b) (outPointIndex $ prevOutput b)
    --          predBlkHash <- getChainIndexByHeight $ fromIntegral blkht - 10
    --          case predBlkHash of
    --              Just pbh -> do
    --                  _ <- zRPCDispatchTraceOutputs opt pbh True 0 -- TODO: use appropriate Stale marker blockhash
    --                  return ()
    --              Nothing -> do
    --                  if blkht > 11
    --                      then throw InvalidBlockHashException
    --                      else return ()
    --          return ())
    --     (inputs)
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
    vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txHash tx)
    case vall of
        Just ev -> liftIO $ EV.signal ev
        Nothing -> return ()
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx) ++ ": end of processing signaled"
    cf' <- liftIO $ TSH.lookup cf (getEpochTxOutCF epoch)
    case cf' of
        Just cf'' -> do
            mapM_
                (\(txOut, oind) -> do
                     debug lg $ LG.msg $ "inserting output " ++ show txOut
                     insertTxIdOutputs conn cf'' (txHash tx, oind) (txOut))
                outputs
        Nothing -> return () -- ideally should be unreachable
    let outpts = map (\(tid, idx) -> OutPoint tid idx) outpoints
    return (outpts)

--
-- Get ZUT from outpoint
-- getZUTFromOutpoint ::
--        (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
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
processBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg ("processing deflated Block! " ++ show dblk)
    -- liftIO $ signalQSem (blockFetchBalance bp2pEnv)
    return ()

processCompactBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => CompactBlock -> BitcoinPeer -> m ()
processCompactBlock cmpct peer = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let bhdr = cbHeader cmpct
        bhash = headerHash bhdr
    debug lg $ LG.msg ("processing Compact Block! " ++ show bhash ++ "  " ++ show cmpct)
    --
    let skey = getCompactBlockSipKey bhdr (cbNonce cmpct)
    let cmpctTxLst = zip (cbShortIDs cmpct) [1 ..]
    cb <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) (bhash)
    case cb of
        Nothing -> do
            return ()
        Just dag -> do
            debug lg $ LG.msg $ ("New Candidate Block Found over: " ++ show bhash)
            mpTxLst <- liftIO $ DAG.getTopologicalSortedForest dag
            let mpShortTxIDList = map (\(txid, rt) -> do (txHashToShortId' txid skey, (txid, rt))) mpTxLst
            let mpShortTxIDMap = HM.fromList mpShortTxIDList
            let (usedTxns, missingTxns) =
                    L.partition
                        (\(sid, index) -> do
                             let idx = HM.lookup sid mpShortTxIDMap
                             case idx of
                                 Just x -> True
                                 Nothing -> False)
                        cmpctTxLst
            let usedTxnMap = HM.fromList usedTxns
            mapM
                (\(sid, (txid, rt)) -> do
                     let fd = HM.lookup sid usedTxnMap
                     case fd of
                         Just x -> return ()
                         Nothing
                             -- TODO: lock the previous dag and insert into a NEW dag!!
                          -> do
                             case rt of
                                 Just p -> liftIO $ DAG.coalesce dag txid [p] 999 (+) nextBcState
                                 Nothing -> liftIO $ DAG.coalesce dag txid [] 999 (+) nextBcState)
                mpShortTxIDList
            --    lastIndex <- liftIO $ newIORef 0
            --    mtxIndexes <-
            --        mapM
            --            (\(__, indx) -> do
            --                prev <- liftIO $ readIORef lastIndex
            --                liftIO $ writeIORef lastIndex indx
            --                return $ indx - prev)
            --            missingTxns
            let mtxIndexes = map snd missingTxns
            let gbtxn = GetBlockTxns bhash (fromIntegral $ L.length mtxIndexes) mtxIndexes
            sendRequestMessages peer $ MGetBlockTxns gbtxn
            --
            debug lg $ LG.msg ("processing prefilled txns in compact block! " ++ show bhash)
            S.drain $
                aheadly $
                S.fromList (cbPrefilledTxns cmpct) & S.mapM (\ptx -> processDeltaTx bhash $ pfTx ptx) &
                S.maxBuffer (maxTxProcessingBuffer $ nodeConfig bp2pEnv) &
                S.maxThreads (maxTxProcessingThreads $ nodeConfig bp2pEnv)
            --
            let scb = SQ.fromList $ cbShortIDs cmpct
            liftIO $
                TSH.insert
                    (prefilledShortIDsProcessing bp2pEnv)
                    bhash
                    (skey, scb, cbPrefilledTxns cmpct, mpShortTxIDMap)
            --
            -- 
            return ()

--
processBlockTransactions :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockTxns -> m ()
processBlockTransactions blockTxns = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    dbe <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        bhash = btBlockhash blockTxns
        txhashes = txHash <$> (btTransactions blockTxns)
        conn = rocksDB dbe
    (bhash', _) <- fetchBestBlock conn net
    debug lg $ LG.msg ("processing Block Transactions! " ++ show bhash)
    debug lg $ LG.msg ("processing Block Transactions! " ++ show blockTxns)
    S.drain $
        aheadly $
        S.fromList (btTransactions blockTxns) & S.mapM (processDeltaTx bhash) &
        S.maxBuffer (maxTxProcessingBuffer $ nodeConfig bp2pEnv) &
        S.maxThreads (maxTxProcessingThreads $ nodeConfig bp2pEnv)
    --
    cb <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) (bhash)
    case cb of
        Nothing -> err lg $ LG.msg $ ("Candidate block not found!: " ++ show bhash)
        Just dag -> do
            res <- liftIO $ TSH.lookup (prefilledShortIDsProcessing bp2pEnv) bhash
            case res of
                Just (skey, sids, cbpftxns, lkmap')
                    -- TODO: first insert blockTxns into `lkmap` we can find it subsequently
                 -> do
                    let lkmap =
                            HM.union
                                lkmap'
                                (HM.fromList $ fmap (\txh -> (txHashToShortId' txh skey, (txh, Nothing))) txhashes)
                    pair <- liftIO $ newIORef sids
                    validator <- liftIO $ TSH.new 10
                    mapM_
                        (\ptx -> do
                             edges <- liftIO $ DAG.getOrigEdges dag (txHash $ pfTx ptx)
                             case edges of
                                 Just (edgs, _) -> do
                                     mapM_
                                         (\ed -> do
                                              fd <- liftIO $ TSH.lookup validator ed
                                              case fd of
                                                  Just _ -> return ()
                                                  Nothing -> throw KeyValueDBInsertException -- stop not topologically sorted
                                          )
                                         edgs
                                 Nothing -> return ()
                             liftIO $ TSH.insert validator (txHash $ pfTx ptx) ()
                             cur <- liftIO $ readIORef pair
                             let (frag, rem) = SQ.splitAt (fromIntegral $ pfIndex ptx) cur
                             liftIO $ writeIORef pair rem
                             mapM_
                                 (\fx -> do
                                      let idx = HM.lookup fx lkmap
                                      case idx of
                                          Just (x, _) -> do
                                              edges <- liftIO $ DAG.getOrigEdges dag x
                                              case edges of
                                                  Just (edgs, _) -> do
                                                      mapM_
                                                          (\ed -> do
                                                               fd <- liftIO $ TSH.lookup validator ed
                                                               case fd of
                                                                   Just _ -> return ()
                                                                   Nothing -> throw KeyValueDBInsertException -- stop not topologically sorted
                                                           )
                                                          edgs
                                              return ()
                                          Nothing -> return ())
                                 frag)
                        (cbpftxns)
                Nothing -> return ()
    olddag <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) bhash'
    case olddag of
        Just dag -> do
            newdag <- liftIO $ DAG.rollOver dag txhashes defTxHash 0 emptyBranchComputeState 16 16 (+) (nextBcState)
            liftIO $ TSH.insert (candidateBlocks bp2pEnv) bhash newdag
        Nothing -> do
            newCandidateBlock bhash

processDeltaTx :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> Tx -> m ()
processDeltaTx bhash tx = do
    let bheight = 999999
        txIndex = 0
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

processCompactBlockGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
processCompactBlockGetData pr hash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    debug lg $ LG.msg $ val "processCompactBlockGetData - called."
    bp2pEnv <- getBitcoinP2P
    res <- liftIO $ TSH.lookup (ingressCompactBlocks bp2pEnv) (BlockHash hash)
    case res of
        Just bh -> do
            liftIO $ threadDelay (1000000 * 10)
            res2 <- liftIO $ TSH.lookup (ingressCompactBlocks bp2pEnv) (BlockHash hash)
            case res2 of
                Just bh2 -> return ()
                Nothing -> sendCompactBlockGetData pr hash
        Nothing -> sendCompactBlockGetData pr hash

sendCompactBlockGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
sendCompactBlockGetData pr hash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let gd = GetData $ [InvVector InvCompactBlock hash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendCompactBlockGetData: " ++ show gd
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right _ -> liftIO $ TSH.insert (ingressCompactBlocks bp2pEnv) (BlockHash hash) True
                Left (e :: SomeException) -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

newCandidateBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockHash -> m ()
newCandidateBlock hash = do
    bp2pEnv <- getBitcoinP2P
    tsdag <- liftIO $ DAG.new defTxHash (0 :: Word64) emptyBranchComputeState 16 16
    liftIO $ TSH.insert (candidateBlocks bp2pEnv) hash tsdag

newCandidateBlockChainTip :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
newCandidateBlockChainTip = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    dbe' <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        conn = rocksDB dbe'
    (hash, _) <- fetchBestBlock conn net
    tsdag <- liftIO $ DAG.new defTxHash (0 :: Word64) emptyBranchComputeState 16 16
    liftIO $ TSH.insert (candidateBlocks bp2pEnv) hash tsdag

defTxHash = fromJust $ hexToTxHash "0000000000000000000000000000000000000000000000000000000000000000"
