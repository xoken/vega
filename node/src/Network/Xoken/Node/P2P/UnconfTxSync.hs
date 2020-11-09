{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.UnconfTxSync
    ( processUnconfTransaction
    , processTxGetData
    , runEpochSwitcher
    , addTxCandidateBlocks
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race_)
import Control.Concurrent.Async.Lifted as LA (async)
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
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Sequence ((|>))
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.LocalTime
import Data.Word
import qualified Database.RocksDB as R
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
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.WorkerDispatcher
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig

processTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
processTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
    if indexUnconfirmedTx == False
        then do
            debug lg $ LG.msg $ val "[dag] processTxGetData - indexUnconfirmedTx False."
            return ()
        else do
            let net = bitcoinNetwork $ nodeConfig bp2pEnv
            debug lg $ LG.msg $ val "processTxGetData - called."
            debug lg $ LG.msg $ val "[dag] processTxGetData - called."
            bp2pEnv <- getBitcoinP2P
            tuple <-
                liftIO $
                TSH.lookup
                    (unconfirmedTxCache bp2pEnv)
                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
            case tuple of
                Just (st, fh) ->
                    if st == False
                        then do
                            liftIO $ threadDelay (1000000 * 30)
                            tuple2 <-
                                liftIO $
                                TSH.lookup
                                    (unconfirmedTxCache bp2pEnv)
                                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                            case tuple2 of
                                Just (st2, fh2) ->
                                    if st2 == False
                                        then sendTxGetData pr txHash
                                        else return ()
                                Nothing -> return ()
                        else return ()
                Nothing -> sendTxGetData pr txHash

sendTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
sendTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let gd = GetData $ [InvVector InvTx txHash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendTxGetData: " ++ show gd
    debug lg $ LG.msg $ "[dag] sendTxGetData: " ++ show gd
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right _ ->
                    liftIO $
                    TSH.insert
                        (unconfirmedTxCache bp2pEnv)
                        (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                        (False, TxHash txHash)
                Left (e :: SomeException) -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
            debug lg $ LG.msg $ "[dag] sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

runEpochSwitcher :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEpochSwitcher =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        tm <- liftIO $ getCurrentTime
        let conn = rocksDB $ dbe'
            hour = todHour $ timeToTimeOfDay $ utctDayTime tm
            minute = todMin $ timeToTimeOfDay $ utctDayTime tm
            epoch =
                case hour `mod` 2 of
                    0 -> True
                    1 -> False
        liftIO $ atomically $ writeTVar (epochType bp2pEnv) epoch
        if minute == 0
            then do
                let (op_cf, tx_cf) =
                        case epoch of
                            True -> ("ep_outputs_odd", "ep_transactions_odd")
                            False -> ("ep_outputs_even", "ep_transactions_even")
                --R.dropCF rkdb op_cf
                --R.dropCF rkdb tx_cf
                --o_ptr <- R.createCF rkdb config op_cf
                --t_ptr <- R.createCF rkdb config op_cf
                liftIO $ threadDelay (1000000 * 60 * 60)
            else liftIO $ threadDelay (1000000 * 60 * (60 - minute))
        return ()

insertTxIdOutputs :: (HasLogger m, MonadIO m) => R.DB -> R.ColumnFamily -> (TxHash, Word32) -> TxOut -> m ()
insertTxIdOutputs conn cf (txid, outputIndex) txOut = do
    lg <- getLogger
    res <- liftIO $ try $ putDBCF conn cf (txid, outputIndex) txOut
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into " ++ (show cf) ++ ": " ++ show e
            throw KeyValueDBInsertException

isNotConfirmed :: TxHash -> IO Bool
isNotConfirmed txHash = return False

coalesceUnconfTransaction :: (TSDirectedAcyclicGraph TxHash Word64) -> TxHash -> [TxHash] -> Word64 -> IO ()
coalesceUnconfTransaction dag txhash hashes sats = do
    print $ "coalesceUnconfTransaction called for tx: " ++ show (txhash)
    unconfHashes <- filterM (isNotConfirmed) hashes
    DAG.coalesce dag txhash unconfHashes sats (+)

processUnconfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> m ([TxHash])
processUnconfTransaction tx = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = rocksDB $ dbe'
        cf = rocksCF dbe'
    debug lg $ LG.msg $ "processing Unconf Tx " ++ show (txHash tx)
    debug lg $ LG.msg $ "[dag] processUnconfTransaction: processing Unconf Tx " ++ show (txHash tx)
    let inputs = zip (txIn tx) [0 :: Word32 ..]
    let outputs = zip (txOut tx) [0 :: Word32 ..]
 --
    let outpoints =
            map (\(b, _) -> ((outPointHash $ prevOutput b), fromIntegral $ outPointIndex $ prevOutput b)) (inputs)
 --
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx) ++ ": end of processing signaled"
    cf' <- liftIO $ TSH.lookup cf (getEpochTxOutCF epoch)
    case cf' of
        Just cf'' -> do
            mapM_
                (\(txOut, oind) -> do
                     debug lg $ LG.msg $ "inserting output " ++ show txOut
                     debug lg $ LG.msg $ "[dag] processUnconfTransaction: inserting output " ++ show txOut
                     insertTxIdOutputs conn cf'' (txHash tx, oind) (txOut))
                outputs
        Nothing -> do
            debug lg $ LG.msg $ val "[dag] validateOutpoint: Error: cf Nothing"
            return () -- ideally should be unreachable
    inputValsOutpoints <-
        mapM
            (\(b, indx) -> do
                 let opHash = outPointHash $ prevOutput b
                     shortHash = getTxShortHash opHash 20
                 let opindx = fromIntegral $ outPointIndex $ prevOutput b
                 if (outPointHash nullOutPoint) == opHash
                     then do
                         bb <- fetchBestBlock conn net
                         let sval = fromIntegral $ computeSubsidy net $ (fromIntegral (snd bb) :: Word32) -- TODO: replace with correct  block height
                         return (sval, (shortHash, [], opHash, opindx))
                     else do
                         debug lg $
                             LG.msg
                                 ("[dag] processUnconfTransaction: inputValsOutpoint (inputs): " ++ (show $ (b, indx)))
                         zz <- LE.try $ zRPCDispatchGetOutpoint (prevOutput b) (Nothing) -- bhash
                         -- validateOutpoint timeout value should be zero, and TimeOut not to be considered an error 
                         -- even if parent tx is missing, lets proceed hoping it will become available soon. 
                         -- this assumption is crucial for async ZTXI logic.    
                         case zz of
                             Right (val, bsh) -> do
                                 debug lg $ LG.msg $ "[dag] processUnconfTransaction: zz: " ++ (show $ zz)
                                 return (val, (shortHash, bsh, opHash, opindx))
                             Left (e :: SomeException) -> do
                                 err lg $
                                     LG.msg $
                                     "Error: [pCT calling gSVFO] WHILE Processing Unconf TxID " ++
                                     show (txHashToHex $ txHash tx) ++
                                     ", getting value for dependent input (TxID,Index): (" ++
                                     show (txHashToHex $ outPointHash (prevOutput b)) ++
                                     ", " ++ show (outPointIndex $ prevOutput b) ++ ")" ++ (show e)
                                 throw e)
            (inputs)
    -- insert UTXO/s
    cf' <- liftIO $ TSH.lookup cf ("outputs")
    liftIO $ print $ "cf: " ++ show cf'
    ovs <-
        mapM
            (\(opt, oindex) -> do
                 debug lg $ LG.msg $ "Inserting UTXO : " ++ show (txHash tx, oindex)
                 debug lg $ LG.msg $ "[dag] processUnconfTransaction: Inserting UTXO : " ++ show (txHash tx, oindex)
                 let zut =
                         ZtxiUtxo
                             (txHash tx)
                             (oindex)
                             [] -- if already present then ADD to the existing list of BlockHashes
                             (fromIntegral 9999999)
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
    -- TODO : cleanup this!!
    --
    -- debug lg $ LG.msg $ "[dag] BEFORE COALESCE for txid" ++ show (txHash tx)
    -- let coalesceInp =
    --         catMaybes $
    --         fmap
    --             (\(val, (_, bh, txh, _)) ->
    --                  case bh of
    --                      [] -> Just (txh, val)
    --                      (x:_) -> Nothing)
    --             inputValsOutpoints
    --     txhs = fmap fst coalesceInp
    --     vs = sum $ fmap snd coalesceInp
    -- liftIO $
    --     TSH.mapM_
    --         (\(bsh, dag) -> do
    --              print $ "TXHASH: " ++ show (txHash tx) ++ "; EDGES: " ++ show txhs
    --              LA.async $ DAG.coalesce dag (txHash tx) txhs vs (+)
    --              putStrLn "BEFORE getTopologicalSortedForest DAG: "
    --              dagT <- DAG.getTopologicalSortedForest dag
    --              debug lg $ LG.msg $ "DAG: " ++ show (dagT)
    --              return ())
    --         (candidateBlocks bp2pEnv)
    -- debug lg $ LG.msg $ "[dag] AFTER COALESCE for txid" ++ show (txHash tx)
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
    -- let outpts = map (\(tid, idx) -> OutPoint tid idx) outpoints
    let parentTxns = map (\x -> outPointHash $ prevOutput x) (txIn tx)
    return (parentTxns)

getSatsValueFromEpochOutpoint ::
       R.DB
    -> Bool
    -> (TSH.TSHashTable TxHash EV.Event)
    -> Logger
    -> Network
    -> OutPoint
    -> Int
    -> TSH.TSHashTable String R.ColumnFamily
    -> IO (Text, C.ByteString, Int64)
getSatsValueFromEpochOutpoint rkdb epoch txSync lg net outPoint waitSecs cfs = do
    cf <- liftIO $ TSH.lookup cfs (getEpochTxCF epoch)
    res <-
        liftIO $
        try $
        getDBCF rkdb (fromJust cf) (txHashToHex $ outPointHash outPoint, fromIntegral $ outPointIndex outPoint :: Int32)
    case res of
        Right Nothing -> do
            debug lg $
                LG.msg $
                "[Unconfirmed] Tx not found: " ++
                (show $ txHashToHex $ outPointHash outPoint) ++ "... waiting for event"
            valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
            event <-
                case valx of
                    Just evt -> return evt
                    Nothing -> EV.new
            liftIO $ TSH.insert txSync (outPointHash outPoint) event
            tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
            if tofl == False
                then do
                    liftIO $ TSH.delete txSync (outPointHash outPoint)
                    debug lg $
                        LG.msg $ "[Unconfirmed] TxIDNotFoundException: " ++ (show $ txHashToHex $ outPointHash outPoint)
                    throw TxIDNotFoundException
                else getSatsValueFromEpochOutpoint rkdb epoch txSync lg net outPoint waitSecs cfs
        Right (Just sv) -> return sv
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getSatsValueFromEpochOutpoint: " ++ show e
            throw e

convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (T.pack s)
    (T.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr

addTxCandidateBlocks :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TxHash -> [BlockHash] -> [TxHash] -> m ()
addTxCandidateBlocks txHash candBlockHashes depTxHashes = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    --epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = rocksDB $ dbe'
        cfs = rocksCF dbe'
    debug lg $ LG.msg $ "Appending Tx to candidate block: " ++ show (txHash)
    mapM_
        (\bhash -> addTxCandidateBlock txHash bhash depTxHashess)
        candBlockHashes

addTxCandidateBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TxHash -> BlockHash -> [TxHash] -> m ()
addTxCandidateBlock txHash candBlockHash depTxHashes = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    q <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) (bhash)
    debug lg $ LG.msg $ "Appending Tx " ++ show txHash ++ "to candidate block: " ++ show candBlockHash
    case q of
        Nothing -> err lg $ LG.msg $ ("did-not-find : " ++ show candBlockHash)
        Just dag -> do
            liftIO $ DAG.coalesce dag txHash depTxHashes 0 (+)
            return ()

