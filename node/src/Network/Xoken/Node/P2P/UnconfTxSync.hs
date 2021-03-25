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
    --, runEpochSwitcher
    , addTxCandidateBlocks
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Event as EV
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Reader
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as L
import Data.EnumBitSet (fromEnums, (.|.))
import Data.Maybe
import Data.Serialize
import Data.Word
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Exception
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Script
import Network.Xoken.Transaction.Common
import System.Logger as LG
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
            debug lg $ LG.msg $ val "processTxGetData - called."
            bp2pEnv <- getBitcoinP2P
            tuple <-
                liftIO $
                TSH.lookup
                    (unconfirmedTxCache bp2pEnv)
                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
            case tuple of
                Just (st, _) ->
                    if st == False
                        then do
                            liftIO $ threadDelay (1000000 * 30)
                            tuple2 <-
                                liftIO $
                                TSH.lookup
                                    (unconfirmedTxCache bp2pEnv)
                                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                            case tuple2 of
                                Just (st2, _) ->
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

{- UNUSED?
runEpochSwitcher :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEpochSwitcher =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        tm <- liftIO $ getCurrentTime
        let hour = todHour $ timeToTimeOfDay $ utctDayTime tm
            minute = todMin $ timeToTimeOfDay $ utctDayTime tm
            epoch =
                case hour `mod` 3 of
                    0 -> Epoch0
                    1 -> Epoch1
                    2 -> Epoch2
        liftIO $ atomically $ writeTVar (epochType bp2pEnv) epoch
        if minute == 0
            then do
                let delcf = 
                --R.dropCF rkdb op_cf
                --R.dropCF rkdb tx_cf
                --o_ptr <- R.createCF rkdb config op_cf
                --t_ptr <- R.createCF rkdb config op_cf
                liftIO $ threadDelay (1000000 * 60 * 60)
            else liftIO $ threadDelay (1000000 * 60 * (60 - minute))
        return ()

isNotConfirmed :: TxHash -> IO Bool
isNotConfirmed txHash = return False

coalesceUnconfTransaction ::
       (TSDirectedAcyclicGraph TxHash Word64 IncrementalBranch) -> TxHash -> [TxHash] -> Word64 -> IO ()
coalesceUnconfTransaction dag txhash hashes sats = do
    print $ "coalesceUnconfTransaction called for tx: " ++ show (txhash)
    unconfHashes <- filterM (isNotConfirmed) hashes
    DAG.coalesce dag txhash unconfHashes sats (+) nextBcState
-}
processUnconfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> m ([TxHash])
processUnconfTransaction tx = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    bbn <- fetchBestBlock
    let (bsh, bht) = (headerHash $ nodeHeader bbn, nodeHeight bbn)
    prb <- liftIO $ mkProvisionalBlockHashR bsh
    putProvisionalBlockHash prb bsh
    debug lg $ LG.msg $ "processing Unconf Tx " ++ show (txHash tx)
    debug lg $ LG.msg $ "[dag] processUnconfTransaction: processing Unconf Tx " ++ show (txHash tx)
    putTx (txHash tx) tx
    let inputs = zip (txIn tx) [0 :: Word32 ..]
    let outputs = zip (txOut tx) [0 :: Word32 ..]
    let outpoints =
            map (\(b, _) -> ((outPointHash $ prevOutput b), fromIntegral $ outPointIndex $ prevOutput b)) (inputs)
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx) ++ ": end of processing signaled"
    inputValsOutpoints <-
        mapM
            (\(b, indx) -> do
                 let opHash = outPointHash $ prevOutput b
                     shortHash = getTxShortHash opHash 20
                 let opindx = fromIntegral $ outPointIndex $ prevOutput b
                 if (outPointHash nullOutPoint) == opHash
                     then do
                         let sval = fromIntegral $ computeSubsidy net $ (fromIntegral bht :: Word32) -- TODO: replace with correct  block height
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
                             Right (val, bsh, _,scr) -> do
                                 let context = Ctx
                                                { script_flags = fromEnums [GENESIS, UTXO_AFTER_GENESIS, VERIFY_MINIMALIF]
                                                                    .|. mandatoryScriptFlags .|. standardScriptFlags
                                                , consensus = True
                                                , sig_checker_data = Just $ SigCheckerData net tx (fromIntegral indx) val
                                                }
                                     scrd = decode scr
                                     scrdi = decode (scriptInput b)
                                 case (scrd, scrdi) of
                                     (Left e,_) -> debug lg $ LG.msg $ "[SCRIPT] " ++ e
                                     (_,Left e) -> debug lg $ LG.msg $ "[SCRIPT] " ++ e
                                     (Right sc, Right sci) -> debug lg $ LG.msg $ "[SCRIPT] "
                                                                                ++ show (error_msg (verifyScriptWith context empty_env sci sc))
                                                                                ++ "; for tx: "
                                                                                ++ show (txHash tx)
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
    let opCount = fromIntegral $ L.length outputs
    ovs <-
        mapM
            (\(opt, oindex) -> do
                 debug lg $ LG.msg $ "Inserting UTXO : " ++ show (txHash tx, oindex)
                 debug lg $ LG.msg $ "[dag] processUnconfTransaction: Inserting UTXO : " ++ show (txHash tx, oindex)
                 let zut =
                         ZtxiUtxo
                             (txHash tx)
                             (oindex)
                             [prb] -- if already present then ADD to the existing list of BlockHashes
                             (fromIntegral 9999999)
                             outpoints
                             []
                             (fromIntegral $ outValue opt)
                             opCount
                             (scriptOutput opt)
                 res <- LE.try $ putOutput (OutPoint (txHash tx) oindex) zut
                 case res of
                     Right _ -> return (zut)
                     Left (e :: SomeException) -> do
                         err lg $ LG.msg $ "Error: INSERTing into outputs: " ++ show e
                         throw KeyValueDBInsertException)
            outputs
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
    let parentTxns =
            catMaybes $
            map
                (\(_, (_, bsh, ophs, _)) ->
                     case bsh of
                         [] -> Just ophs
                         _ -> Nothing)
                inputValsOutpoints
    return (parentTxns)

{- UNUSED?
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
-}
addTxCandidateBlocks :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TxHash -> [BlockHash] -> [TxHash] -> m ()
addTxCandidateBlocks txHash candBlockHashes depTxHashes = do
    lg <- getLogger
    debug lg $
        LG.msg $ "Appending to candidate blocks Tx " ++ show (txHash) ++ " with parent Tx's: " ++ show depTxHashes
    mapM_ (\bhash -> addTxCandidateBlock txHash bhash depTxHashes) candBlockHashes

addTxCandidateBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TxHash -> BlockHash -> [TxHash] -> m ()
addTxCandidateBlock txHash candBlockHash depTxHashes = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    q <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) candBlockHash
    debug lg $ LG.msg $ "Appending Tx " ++ show txHash ++ "to candidate block: " ++ show candBlockHash
    case q of
        Nothing -> err lg $ LG.msg $ ("did-not-find : " ++ show candBlockHash)
        Just dag -> do
            liftIO $ DAG.coalesce dag txHash depTxHashes 9999 (+) updateMerkleBranch
            dagT <- liftIO $ (DAG.getTopologicalSortedForest dag)
            dagP <- liftIO $ (DAG.getPrimaryTopologicalSorted dag)
            liftIO $ print $ "dag (" ++ show candBlockHash ++ "): " ++ show dagT ++ "; " ++ show dagP
            return ()
