{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.Process.Tx where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Event as EV
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Reader
import Data.Function ((&))
import qualified Data.List as L
import Data.Maybe
import Data.Word
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Message.Sender
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Transaction.Common
import Streamly as S
import qualified Streamly.Prelude as S
import System.Logger as LG
import Xoken.NodeConfig

processTxGetData :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
processTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
    if indexUnconfirmedTx
        then do
            debug lg $ LG.msg $ val "processTxGetData - called."
            bp2pEnv <- getBitcoinP2P
            tuple <-
                liftIO $
                TSH.lookup
                    (unconfirmedTxCache bp2pEnv)
                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
            case tuple of
                Just (st, _) ->
                    unless st $ do
                        liftIO $ threadDelay (1000000 * 30)
                        tuple2 <-
                            liftIO $
                            TSH.lookup
                                (unconfirmedTxCache bp2pEnv)
                                (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                        case tuple2 of
                            Just (st2, _) -> unless st2 $ sendTxGetData pr txHash
                            Nothing -> return ()
                Nothing -> sendTxGetData pr txHash
        else do
            debug lg $ LG.msg $ val "[dag] processTxGetData - indexUnconfirmedTx False."
            return ()

addTxCandidateBlocks :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> [BlockHash] -> [TxHash] -> m ()
addTxCandidateBlocks txHash candBlockHashes depTxHashes = do
    lg <- getLogger
    debug lg $
        LG.msg $ "Appending to candidate blocks Tx " ++ show (txHash) ++ " with parent Tx's: " ++ show depTxHashes
    mapM_ (\bhash -> addTxCandidateBlock txHash bhash depTxHashes) candBlockHashes

addTxCandidateBlock :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> BlockHash -> [TxHash] -> m ()
addTxCandidateBlock txHash candBlockHash depTxHashes = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    q <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) candBlockHash
    debug lg $ LG.msg $ "Appending Tx " ++ show txHash ++ "to candidate block: " ++ show candBlockHash
    case q of
        Nothing -> err lg $ LG.msg $ ("did-not-find : " ++ show candBlockHash)
        Just dag -> do
            liftIO $ DAG.coalesce dag txHash depTxHashes 9999 (+) nextBcState
            dagT <- liftIO $ (DAG.getTopologicalSortedForest dag)
            dagP <- liftIO $ (DAG.getPrimaryTopologicalSorted dag)
            liftIO $ print $ "dag (" ++ show candBlockHash ++ "): " ++ show dagT ++ "; " ++ show dagP
            return ()

processTx :: (HasXokenNodeEnv env m, MonadIO m) => Tx -> Maybe BlockHash -> Word32 -> m ([OutPoint],[TxHash])
processTx tx bhash blkht = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let inputs = zip (txIn tx) [0 :: Word32 ..]
    let outputs = zip (txOut tx) [0 :: Word32 ..]
    let outpoints =
            map (\(b, _) -> ((outPointHash $ prevOutput b), fromIntegral $ outPointIndex $ prevOutput b)) (inputs)
    bsh <- case bhash of
            Just bh -> return bh
            Nothing -> do
                prb <- liftIO $ mkProvisionalBlockHashR bsh
                putProvisionalBlockHash prb bsh
                return prb
    inputValsOutpoints <- mapM
            (\(b, indx) -> do
                 let opHash = outPointHash $ prevOutput b
                     shortHash = getTxShortHash opHash 20
                 let opindx = fromIntegral $ outPointIndex $ prevOutput b
                 if outPointHash nullOutPoint == opHash
                     then do
                         let sval = fromIntegral $ computeSubsidy net (fromIntegral bht :: Word32) -- TODO: replace with correct  block height
                         return (sval, (shortHash, [], opHash, opindx))
                     else do
                         debug lg $
                             LG.msg
                                 ("[dag] processUnconfTransaction: inputValsOutpoint (inputs): " ++ (show (b, indx)))
                         zz <- LE.try $ zRPCDispatchGetOutpoint (prevOutput b) bhash -- Nothing for unconf
                         -- validateOutpoint timeout value should be zero, and TimeOut not to be considered an error 
                         -- even if parent tx is missing, lets proceed hoping it will become available soon. 
                         -- this assumption is crucial for async ZTXI logic.    
                         case zz of
                             Right (val, bsh, _) -> do
                                 debug lg $ LG.msg $ "[dag] processUnconfTransaction: zz: " ++ (show  zz)
                                 return (val, (shortHash, bsh, opHash, opindx))
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
    let opCount = fromIntegral $ L.length outputs
    ovs <- 
        mapM
            (\(opt, oindex) -> do
                 debug lg $ LG.msg $ "Inserting UTXO : " ++ show (txHash tx, oindex)
                 debug lg $ LG.msg $ "[dag] processUnconfTransaction: Inserting UTXO : " ++ show (txHash tx, oindex)
                 let zut =
                         ZtxiUtxo
                             (txHash tx)
                             oindex
                             [bsh] -- [bhash] if already present then ADD to the existing list of BlockHashes
                             (fromIntegral blkht) -- (fromIntegral blkht) could be 999999 for unconf
                             outpoints
                             []
                             (fromIntegral $ outValue opt)
                             opCount
                 res <- LE.try $ putOutput (OutPoint (txHash tx) oindex) zut
                 case res of
                     Right _ -> return (zut)
                     Left (e :: SomeException) -> do
                         err lg $ LG.msg $ "Error: INSERTing into outputs: " ++ show e
                         throw KeyValueDBInsertException)
            outputs
    let ipSum = sum $ fst <$> inputValsOutpoints
        opSum = sum $ outValue <$> txOut tx
    when (ipSum < opSum) $ do
            debug lg $ LG.msg $ " ZUT value mismatch " ++ (show ipSum) ++ " " ++ (show opSum)
            throw InvalidTxSatsValueException
    vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txHash tx)
    case vall of
        Just ev -> liftIO $ EV.signal ev
        Nothing -> return ()
    let outpts = map (\(tid, idx) -> OutPoint tid idx) outpoints
    let parentTxns =
            mapMaybe
                (\(_, (_, bsh, ophs, _)) ->
                     case bsh of
                         [] -> Just ophs
                         _ -> Nothing)
                inputValsOutpoints
    return (outpts, parentTxns)

processUnconfTransaction :: (HasXokenNodeEnv env m, MonadIO m) => Tx -> m ([TxHash])
processUnconfTransaction tx = do
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    bbn <- fetchBestBlock
    let (bsh, bht) = (headerHash $ nodeHeader bbn, nodeHeight bbn)
    debug lg $ LG.msg $ "processing Unconf Tx " ++ show (txHash tx)
    putTx (txHash tx) tx
    (_, parentTxns) <- processTx tx Nothing
    debug lg $ LG.msg $ "processing Unconf Tx " ++ show (txHash tx) ++ ": end of processing signaled"
    return parentTxns

{- UNUSED? txind isn't used anywhere in processConfTransaction -}
processConfTransaction ::
       (HasXokenNodeEnv env m, MonadIO m) => Tx -> BlockHash -> Word32 -> Word32 -> m ([OutPoint])
processConfTransaction tx bhash blkht txind = do
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx)
    (outpts,_) <- processTx tx (Just (bhash, blkht))
    debug lg $ LG.msg $ "processing Tx " ++ show (txHash tx) ++ ": end of processing signaled"
    return (outpts)

processTxStream :: (HasXokenNodeEnv env m, MonadIO m) => (Tx, BlockInfo, Int) -> m ()
processTxStream (tx, binfo, txIndex) = do
    let bhash = biBlockHash binfo
        bheight = biBlockHeight binfo
    lg <- getLogger
    res <- LE.try $ zRPCDispatchTxValidate processConfTransaction tx bhash bheight (fromIntegral txIndex)
    case res of
        Right () -> return ()
        Left TxIDNotFoundException -> throw TxIDNotFoundException
        Left KeyValueDBInsertException -> do
            err lg $ LG.msg $ val "[ERROR] KeyValueDBInsertException"
            throw KeyValueDBInsertException
        Left e -> do
            err lg $ LG.msg ("[ERROR] Unhandled exception!" ++ show e)
            throw e

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
