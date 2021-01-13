{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.Process.Block where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently_)
import qualified Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Reader
import Control.Monad.STM
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import Data.Function ((&))
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Sequence as SQ
import Data.Serialize
import Data.Time.Calendar
import Data.Time.Clock
import Data.Word
import qualified Network.Socket as NS
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Network.Message
import Network.Xoken.Node.P2P.MessageSender
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Exception
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Process.Tx
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Transaction.Common
import Streamly as S
import qualified Streamly.Prelude as S
import System.Logger as LG
import Xoken.NodeConfig as NC


newCandidateBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockHash -> m ()
newCandidateBlock hash = do
    bp2pEnv <- getBitcoinP2P
    tsdag <- liftIO $ DAG.new defTxHash (0 :: Word64) emptyBranchComputeState 16 16
    liftIO $ TSH.insert (candidateBlocks bp2pEnv) hash tsdag

newCandidateBlockChainTip :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
newCandidateBlockChainTip = do
    bp2pEnv <- getBitcoinP2P
    bbn <- fetchBestBlock
    let hash = headerHash $ nodeHeader bbn
    tsdag <- liftIO $ DAG.new defTxHash (0 :: Word64) emptyBranchComputeState 16 16
    liftIO $ TSH.insert (candidateBlocks bp2pEnv) hash tsdag

defTxHash = fromJust $ hexToTxHash "0000000000000000000000000000000000000000000000000000000000000000"

processBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => DefBlock -> m ()
processBlock dblk = do
    lg <- getLogger
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
        Nothing -> return ()
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
                                 Just _ -> True
                                 Nothing -> False)
                        cmpctTxLst
            let usedTxnMap = HM.fromList usedTxns
            mapM
                (\(sid, (txid, rt)) -> do
                     let fd = HM.lookup sid usedTxnMap
                     case fd of
                         Just _ -> return ()
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
    let bhash = btBlockhash blockTxns
        txhashes = txHash <$> (btTransactions blockTxns)
    bbn <- fetchBestBlock
    let bhash' = headerHash $ nodeHeader bbn
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
    debug lg $ LG.msg $ val "processCompactBlockGetData - called."
    bp2pEnv <- getBitcoinP2P
    res <- liftIO $ TSH.lookup (ingressCompactBlocks bp2pEnv) (BlockHash hash)
    case res of
        Just _ -> do
            liftIO $ threadDelay (1000000 * 10)
            res2 <- liftIO $ TSH.lookup (ingressCompactBlocks bp2pEnv) (BlockHash hash)
            case res2 of
                Just _ -> return ()
                Nothing -> sendCompactBlockGetData pr hash
        Nothing -> sendCompactBlockGetData pr hash
