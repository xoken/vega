{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Xoken.Node.WorkerDispatcher
    ( module Network.Xoken.Node.WorkerDispatcher
    ) where

import Arivi.P2P.P2PEnv
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types
import Codec.Serialise
import qualified Codec.Serialise as CBOR
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, race)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Loops
import Control.Monad.Trans.Class
import Data.Aeson as A
import Data.Binary as DB
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Functor (($>))
import Data.IORef
import Data.Int
import Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize
import qualified Data.Serialize as S
import qualified Data.Set as DS
import Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.X509.CertificateStore
import GHC.Base as GHCB
import GHC.Generics
import qualified Network.Simple.TCP.TLS as TLS
import Network.Socket
import Network.Socket.ByteString as SB (recv, sendAll)
import qualified Network.TLS as NTLS
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction.Common
import Prelude as P
import StmContainers.Map as SM
import System.Logger as LG
import Text.Printf
import Xoken.NodeConfig as NC

zRPCDispatchTxValidate ::
       (HasXokenNodeEnv env m, MonadIO m)
    => (Tx -> BlockHash -> Word32 -> Word32 -> m ([OutPoint]))
    -> Tx
    -> BlockHash
    -> Word32
    -> Word32
    -> m ()
zRPCDispatchTxValidate selfFunc tx bhash bheight txindex = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ "encoded Tx : " ++ (show $ txHash tx)
    let lexKey = getTxShortHash (txHash tx) 8
    worker <- getRemoteWorker lexKey TxValidation
    case worker of
        Nothing
            -- liftIO $ print "zRPCDispatchTxValidate - SELF"
         -> do
            res <- LE.try $ selfFunc tx bhash bheight txindex
            case res of
                Right outpts -> do
                    mapM_
                        (\opt -> do
                             ress <- liftIO $ TSH.lookup (pruneUtxoQueue bp2pEnv) bhash
                             case ress of
                                 Just blktxq -> liftIO $ TSH.insert blktxq opt ()
                                 Nothing -> do
                                     debug lg $ LG.msg ("New Prune queue " ++ show bhash)
                                     opq <- liftIO $ TSH.new 1
                                     liftIO $ TSH.insert (pruneUtxoQueue bp2pEnv) bhash opq)
                        outpts
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("[ERROR] processConfTransaction " ++ show e)
                    throw e
        Just wrk
            -- liftIO $ print $ "zRPCDispatchTxValidate - " ++ show wrk
         -> do
            let mparam = ZValidateTx bhash bheight txindex tx
            resp <- zRPCRequestDispatcher mparam wrk
            -- liftIO $ print $ "zRPCRequestDispatcher - RESPONSE " ++ show resp
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZValidateTxResp val ->
                                    if val
                                        then return ()
                                        else throw InvalidMessageTypeException
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex
    return ()

zRPCDispatchProvisionalBlockHash :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> BlockHash -> m ()
zRPCDispatchProvisionalBlockHash bh pbh = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        rkdb = rocksDB dbe'
    mapM_
        (\wrk -> do
             case wrk of
                 SelfWorker {..} -> do
                     pcf <- liftIO $ TSH.lookup (rocksCF dbe') "provisional_blockhash"
                     case pcf of
                         Just pc -> do
                             putDBCF rkdb pc bh pbh
                             putDBCF rkdb pc pbh bh
                             updatePredecessors
                         Nothing -> return ()
                 RemoteWorker {..} -> do
                     let mparam = ZProvisionalBlockHash bh pbh
                     resp <- zRPCRequestDispatcher mparam wrk
                     case zrsPayload resp of
                         Right spl -> do
                             case spl of
                                 Just pl ->
                                     case pl of
                                         ZProvisionalBlockHashResp -> return ()
                                 Nothing -> throw InvalidMessageTypeException
                         Left er -> do
                             err lg $ LG.msg $ "decoding zRPCDispatchProvisionalBlockHash error resp : " ++ show er
                             let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                             throw mex)
        (wrkrs)

zRPCDispatchGetOutpoint :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> Maybe BlockHash -> m (Word64, [BlockHash], Word32)
zRPCDispatchGetOutpoint outPoint bhash = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ val "[dag] zRPCDispatchGetOutpoint"
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let opIndex = outPointIndex $ outPoint
        lexKey = getTxShortHash (outPointHash outPoint) 8
    worker <- getRemoteWorker lexKey GetOutpoint
    case worker of
        Nothing
            -- liftIO $ print "zRPCDispatchGetOutpoint - SELF"
         -> do
            debug lg $ LG.msg $ val "[dag] zRPCDispatchGetOutpoint: Worker Nothing"
            val <-
                validateOutpoint
                    (outPoint)
                    bhash
                    (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
            return val
        Just wrk
            -- liftIO $ print $ "zRPCDispatchGetOutpoint - " ++ show wrk
         -> do
            debug lg $ LG.msg $ "[dag] zRPCDispatchGetOutpoint: Worker: " ++ show wrk
            let mparam = ZGetOutpoint (outPointHash outPoint) (outPointIndex outPoint) bhash
            resp <- zRPCRequestDispatcher mparam wrk
            -- liftIO $ print $ "zRPCDispatchGetOutpoint - RESPONSE " ++ show resp
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZGetOutpointResp val scr bsh ht -> return (val, bsh, ht)
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex

zRPCDispatchUpdateOutpoint :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> Word32 -> m (Word32)
zRPCDispatchUpdateOutpoint outPoint bhash height = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ val "[dag] zRPCDispatchUpdateOutpoint"
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let opIndex = outPointIndex $ outPoint
        lexKey = getTxShortHash (outPointHash outPoint) 8
    worker <- getRemoteWorker lexKey GetOutpoint
    case worker of
        Nothing
            -- liftIO $ print "zRPCDispatchGetOutpoint - SELF"
         -> do
            debug lg $ LG.msg $ val "[dag] zRPCDispatchGetOutpoint: Worker Nothing"
            val <-
                updateOutpoint
                    outPoint
                    bhash
                    height
            return val
        Just wrk
            -- liftIO $ print $ "zRPCDispatchGetOutpoint - " ++ show wrk
         -> do
            debug lg $ LG.msg $ "[dag] zRPCDispatchGetOutpoint: Worker: " ++ show wrk
            let mparam = ZUpdateOutpoint (outPointHash outPoint) (outPointIndex outPoint) bhash height
            resp <- zRPCRequestDispatcher mparam wrk
            -- liftIO $ print $ "zRPCDispatchGetOutpoint - RESPONSE " ++ show resp
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZUpdateOutpointResp upd -> return upd
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Tx updation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex


zRPCDispatchBlocksTxsOutputs :: (HasXokenNodeEnv env m, MonadIO m) => [BlockHash] -> m ()
zRPCDispatchBlocksTxsOutputs blockHashes = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    mapM_
        (\wrk -> do
             case wrk of
                 SelfWorker {..} -> pruneBlocksTxnsOutputs blockHashes
                 RemoteWorker {..} -> do
                     let mparam = ZPruneBlockTxOutputs blockHashes
                     resp <- zRPCRequestDispatcher mparam wrk
                     case zrsPayload resp of
                         Right spl -> do
                             case spl of
                                 Just pl ->
                                     case pl of
                                         ZPruneBlockTxOutputsResp -> return ()
                                 Nothing -> throw InvalidMessageTypeException
                         Left er -> do
                             err lg $ LG.msg $ "decoding PruneBlockTxOutputs error resp : " ++ show er
                             let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                             throw mex)
        (wrkrs)

-- zRPCDispatchTraceOutputs :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> Bool -> Int -> m (Bool)
-- zRPCDispatchTraceOutputs outPoint bhash isPrevFresh htt = do
--     dbe' <- getDB
--     bp2pEnv <- getBitcoinP2P
--     lg <- getLogger
--     let net = bitcoinNetwork $ nodeConfig bp2pEnv
--         opIndex = outPointIndex outPoint
--         lexKey = getTxShortHash (outPointHash outPoint) 8
--     worker <- getRemoteWorker lexKey TraceOutputs
--     case worker of
--         Nothing -> do
--             pruneSpentOutputs (outPointHash outPoint, opIndex) bhash isPrevFresh htt
--         Just wrk -> do
--             let mparam = ZTraceOutputs (outPointHash outPoint) (outPointIndex outPoint) bhash isPrevFresh htt
--             resp <- zRPCRequestDispatcher mparam wrk
--             case zrsPayload resp of
--                 Right spl -> do
--                     case spl of
--                         Just pl ->
--                             case pl of
--                                 ZTraceOutputsResp flag -> return (flag)
--                         Nothing -> throw InvalidMessageTypeException
--                 Left er -> do
--                     err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
--                     let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
--                     throw mex
zRPCDispatchNotifyNewBlockHeader :: (HasXokenNodeEnv env m, MonadIO m) => [ZBlockHeader] -> BlockNode -> m ()
zRPCDispatchNotifyNewBlockHeader headers bn = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    mapM_
        (\wrk -> do
             case wrk of
                 SelfWorker {..} -> return ()
                 RemoteWorker {..} -> do
                     let mparam = ZNotifyNewBlockHeader headers bn
                     resp <- zRPCRequestDispatcher mparam wrk
                     case zrsPayload resp of
                         Right spl -> do
                             case spl of
                                 Just pl ->
                                     case pl of
                                         ZNotifyNewBlockHeaderResp -> return ()
                                 Nothing -> throw InvalidMessageTypeException
                         Left er -> do
                             err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
                             let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                             throw mex)
        (wrkrs)

--
zRPCDispatchUnconfirmedTxValidate :: (HasXokenNodeEnv env m, MonadIO m) => (Tx -> m ([TxHash])) -> Tx -> m ([TxHash])
zRPCDispatchUnconfirmedTxValidate selfFunc tx = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ "encoded Tx : " ++ (show $ txHash tx)
    let lexKey = getTxShortHash (txHash tx) 8
    worker <- getRemoteWorker lexKey TxValidation
    case worker of
        Nothing
            -- liftIO $ print "zRPCDispatchUnconfirmedTxValidate - SELF"
         -> do
            debug lg $ LG.msg $ "zRPCDispatchUnconfirmedTxValidate Nothing: " ++ (show $ txHash tx)
            res <- LE.try $ selfFunc tx
            case res of
                Right val -> do
                    return (val)
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("[ERROR] processUnconfirmedTransaction " ++ show e)
                    throw e
        Just wrk
            -- liftIO $ print $ "zRPCDispatchUnconfirmedTxValidate - " ++ show wrk
         -> do
            debug lg $ LG.msg $ "zRPCDispatchUnconfirmedTxValidate (Just): " ++ (show $ txHash tx)
            let mparam = ZValidateUnconfirmedTx tx
            resp <- zRPCRequestDispatcher mparam wrk
            -- liftIO $ print $ "zRPCRequestDispatcher - RESPONSE " ++ show resp
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZValidateUnconfirmedTxResp dpTx -> return (dpTx)
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Unconfirmed Tx validation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex

--
zRPCRequestDispatcher :: (HasXokenNodeEnv env m, MonadIO m) => ZRPCRequestParam -> Worker -> m (ZRPCResponse)
zRPCRequestDispatcher param wrk = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    sem <- liftIO $ newEmptyMVar
    mid <-
        liftIO $
        modifyMVar
            (woMsgCounter wrk)
            (\a -> do
                 if a == maxBound
                     then return (1, 1)
                     else return (a + 1, a + 1))
    -- liftIO $ print $ "=====> " ++ (show mid)
    let msg = ZRPCRequest mid param
    debug lg $ LG.msg $ "zRPCRequestDispatcher dispatching to worker: " ++ (show param)
    liftIO $ TSH.insert (woMsgMultiplexer wrk) mid sem
    -- debug lg $ LG.msg $ "dispatching to worker 2: " ++ (show wrk)
    liftIO $ sendMessage (woSocket wrk) (woWriteLock wrk) (CBOR.serialise msg)
    -- debug lg $ LG.msg $ "dispatching to worker 3: " ++ (show wrk)
    resp <- liftIO $ takeMVar sem
    -- debug lg $ LG.msg $ "dispatching to worker 4: " ++ (show wrk)
    liftIO $ TSH.delete (woMsgMultiplexer wrk) mid
    return resp

getRemoteWorker :: (HasXokenNodeEnv env m, MonadIO m) => Word32 -> NodeRole -> m (Maybe Worker)
getRemoteWorker shardingLex role = do
    bp2pEnv <- getBitcoinP2P
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
    let workers =
            P.filter
                (\w -> do
                     let rl =
                             case w of
                                 SelfWorker {..} -> selfRoles
                                 RemoteWorker {..} -> woRoles
                     isJust $ L.findIndex (\x -> x == role) rl)
                wrkrs
    lg <- getLogger
    debug lg $ LG.msg $ "Workers : " ++ (show workers) ++ " role: " ++ (show role)
    let nx = fromIntegral $ ((fromIntegral shardingLex) + (shardingHashSecretSalt $ nodeConfig bp2pEnv))
    let wind = nx `mod` (L.length workers)
        wrk = workers !! wind
    case wrk of
        SelfWorker {..}
            -- liftIO $ print "^^^"
         -> do
            return Nothing
        RemoteWorker {..}
            -- liftIO $ print ">>>"
         -> do
            return $ Just wrk

-- pruneSpentOutputs :: (HasXokenNodeEnv env m, MonadIO m) => (TxHash, Word32) -> BlockHash -> Bool -> Int -> m (Bool)
-- pruneSpentOutputs (txId, opIndex) staleMarker __ htt = do
--     bp2pEnv <- getBitcoinP2P
--     dbe <- getDB
--     let rkdb = rocksDB dbe
--         cfs = rocksCF dbe
--     lg <- getLogger
--     cf <- liftIO $ TSH.lookup cfs ("outputs")
--     res <- liftIO $ try $ getDBCF rkdb (fromJust cf) (txId, opIndex)
--     case res of
--         Right op -> do
--             case op of
--                 Just zu -> do
--                     rst <- P.mapM (\bhash -> bhash `predecessorOf` staleMarker) (zuBlockHash zu)
--                     let noneStale = P.null $ P.filter (\x -> x == True) rst
--                     isLast <- liftIO $ newIORef False
--                     P.mapM_
--                         (\opt -> do
--                              if noneStale
--                                  then do
--                                      debug lg $
--                                          LG.msg $
--                                          "recur pvFrsh=TRUE: " ++ show (txId, opIndex) ++ " htt: " ++ (show htt)
--                                      _ <-
--                                          zRPCDispatchTraceOutputs
--                                              (OutPoint (fst opt) (snd opt))
--                                              staleMarker
--                                              True
--                                              (htt + 1)
--                                      return ()
--                                  else do
--                                      debug lg $
--                                          LG.msg $
--                                          "recur pvFrsh=FALSE: " ++ show (txId, opIndex) ++ " htt: " ++ (show htt)
--                                      res <-
--                                          zRPCDispatchTraceOutputs
--                                              (OutPoint (fst opt) (snd opt))
--                                              staleMarker
--                                              False
--                                              (htt + 1)
--                                      if res
--                                          then liftIO $ writeIORef isLast True
--                                          else return ())
--                         (zuInputs zu)
--                     if not noneStale -- if current ZUT is stale
--                         then do
--                             debug lg $ LG.msg $ "Deleting finalized spent-TXO : " ++ show (txId, opIndex)
--                             res <- liftIO $ try $ deleteDBCF rkdb (fromJust cf) (txId, opIndex)
--                             case res of
--                                 Right _ -> return ()
--                                 Left (e :: SomeException) -> do
--                                     err lg $ LG.msg $ "Error: Deleting from " ++ (show cf) ++ ": " ++ show e
--                                     throw KeyValueDBInsertException
--                         else return ()
--                     debug lg $ LG.msg $ "rtrn FALSE : " ++ show (txId, opIndex)
--                     return (False)
--                 Nothing -> do
--                     debug lg $ LG.msg $ "rtrn TRUE : " ++ show (txId, opIndex) ++ " htt: " ++ (show htt)
--                     return (True)
--         Left (e :: SomeException) -> do
--             err lg $ LG.msg $ "Error: Fetching from " ++ (show cf) ++ ": " ++ show e
--             throw KeyValueDBInsertException
-- --
-- --
-- -- given two block hashes x & y , check if 'x' is predecessorOf 'y' 
-- predecessorOf :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> BlockHash -> m Bool
-- predecessorOf x y = do
--     bp2pEnv <- getBitcoinP2P
--     ci <- liftIO $ readTVarIO (confChainIndex bp2pEnv)
--     let ch = hashIndex ci
--     return $ (M.lookup x ch) < (M.lookup y ch)
validateOutpoint ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => OutPoint
    -> Maybe BlockHash
    -> Int
    -> m (Word64, [BlockHash], Word32)
validateOutpoint outPoint curBlkHash wait = do
    dbe <- getDB
    let rkdb = rocksDB dbe
        cfs = rocksCF dbe
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    predecessors <- liftIO $ readTVarIO (predecessors bp2pEnv)
    -- TODO get predecessors
    debug lg $
        LG.msg $
        "[dag] validateOutpoint called for (Outpoint,Set BlkHash, Maybe BlkHash, Int) " ++
        (show (outPoint, predecessors, curBlkHash, wait))
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        txSync = txSynchronizer bp2pEnv
        opindx = fromIntegral $ outPointIndex outPoint
        optxid = outPointHash outPoint
    cf <- liftIO $ TSH.lookup cfs ("outputs")
    res <- liftIO $ try $ getDBCF rkdb (fromJust cf) (optxid, opindx)
    case res of
        Right op -> do
            case op of
                Just zu -> do
                    debug lg $ LG.msg $ " ZUT entry found : " ++ (show zu)
                    mapM_
                        (\s ->
                             if (spBlockHash s) `L.elem` predecessors
                                 then throw OutputAlreadySpentException -- predecessors to be passed correctly
                                 else return ())
                        (zuSpending zu)
                        -- eagerly mark spent, in the unlikely scenario script stack eval fails, mark unspent
                    let vx = spendZtxiUtxo curBlkHash (optxid) (opindx) zu
                    liftIO $ putDBCF rkdb (fromJust cf) (optxid, opindx) vx
                    return $ (zuSatoshiValue zu, zuBlockHash zu, zuBlockHeight zu)
                Nothing -> do
                    debug lg $
                        LG.msg $
                        "Tx not found: " ++ (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
                    debug lg $
                        LG.msg $
                        "[dag] validateOutpoint: Tx not found: " ++
                        (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
                    valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
                    event <-
                        case valx of
                            Just evt -> return evt
                            Nothing -> liftIO $ EV.new
                    liftIO $ TSH.insert txSync (outPointHash outPoint) event
                    tofl <- liftIO $ waitTimeout (event) (fromIntegral (wait * 1000000))
                    if tofl == False
                        then do
                            err lg $
                                LG.msg $
                                "[dag] validateOutpoint: Error: tofl False for outPoint: " ++
                                (show $ outPointHash outPoint)
                            liftIO $ TSH.delete txSync (outPointHash outPoint)
                            throw TxIDNotFoundException
                        else do
                            debug lg $
                                LG.msg $ "event received _available_: " ++ (show $ txHashToHex $ outPointHash outPoint)
                            debug lg $
                                LG.msg $
                                "[dag] validateOutpoint: event received _available_: " ++
                                (show $ txHashToHex $ outPointHash outPoint)
                            validateOutpoint outPoint curBlkHash 0
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: Fetching from " ++ (show cf) ++ ": " ++ show e
            err lg $ LG.msg $ "[dag] validateOutpoint: Error: Fetching from " ++ (show cf) ++ ": " ++ show e
            throw KeyValueDBInsertException

updateOutpoint ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => OutPoint
    -> BlockHash
    -> Word32
    -> m (Word32)
updateOutpoint outPoint bhash bht = do
    dbe <- getDB
    let rkdb = rocksDB dbe
        cfs = rocksCF dbe
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $
        LG.msg $
        "[dag] updateOutpoint called for (Outpoint,blkHash,blkHeight) " ++
        (show (outPoint, bhash, bht))
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        opindx = fromIntegral $ outPointIndex outPoint :: Word32
        optxid = outPointHash outPoint
    cf <- liftIO $ TSH.lookup cfs ("outputs")
    res <- liftIO $ try $ getDBCF rkdb (fromJust cf) (optxid, opindx)
    case res of
        Right (op :: Maybe ZtxiUtxo) -> do
            case op of
                Just zu -> do
                    debug lg $ LG.msg $ " ZUT entry found : " ++ (show zu)
                    let bhashes = replaceProvisionals bhash $ zuBlockHash zu
                        zu' = zu {zuBlockHash = bhashes, zuBlockHeight = bht}
                    liftIO $ putDBCF rkdb (fromJust cf) (optxid, opindx) zu'
                    return $ zuOpCount zu
                Nothing -> do
                    debug lg $
                        LG.msg $
                        "Tx not found: " ++ (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
                    debug lg $
                        LG.msg $
                        "[dag] validateOutpoint: Tx not found: " ++
                        (show $ txHashToHex $ outPointHash outPoint) ++ " _waiting_ for event"
                    return 0
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: Fetching from " ++ (show cf) ++ ": " ++ show e
            err lg $ LG.msg $ "[dag] validateOutpoint: Error: Fetching from " ++ (show cf) ++ ": " ++ show e
            throw KeyValueDBInsertException

pruneBlocksTxnsOutputs :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => [BlockHash] -> m ()
pruneBlocksTxnsOutputs blockHashes = do
    bp2pEnv <- getBitcoinP2P
    dbe <- getDB
    let rkdb = rocksDB dbe
        cfs = rocksCF dbe
    lg <- getLogger
    cf <- liftIO $ TSH.lookup cfs ("outputs")
    mapM_
        (\bhash -> do
             opqueue <- liftIO $ TSH.lookup (pruneUtxoQueue bp2pEnv) bhash
            -- oplist <- liftIO $ TSH.toList (que)
             case opqueue of
                 Just opq -> do
                     liftIO $
                         TSH.mapM_
                             (\(OutPoint txId opIndex, ()) -> do
                                  debug lg $ LG.msg $ "Pruning spent-TXO : " ++ show (txId, opIndex)
                                  res <- try $ deleteDBCF rkdb (fromJust cf) (txId, opIndex)
                                  case res of
                                      Right _ -> return ()
                                      Left (e :: SomeException) -> do
                                          err lg $ LG.msg $ "Error: Deleting from " ++ (show cf) ++ ": " ++ show e
                                          throw KeyValueDBInsertException)
                             opq
                 Nothing -> debug lg $ LG.msg $ "BlockHash not found, nothing to prune! " ++ (show bhash)
             liftIO $ TSH.delete (pruneUtxoQueue bp2pEnv) bhash)
        blockHashes
    return ()

spendZtxiUtxo :: Maybe BlockHash -> TxHash -> Word32 -> ZtxiUtxo -> ZtxiUtxo
spendZtxiUtxo mbh tsh ind zu =
    case mbh of
        Nothing -> zu
        Just bh ->
            ZtxiUtxo
                { zuTxHash = zuTxHash zu
                , zuOpIndex = zuOpIndex zu
                , zuBlockHash = zuBlockHash zu
                , zuBlockHeight = zuBlockHeight zu
                , zuInputs = zuInputs zu
                , zuSpending = (zuSpending zu) ++ [Spending bh tsh ind]
                , zuSatoshiValue = zuSatoshiValue zu
                , zuOpCount = zuOpCount zu
                }

unSpendZtxiUtxo :: BlockHash -> TxHash -> Word32 -> ZtxiUtxo -> ZtxiUtxo
unSpendZtxiUtxo bh tsh ind zu =
    ZtxiUtxo
        { zuTxHash = zuTxHash zu
        , zuOpIndex = zuOpIndex zu
        , zuBlockHash = zuBlockHash zu
        , zuBlockHeight = zuBlockHeight zu
        , zuInputs = zuInputs zu
        , zuSpending = L.delete (Spending bh tsh ind) (zuSpending zu)
        , zuSatoshiValue = zuSatoshiValue zu
        , zuOpCount = zuOpCount zu
        }
