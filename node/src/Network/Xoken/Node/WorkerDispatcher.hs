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
import Control.Concurrent.Async.Lifted as LA (async)
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
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
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
    => (Tx -> BlockHash -> Word32 -> Word32 -> m ())
    -> Tx
    -> BlockHash
    -> Word32
    -> Word32
    -> m ()
zRPCDispatchTxValidate selfFunc tx bhash bheight txindex = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ "encoded Tx : " ++ (show $ txHash tx)
    let lexKey = getTxShortCode (txHash tx)
    worker <- getRemoteWorker lexKey TxValidation
    case worker of
        Nothing -> do
            liftIO $ print "zRPCDispatchTxValidate - SELF"
            res <- LE.try $ selfFunc tx bhash bheight txindex
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("[ERROR] processConfTransaction " ++ show e)
                    throw e
        Just wrk -> do
            liftIO $ print $ "zRPCDispatchTxValidate - " ++ show wrk
            let mparam = ZValidateTx bhash bheight txindex tx
            resp <- zRPCRequestDispatcher mparam wrk
            liftIO $ print $ "zRPCRequestDispatcher - RESPONSE " ++ show resp
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

zRPCDispatchGetOutpoint :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> m (Word64)
zRPCDispatchGetOutpoint outPoint bhash = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let opIndex = outPointIndex $ outPoint
        lexKey = getTxShortCode $ outPointHash outPoint
    worker <- getRemoteWorker lexKey GetOutpoint
    case worker of
        Nothing -> do
            liftIO $ print "zRPCDispatchGetOutpoint - SELF"
            val <-
                validateOutpoint
                    (outPoint)
                    (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                    (DS.singleton bhash) -- TODO: needs to contains more predecessors
                    bhash
            return val
        Just wrk -> do
            liftIO $ print $ "zRPCDispatchGetOutpoint - " ++ show wrk
            let mparam = ZGetOutpoint (outPointHash outPoint) (outPointIndex outPoint) bhash (DS.singleton bhash)
            resp <- zRPCRequestDispatcher mparam wrk
            liftIO $ print $ "zRPCDispatchGetOutpoint - RESPONSE " ++ show resp
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZGetOutpointResp val scr -> return (val)
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex

zRPCDispatchTraceOutputs :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> Bool -> m (Bool)
zRPCDispatchTraceOutputs outPoint bhash isPrevFresh = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        txZUT = txZtxiUtxoTable bp2pEnv
    let opIndex = outPointIndex outPoint
        lexKey = getTxShortCode $ outPointHash outPoint
    worker <- getRemoteWorker lexKey TraceOutputs
    case worker of
        Nothing -> do
            traceStaleSpentOutputs txZUT (outPointHash outPoint, opIndex) bhash isPrevFresh
        Just wrk -> do
            let mparam = ZTraceOutputs (outPointHash outPoint) (outPointIndex outPoint) bhash isPrevFresh
            resp <- zRPCRequestDispatcher mparam wrk
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZTraceOutputsResp flag -> return (flag)
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex

zRPCDispatchNotifyNewBlockHeader :: (HasXokenNodeEnv env m, MonadIO m) => [ZBlockHeader] -> m ()
zRPCDispatchNotifyNewBlockHeader headers = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    -- let bhash = zBlockHash headers
    --     blkht = zBlockHeight headers
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        txZUT = txZtxiUtxoTable bp2pEnv
    mapM_
        (\wrk -> do
             case wrk of
                 SelfWorker {..} -> return ()
                 RemoteWorker {..} -> do
                     let mparam = ZNotifyNewBlockHeader headers
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
    liftIO $ print $ "=====> " ++ (show mid)
    let msg = ZRPCRequest mid param
    debug lg $ LG.msg $ "dispatching to worker 1: " ++ (show wrk)
    liftIO $ TSH.insert (woMsgMultiplexer wrk) mid sem
    debug lg $ LG.msg $ "dispatching to worker 2: " ++ (show wrk)
    liftIO $ sendMessage (woSocket wrk) (woWriteLock wrk) (CBOR.serialise msg)
    debug lg $ LG.msg $ "dispatching to worker 3: " ++ (show wrk)
    resp <- liftIO $ takeMVar sem
    debug lg $ LG.msg $ "dispatching to worker 4: " ++ (show wrk)
    liftIO $ TSH.delete (woMsgMultiplexer wrk) mid
    return resp

getRemoteWorker :: (HasXokenNodeEnv env m, MonadIO m) => Word8 -> NodeRole -> m (Maybe Worker)
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
        SelfWorker {..} -> do
            liftIO $ print "^^^"
            return Nothing
        RemoteWorker {..} -> do
            liftIO $ print ">>>"
            return $ Just wrk

traceStaleSpentOutputs ::
       (HasXokenNodeEnv env m, MonadIO m)
    => (TSH.TSHashTable (TxHash, Word32) ZtxiUtxo)
    -> (TxHash, Word32)
    -> BlockHash
    -> Bool
    -> m (Bool)
traceStaleSpentOutputs txZUT (txId, opIndex) staleMarker prevFresh = do
    op <- liftIO $ TSH.lookup (txZUT) (txId, opIndex)
    dbe <- getDB
    let rkdb = rocksDB dbe
        cfs = rocksCF dbe
    lg <- getLogger
    case op of
        Just zu -> do
            P.mapM_
                (\opt -> do
                     rst <- P.mapM (\bhash -> bhash `predecessorOf` staleMarker) (zuBlockHash zu)
                     if P.null $ P.filter (\x -> x == True) rst
                         then do
                             _ <- zRPCDispatchTraceOutputs (OutPoint (fst opt) (snd opt)) staleMarker True
                             return ()
                         else do
                             isRoot <- zRPCDispatchTraceOutputs (OutPoint (fst opt) (snd opt)) staleMarker False
                             debug lg $ LG.msg $ "Deleting ZUT : " ++ show (txId, opIndex)
                             liftIO $ TSH.delete (txZUT) (txId, opIndex)
                             cf <- liftIO $ TSH.lookup cfs ("finalized_outputs")
                             if isRoot
                                 then do
                                     res <- liftIO $ try $ deleteDBCF rkdb (fromJust cf) (txId, opIndex)
                                     case res of
                                         Right _ -> return ()
                                         Left (e :: SomeException) -> do
                                             err lg $ LG.msg $ "Error: Deleting from " ++ (show cf) ++ ": " ++ show e
                                             throw KeyValueDBInsertException
                                 else return ()
                             if prevFresh == True
                                 then do
                                     res <-
                                         liftIO $ try $ putDBCF rkdb (fromJust cf) (txId, opIndex) (zuSatoshiValue zu)
                                     case res of
                                         Right _ -> return ()
                                         Left (e :: SomeException) -> do
                                             err lg $ LG.msg $ "Error: INSERTing into " ++ (show cf) ++ ": " ++ show e
                                             throw KeyValueDBInsertException
                                 else return ())
                (zuInputs zu)
            return (False)
        Nothing -> return (True)

--
--
-- given two block hashes x & y , check if 'x' is predecessorOf 'y' 
predecessorOf :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> BlockHash -> m Bool
predecessorOf x y = do
    bp2pEnv <- getBitcoinP2P
    ci <- liftIO $ readTVarIO (confChainIndex bp2pEnv)
    let ch = hashIndex ci
    return $ (M.lookup x ch) < (M.lookup y ch)

validateOutpoint ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => OutPoint -> Int -> DS.Set BlockHash -> BlockHash -> m (Word64)
validateOutpoint outPoint waitSecs predecessors curBlkHash = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        txZUT = txZtxiUtxoTable bp2pEnv
        txSync = txSynchronizer bp2pEnv
        opindx = fromIntegral $ outPointIndex outPoint
    op <- liftIO $ TSH.lookup (txZUT) (outPointHash outPoint, opindx)
    case op of
        Just zu -> do
            debug lg $ LG.msg $ " ZUT entry found : " ++ (show zu)
            mapM_
                (\s ->
                     if (spBlockHash s) `DS.member` predecessors
                         then return () -- TODO: throw OutputAlreadySpentException -- predecessors to be passed correctly
                         else return ())
                (zuSpending zu)
            -- eagerly mark spent, in the unlikely scenario script stack eval fails, mark unspent
            let vx = spendZtxiUtxo curBlkHash (outPointHash outPoint) (opindx) zu
            liftIO $ TSH.insert (txZtxiUtxoTable bp2pEnv) (outPointHash outPoint, opindx) vx
            return $ zuSatoshiValue zu
        Nothing -> do
            debug lg $ LG.msg $ " ZUT entry not found for : " ++ (show (outPointHash outPoint, opindx))
            vl <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
            event <-
                case vl of
                    Just evt -> return evt
                    Nothing -> liftIO $ EV.new
            liftIO $ TSH.insert txSync (outPointHash outPoint) event
            tofl <- liftIO $ waitTimeout event (1000000 * (fromIntegral $ waitSecs))
            if tofl == False
                then do
                    liftIO $ TSH.delete (txSync) (outPointHash outPoint)
                    debug lg $
                        LG.msg $
                        "TxIDNotFoundException: While querying txid_outputs for (TxID, Index): " ++
                        (show $ txHashToHex $ outPointHash outPoint) ++ ", " ++ show (outPointIndex $ outPoint) ++ ")"
                    throw TxIDNotFoundException
                else validateOutpoint outPoint waitSecs predecessors curBlkHash

spendZtxiUtxo :: BlockHash -> TxHash -> Word32 -> ZtxiUtxo -> ZtxiUtxo
spendZtxiUtxo bh tsh ind zu =
    ZtxiUtxo
        { zuTxHash = zuTxHash zu
        , zuOpIndex = zuOpIndex zu
        , zuBlockHash = zuBlockHash zu
        , zuBlockHeight = zuBlockHeight zu
        , zuInputs = zuInputs zu
        , zuSpending = (zuSpending zu) ++ [Spending bh tsh ind]
        , zuSatoshiValue = zuSatoshiValue zu
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
        }
