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
import qualified System.ZMQ4 as Z
import Text.Printf
import Xoken.NodeConfig as NC

zRPCDispatchTxValidate :: (HasXokenNodeEnv env m, MonadIO m) => Tx -> BlockHash -> Word32 -> Word32 -> m ()
zRPCDispatchTxValidate tx bhash txindex bheight = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ "encoded Tx : " ++ (show $ txHash tx)
    let mid = getTxShortHash' (txHash tx) 32
        msg = ZRPCRequest mid $ ZValidateTx bhash bheight txindex tx
    resp <- zRPCRequestDispatcher msg
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
    -- let conn = keyValDB $ dbe'
    let shortHash = (getTxShortHash' (outPointHash $ outPoint)) 20
        opIndex = outPointIndex $ outPoint
        lexKey = (getTxShortHash' (outPointHash outPoint) 32)
    (wrk, self) <- getWorker lexKey
    if self
        then do
            val <-
                validateOutpoint
                    (outPoint)
                    (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                    (DS.singleton bhash) -- TODO: needs to contains more predecessors
                    bhash
            return val
        else do
            let mid = lexKey + (fromIntegral $ outPointIndex outPoint)
                msg =
                    ZRPCRequest mid $
                    ZGetOutpoint (outPointHash outPoint) (outPointIndex outPoint) bhash (DS.singleton bhash)
            resp <- zRPCRequestDispatcher msg
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

zRPCDispatchTraceOutputs :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> m ([(TxShortHash, Word32)])
zRPCDispatchTraceOutputs outPoint bhash = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        txZUT = txZtxiUtxoTable bp2pEnv
    -- let conn = keyValDB $ dbe'
    let shortHash = (getTxShortHash (outPointHash $ outPoint)) 20
        opIndex = outPointIndex outPoint
        lexKey = (getTxShortHash' (outPointHash outPoint) 32)
    (wrk, self) <- getWorker lexKey
    if self
        then do
            traceStaleSpentOutputs txZUT (shortHash, opIndex) bhash
        else do
            let mid = lexKey + (fromIntegral $ outPointIndex outPoint)
                msg = ZRPCRequest mid $ ZTraceOutputs (outPointHash outPoint) (outPointIndex outPoint) bhash
            resp <- zRPCRequestDispatcher msg
            case zrsPayload resp of
                Right spl -> do
                    case spl of
                        Just pl ->
                            case pl of
                                ZTraceOutputsResp zul -> return (zul)
                        Nothing -> throw InvalidMessageTypeException
                Left er -> do
                    err lg $ LG.msg $ "decoding Tx validation error resp : " ++ show er
                    let mex = (read $ fromJust $ zrsErrorData er) :: BlockSyncException
                    throw mex

zRPCRequestDispatcher :: (HasXokenNodeEnv env m, MonadIO m) => ZRPCRequest -> m (ZRPCResponse)
zRPCRequestDispatcher msg = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    (wrk, _) <- getWorker $ zrqId msg
    sem <- liftIO $ newEmptyMVar
    debug lg $ LG.msg $ "dispatching to worker : " ++ (show wrk)
    liftIO $ TSH.insert (woMsgMultiplexer wrk) (zrqId msg) sem
    liftIO $ Z.send (woSocket wrk) [] (LC.toStrict $ CBOR.serialise msg)
    resp <- liftIO $ takeMVar sem
    liftIO $ TSH.delete (woMsgMultiplexer wrk) (zrqId msg)
    return resp

getWorker :: (HasXokenNodeEnv env m, MonadIO m) => Word32 -> m (Worker, Bool)
getWorker shardingLex = do
    bp2pEnv <- getBitcoinP2P
    workers <- liftIO $ readTVarIO $ workerConns bp2pEnv
    lg <- getLogger
    debug lg $ LG.msg $ "workerConns : " ++ (show workers)
    let nx = fromIntegral $ (shardingLex + (shardingHashSecretSalt $ nodeConfig bp2pEnv))
    let wind = nx `mod` (L.length workers)
    -- TODO: include self, (1 + x)
    let wrk = workers !! wind
    let self = (woID wrk) == (NC._nodeID $ NC.vegaNode $ nodeConfig bp2pEnv)
    return (wrk, True)

-- pruneStaleSpentOutputs ::
--        (HasXokenNodeEnv env m, MonadIO m)
--     => (TSH.TSHashTable (TxShortHash, Int16) ZtxiUtxo)
--     -> (TxShortHash, Int16)
--     -> BlockHash
--     -> m ()
-- pruneStaleSpentOutputs txZUT (shortHash, opIndex) staleMarker = do
--     staleOps <- traceStaleSpentOutputs txZUT (shortHash, opIndex) staleMarker
--     mapM_ (\x -> do liftIO $ TSH.delete (txZUT) (zuTxShortHash x, zuOpIndex x)) (DS.toList $ DS.fromList staleOps)
--     return ()
traceStaleSpentOutputs ::
       (HasXokenNodeEnv env m, MonadIO m)
    => (TSH.TSHashTable (TxShortHash, Word32) ZtxiUtxo)
    -> (TxShortHash, Word32)
    -> BlockHash
    -> m ([(TxShortHash, Word32)])
traceStaleSpentOutputs txZUT (shortHash, opIndex) staleMarker = do
    op <- liftIO $ TSH.lookup (txZUT) (shortHash, opIndex)
    case op of
        Just zu -> do
            res <-
                P.mapM
                    (\opt -> do
                         val <- traceStaleSpentOutputs txZUT opt staleMarker
                         x <-
                             traverse
                                 (\bhsh -> do
                                      predOf <- bhsh `predecessorOf` staleMarker
                                      if predOf
                                          then return (val ++ [(zuTxShortHash zu, zuOpIndex zu)])
                                          else return val)
                                 (zuBlockHash zu)
                         return $ P.concat x)
                    (zuInputs zu)
            -- delete in same context
            mapM_ (\x -> do liftIO $ TSH.delete (txZUT) (fst x, snd x)) (DS.toList $ DS.fromList (P.concat res))
            return $ P.concat res
        Nothing -> return []

--
--
-- given two block hashes x & y , check if 'x' is predecessorOf 'y' 
predecessorOf :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> BlockHash -> m Bool
predecessorOf x y = do
    bp2pEnv <- getBitcoinP2P
    ci <- liftIO $ readTVarIO (confChainIndex bp2pEnv)
    let tx = DTE.decodeUtf8' . fromShort . getHash256 . getBlockHash $ x
        ty = DTE.decodeUtf8' . fromShort . getHash256 . getBlockHash $ y
        ch = hashIndex ci
    case (tx, ty) of
        (Right tx', Right ty') -> return $ (M.lookup tx' ch) < (M.lookup ty' ch)
        _ -> return False

validateOutpoint ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => OutPoint -> Int -> DS.Set BlockHash -> BlockHash -> m (Word64)
validateOutpoint outPoint waitSecs predecessors curBlkHash = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        txZUT = txZtxiUtxoTable bp2pEnv
        txSync = txSynchronizer bp2pEnv
        shortHash = getTxShortHash (outPointHash outPoint) 20
        opindx = fromIntegral $ outPointIndex outPoint
    op <- liftIO $ TSH.lookup (txZUT) (shortHash, opindx)
    case op of
        Just zu -> do
            debug lg $ LG.msg $ " ZUT entry found : " ++ (show zu)
            mapM_
                (\s ->
                     if (spBlockHash s) `DS.member` predecessors
                         then throw OutputAlreadySpentException
                         else return ())
                (zuSpending zu)
            -- eagerly mark spent, in the unlikely scenario script stack eval fails, mark unspent
            let vx = spendZtxiUtxo curBlkHash (shortHash) (opindx) zu
            liftIO $ TSH.insert (txZtxiUtxoTable bp2pEnv) (shortHash, opindx) vx
            return $ zuSatoshiValue zu
        Nothing -> do
            debug lg $ LG.msg $ " ZUT entry not found for : " ++ (show (shortHash, opindx))
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

spendZtxiUtxo :: BlockHash -> TxShortHash -> Word32 -> ZtxiUtxo -> ZtxiUtxo
spendZtxiUtxo bh tsh ind zu =
    ZtxiUtxo
        { zuTxShortHash = zuTxShortHash zu
        , zuTxFullHash = zuTxFullHash zu
        , zuOpIndex = zuOpIndex zu
        , zuBlockHash = zuBlockHash zu
        , zuBlockHeight = zuBlockHeight zu
        , zuInputs = zuInputs zu
        , zuSpending = (zuSpending zu) ++ [Spending bh tsh ind]
        , zuSatoshiValue = zuSatoshiValue zu
        }

unSpendZtxiUtxo :: BlockHash -> TxShortHash -> Word32 -> ZtxiUtxo -> ZtxiUtxo
unSpendZtxiUtxo bh tsh ind zu =
    ZtxiUtxo
        { zuTxShortHash = zuTxShortHash zu
        , zuTxFullHash = zuTxFullHash zu
        , zuOpIndex = zuOpIndex zu
        , zuBlockHash = zuBlockHash zu
        , zuBlockHeight = zuBlockHeight zu
        , zuInputs = zuInputs zu
        , zuSpending = L.delete (Spending bh tsh ind) (zuSpending zu)
        , zuSatoshiValue = zuSatoshiValue zu
        }
