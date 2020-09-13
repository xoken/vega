{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Xoken.Node.WorkerListener
    ( module Network.Xoken.Node.WorkerListener
    ) where

import Arivi.P2P.P2PEnv
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types
import Codec.Serialise
import qualified Codec.Serialise as CBOR
import Control.Concurrent.Async.Lifted as LA (async)
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
import Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.Char8 as LC
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
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.X509.CertificateStore
import GHC.Base as GHCB
import GHC.Generics
import qualified Network.Simple.TCP.TLS as TLS
import Network.Socket
import qualified Network.TLS as NTLS
import Network.Xoken.Block.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.WorkerDispatcher
import Network.Xoken.Transaction.Common
import Prelude as P
import StmContainers.Map as SM
import System.Logger as LG
import qualified System.ZMQ4 as Z
import Text.Printf
import Xoken.NodeConfig as NC

workerMessageMultiplexer :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Worker -> m ()
workerMessageMultiplexer worker = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    forever $ do
        msg <- liftIO $ Z.receive (woSocket worker)
        case (deserialiseOrFail $ LBS.fromStrict msg) of
            Right ms ->
                case ms of
                    ZRPCResponse mid resp -> do
                        sem <- liftIO $ TSH.lookup (woMsgMultiplexer worker) (mid)
                        case sem of
                            Just s -> do
                                debug lg $ LG.msg $ "ZRPCResponse RECEIVED (1), mid: " ++ (show mid)
                                liftIO $ putMVar s ms
                                debug lg $ LG.msg $ "ZRPCResponse RECEIVED (2), mid: " ++ (show mid)
                            Nothing -> do
                                err lg $ LG.msg $ val $ "Error: Mux, unable to match response"
                        return ()
            Left e -> do
                err lg $ LG.msg $ "Error: deserialise Failed : " ++ (show e)
                return ()

requestDispatcher ::
       (HasXokenNodeEnv env m, MonadIO m, Z.Receiver z, Z.Sender z)
    => Z.Context
    -> Z.Socket z
    -> C.ByteString
    -> C.ByteString
    -> m ()
requestDispatcher zctxt router ident msg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    resp <-
        case (deserialiseOrFail $ LBS.fromStrict msg) of
            Right ms ->
                case ms of
                    ZRPCRequest mid param -> do
                        case param of
                            ZInvite cluster clusterID -> do
                                let myNode = vegaNode $ nodeConfig bp2pEnv
                                LG.debug lg $ LG.msg $ "Received Invite, sending Ok : " ++ show cluster
                                if _nodeType myNode == NC.Master
                                    then return $ successResp mid ZOk
                                    else do
                                        async $ initializeWorkers (zctxt) myNode cluster
                                        return $ successResp mid ZOk
                            ZPing -> do
                                return $ successResp mid ZPong
                            ZGetOutpoint txId index bhash pred -> do
                                liftIO $ print $ "ZGetOutpoint - REQUEST " ++ show (txId, index)
                                zz <-
                                    LE.try $
                                    validateOutpoint
                                        (OutPoint txId index)
                                        (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                        (DS.singleton bhash) -- TODO: needs to contains more predecessors
                                        bhash
                                case zz of
                                    Right val -> do
                                        liftIO $
                                            print $
                                            "ZGetOutpoint - sending RESPONSE " ++ show (txId, index) ++ (show mid)
                                        return $ successResp mid $ ZGetOutpointResp val B.empty
                                    Left (e :: SomeException) -> do
                                        return $ errorResp mid (show e)
                            ZTraceOutputs toTxID toIndex toBlockHash -> do
                                let shortHash = (getTxShortHash toTxID) 20
                                ret <- traceStaleSpentOutputs (txZtxiUtxoTable bp2pEnv) (shortHash, toIndex) toBlockHash
                                return $ successResp mid $ ZTraceOutputsResp ret
                            ZValidateTx bhash blkht txind tx -> do
                                liftIO $ print $ "ZValidateTx - REQUEST " ++ (show $ txHash tx)
                                debug lg $ LG.msg $ "decoded ZValidateTx : " ++ (show $ txHash tx)
                                res <-
                                    LE.try $ processConfTransaction tx bhash (fromIntegral txind) (fromIntegral blkht)
                                case res of
                                    Right () -> do
                                        liftIO $
                                            print $
                                            "ZValidateTx - sending RESPONSE " ++ (show $ txHash tx) ++ (show mid)
                                        return $ successResp mid (ZValidateTxResp True)
                                    Left (e :: SomeException) -> return $ errorResp mid (show e)
            Left e -> do
                err lg $ LG.msg $ "Error: deserialise Failed : " ++ (show e)
                return $ errorResp (0) "Deserialise failed"
    liftIO $ Z.sendMulti router $ ident :| [resp]
    debug lg $ LG.msg $ val $ "ZRPCResponse SENT "
  where
    successResp mid rsp = LBS.toStrict $ serialise $ ZRPCResponse mid (Right $ Just rsp)
    errorResp mid err = LBS.toStrict $ serialise $ ZRPCResponse mid (Left $ ZRPCError Z_INTERNAL_ERROR (Just err))

-- 
startZMQRouter :: (HasXokenNodeEnv env m, MonadIO m, Z.Receiver z, Z.Sender z) => Z.Context -> Z.Socket z -> m ()
startZMQRouter zctxt router = do
    lg <- getLogger
    continue <- liftIO $ newIORef True
    whileM_ (liftIO $ readIORef continue) $ do
        rm <- liftIO $ Z.receiveMulti router
        let ident = rm !! 0
        let msg = rm !! 1
        debug lg $ LG.msg $ "msg received from: " ++ (C.unpack $ ident)
        LA.async $ requestDispatcher zctxt router ident msg

--
-- masterWorkerConnSetup zctxt (workers nodeConf)
initializeWorkers :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Z.Context -> Node -> [Node] -> m ()
initializeWorkers zctxt myNode clstrNodes = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let clusterID = B64.encode $ C.pack $ show clstrNodes
    ws <-
        P.mapM
            (\w -> do
                 if (_nodeID myNode == _nodeID w)
                     then return $ SelfWorker (_nodeID myNode) (_nodeRoles myNode)
                     else do
                         let wrkSock = "tcp://" ++ (_nodeIPAddr w) ++ ":" ++ (show $ _nodePort w)
                         LG.debug lg $ LG.msg $ "Connecting to " ++ (show $ _nodeType w) ++ " Node : " ++ wrkSock
                         dlr <- liftIO $ Z.socket zctxt Z.Dealer
                         liftIO $ Z.setIdentity (Z.restrict $ C.pack $ _nodeID myNode) dlr
                         liftIO $ Z.connect dlr wrkSock
                         let inv = ZRPCRequest (0) $ ZInvite clstrNodes clusterID
                         liftIO $ Z.send dlr [] (LC.toStrict $ CBOR.serialise inv)
                         m <- liftIO $ Z.receive dlr
                         case (CBOR.deserialiseOrFail $ LC.fromStrict m) of
                             Right ms ->
                                 case ms of
                                     ZRPCResponse mid pl -> do
                                         case pl of
                                             Right rs ->
                                                 case fromJust rs of
                                                     ZOk -> do
                                                         LG.debug lg $
                                                             LG.msg $ ("node accepted Invite with OK: " ++ wrkSock)
                                                         mux <- liftIO $ TSH.new 1
                                                         ctr <- liftIO $ newMVar 1
                                                         return $
                                                             RemoteWorker
                                                                 (_nodeID w)
                                                                 (_nodeIPAddr w)
                                                                 (_nodePort w)
                                                                 dlr
                                                                 (_nodeRoles w)
                                                                 mux
                                                                 ctr
                                             Left x -> do
                                                 LG.err lg $ LG.msg $ ("node rejected Invite : " ++ wrkSock)
                                                 throw WorkerConnectionRejectException
                             Left e -> do
                                 LG.err lg $ LG.msg $ "Error: deserialise Failed : " ++ (show e)
                                 throw WorkerConnectionRejectException)
            (clstrNodes)
    mapM_ (\w -> async $ workerMessageMultiplexer w) ws
    liftIO $ atomically $ writeTVar (workerConns bp2pEnv) ws
