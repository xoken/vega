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
import Data.ByteString.Builder
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
import Data.Word
import Data.X509.CertificateStore
import GHC.Base as GHCB
import GHC.Generics
import Network.Socket as NS
import Network.Socket.ByteString.Lazy as SB (recv, sendAll)
import qualified Network.TLS as NTLS
import Network.Xoken.Block.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Node.Service.Chain
import Network.Xoken.Node.WorkerDispatcher
import Network.Xoken.Transaction.Common
import Prelude as P
import StmContainers.Map as SM
import System.Logger as LG
import Text.Printf
import Xoken.NodeConfig as NC

workerMessageMultiplexer :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Worker -> m ()
workerMessageMultiplexer worker = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    forever $ do
        msg <- liftIO $ receiveMessage (woSocket worker)
        case (deserialiseOrFail msg) of
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
                err lg $ LG.msg $ "Error: deserialise Failed (workerMessageMultiplexer) : " ++ (show e)
                return ()

requestHandler :: (HasXokenNodeEnv env m, MonadIO m) => Socket -> MVar () -> LC.ByteString -> m ()
requestHandler sock writeLock msg = do
    lg <- getLogger
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    let rkdb = rocksDB dbe'
    resp <-
        case (deserialiseOrFail msg) of
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
                                        async $ initializeWorkers myNode cluster
                                        return $ successResp mid ZOk
                            ZPing -> do
                                return $ successResp mid ZPong
                            ZGetOutpoint txId index bhash pred
                                -- liftIO $ print $ "ZGetOutpoint - REQUEST " ++ show (txId, index)
                             -> do
                                zz <-
                                    do let bhash' =
                                               case bhash of
                                                   Nothing -> DS.empty
                                                   Just bh -> (DS.singleton bh) -- TODO: needs to contains more predecessors
                                       LE.try $
                                           validateOutpoint
                                               (OutPoint txId index)
                                               bhash'
                                               bhash
                                               (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                                case zz of
                                    Right (val, bhs)
                                        -- liftIO $
                                        --     print $
                                        --     "ZGetOutpoint - sending RESPONSE " ++ show (txId, index) ++ (show mid)
                                     -> do
                                        return $ successResp mid $ ZGetOutpointResp val B.empty bhs
                                    Left (e :: SomeException) -> do
                                        return $ errorResp mid (show e)
                            -- ZTraceOutputs toTxID toIndex toBlockHash prevFresh htt -> do
                            --     ret <- pruneSpentOutputs (toTxID, toIndex) toBlockHash prevFresh htt
                            --     return $ successResp mid $ ZTraceOutputsResp ret
                            ZUpdateOutpoint txId index bhash ht
                                -- liftIO $ print $ "ZGetOutpoint - REQUEST " ++ show (txId, index)
                             -> do
                                zz <-
                                    LE.try $
                                        updateOutpoint
                                            (OutPoint txId index)
                                            bhash
                                            ht
                                case zz of
                                    Right upd
                                        -- liftIO $
                                        --     print $
                                        --     "ZGetOutpoint - sending RESPONSE " ++ show (txId, index) ++ (show mid)
                                     -> do
                                        return $ successResp mid $ ZUpdateOutpointResp upd
                                    Left (e :: SomeException) -> do
                                        return $ errorResp mid (show e)
                            -- ZTraceOutputs toTxID toIndex toBlockHash prevFresh htt -> do
                            --     ret <- pruneSpentOutputs (toTxID, toIndex) toBlockHash prevFresh htt
                            --     return $ successResp mid $ ZTraceOutputsResp ret
                            ZValidateTx bhash blkht txind tx
                                -- liftIO $ print $ "ZValidateTx - REQUEST " ++ (show $ txHash tx)
                             -> do
                                debug lg $ LG.msg $ "decoded ZValidateTx (Conf) : " ++ (show $ txHash tx)
                                res <-
                                    LE.try $ processConfTransaction tx bhash (fromIntegral blkht) (fromIntegral txind)
                                case res of
                                    Right outpts -> do
                                        mapM_
                                            (\opt -> do
                                                 ress <- liftIO $ TSH.lookup (pruneUtxoQueue bp2pEnv) bhash
                                                 case ress of
                                                     Just blktxq -> do
                                                         liftIO $ TSH.insert blktxq opt ()
                                                         liftIO $
                                                             print $
                                                             "ZValidateTx - sending RESPONSE " ++
                                                             (show $ txHash tx) ++ (show mid)
                                                     Nothing -> do
                                                         debug lg $ LG.msg ("New Prune queue " ++ show bhash)
                                                         opq <- liftIO $ TSH.new 1
                                                         liftIO $ TSH.insert (pruneUtxoQueue bp2pEnv) bhash opq)
                                            outpts
                                        return $ successResp mid (ZValidateTxResp True)
                                    Left (e :: SomeException) -> return $ errorResp mid (show e)
                            ZValidateUnconfirmedTx tx
                                -- liftIO $ print $ "ZValidateTx - REQUEST " ++ (show $ txHash tx)
                             -> do
                                debug lg $ LG.msg $ "decoded ZValidateTx (Unconf) : " ++ (show $ txHash tx)
                                res <- LE.try $ processUnconfTransaction tx
                                case res of
                                    Right (txs) -> do
                                        return $ successResp mid (ZValidateUnconfirmedTxResp txs)
                                    Left (e :: SomeException) -> return $ errorResp mid (show e)
                            ZPruneBlockTxOutputs blockHashes
                                -- liftIO $ print $ "ZPruneBlockTxOutputs - REQUEST " ++ (show blockHashes)
                             -> do
                                pruneBlocksTxnsOutputs blockHashes
                                return $ successResp mid $ ZPruneBlockTxOutputsResp
                            ZNotifyNewBlockHeader headers
                                -- liftIO $ print $ "ZNotifyNewBlockHeader - REQUEST " ++ (show $ P.head headers)
                             -> do
                                debug lg $ LG.msg $ "decoded ZNotifyNewBlockHeader : " ++ (show $ P.head headers)
                                res <-
                                    LE.try $
                                    mapM_
                                        (\(ZBlockHeader header blkht) -> do
                                             let hdrHash = blockHashToHex $ headerHash header
                                             resp <-
                                                 liftIO $
                                                 try $ do
                                                     putDB rkdb blkht (hdrHash, header)
                                                     putDB rkdb hdrHash (blkht, header)
                                             case resp of
                                                 Right () -> return ()
                                                 Left (e :: SomeException) ->
                                                     liftIO $ do
                                                         err lg $
                                                             LG.msg ("Error: INSERT into 'ROCKSDB' failed: " ++ show e)
                                                         throw KeyValueDBInsertException
                                             liftIO $
                                                 TSH.insert
                                                     (blockTree bp2pEnv)
                                                     (headerHash header)
                                                     (fromIntegral blkht, header))
                                        headers
                                case res of
                                    Right () -> do
                                        liftIO $
                                            print $
                                            "ZNotifyNewBlockHeader - sending RESPONSE " ++ (show $ P.head headers)
                                        return $ successResp mid (ZNotifyNewBlockHeaderResp)
                                    Left (e :: SomeException) -> return $ errorResp mid (show e)
                            otherwise -> do
                                err lg $ LG.msg $ "requestHandler unknown handler: " ++ (show param)
                                return $ errorResp mid (show ZUnknownHandler)
            Left e -> do
                err lg $ LG.msg $ "Error: deserialise Failed (requestHandler) : " ++ (show e)
                return $ errorResp (0) "Deserialise failed"
    liftIO $ sendMessage sock writeLock resp
    debug lg $ LG.msg $ val $ "ZRPCResponse SENT "
  where
    successResp mid rsp = serialise $ ZRPCResponse mid (Right $ Just rsp)
    errorResp mid err = serialise $ ZRPCResponse mid (Left $ ZRPCError Z_INTERNAL_ERROR (Just err))

-- 
startTCPServer :: (HasXokenNodeEnv env m, MonadIO m) => String -> Word16 -> m ()
startTCPServer ip port = do
    lg <- getLogger
    addrinfos <- liftIO $ getAddrInfo Nothing (Just ip) (Just $ show port)
    let serveraddr = P.head addrinfos
    sock <- liftIO $ socket (addrFamily serveraddr) Stream defaultProtocol
    liftIO $ bind sock (addrAddress serveraddr)
    liftIO $ listen sock 4
    liftIO $ print "TCP server started."
    (conn, _) <- liftIO $ accept sock
    liftIO $ print "TCP server accepted connection."
    readLoop conn lg
    liftIO $ print "TCP server socket is closing now."
    liftIO $ NS.close conn
    liftIO $ NS.close sock
  where
    readLoop conn lg = do
        wl <- liftIO $ newMVar ()
        forever $ do
            msg <- receiveMessage conn
            unless (LBS.null msg) $ do
                LA.async $ requestHandler conn wl msg
                -- debug lg $ LG.msg $ "msg received : " ++ (LC.unpack msg)
                -- liftIO $ print ("TCP server received: " ++ LC.unpack msg)
                return ()

--
-- masterWorkerConnSetup zctxt (workers nodeConf)
initializeWorkers :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Node -> [Node] -> m ()
initializeWorkers myNode clstrNodes = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let clusterID = B64.encode $ C.pack $ show clstrNodes
    ws <-
        P.mapM
            (\w -> do
                 if (_nodeID myNode == _nodeID w)
                     then return $ SelfWorker (_nodeID myNode) (_nodeRoles myNode)
                     else do
                         let wrkSock = (_nodeIPAddr w) ++ ":" ++ (show $ _nodePort w)
                         LG.debug lg $ LG.msg $ "Connecting to " ++ (show $ _nodeType w) ++ " Node : " ++ wrkSock
                         liftIO $ print $ "Connecting to " ++ (show $ _nodeType w) ++ " Node : " ++ wrkSock
                         addrinfos <- liftIO $ getAddrInfo Nothing (Just $ _nodeIPAddr w) (Just $ show $ _nodePort w)
                         let serveraddr = P.head addrinfos
                         sock <- liftIO $ socket (addrFamily serveraddr) Stream defaultProtocol
                         liftIO $ connect sock (addrAddress serveraddr)
                         let inv = ZInvite clstrNodes clusterID
                         mux <- liftIO $ TSH.new 1
                         ctr <- liftIO $ newMVar 1
                         wl <- liftIO $ newMVar ()
                         let wrk = RemoteWorker (_nodeID w) (_nodeIPAddr w) (_nodePort w) sock (_nodeRoles w) mux ctr wl
                         async $ workerMessageMultiplexer wrk
                         resp <- zRPCRequestDispatcher inv wrk
                         liftIO $ print $ "ZInvite - RESPONSE " ++ show resp
                         case zrsPayload resp of
                             Right spl -> do
                                 case fromJust spl of
                                     ZOk -> do
                                         LG.debug lg $ LG.msg $ ("node accepted Invite with OK: " ++ wrkSock)
                                         return (wrk)
                             Left er -> do
                                 LG.err lg $ LG.msg $ ("error: node rejected Invite : " ++ wrkSock)
                                 throw WorkerConnectionRejectException)
            (clstrNodes)
    liftIO $ atomically $ writeTVar (workerConns bp2pEnv) ws
