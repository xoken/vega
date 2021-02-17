{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Xoken.Node.Worker.Dispatcher
    ( module Network.Xoken.Node.Worker.Dispatcher
    ) where
import Control.Concurrent.STM
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.IO.Class
import Data.Binary as DB
import Data.List as L
import Data.Maybe
import GHC.Base as GHCB
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Exception
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Common
import Network.Xoken.Node.Worker.Types
import Network.Xoken.Transaction.Common
import Prelude as P
import System.Logger as LG
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
                    let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                    mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                    case mex of
                        Right exp -> throw exp
                        Left (_ :: SomeException) -> throw $ ZUnknownException mexStr
    return ()

zRPCDispatchProvisionalBlockHash :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> BlockHash -> m ()
zRPCDispatchProvisionalBlockHash bh pbh = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
    mapM_
        (\wrk -> do
             case wrk of
                 SelfWorker {..} -> putProvisionalBlockHash pbh bh
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
                             let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                             mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                             case mex of
                                Right exp -> throw exp
                                Left (_ :: SomeException) -> throw $ ZUnknownException mexStr)
        (wrkrs)

zRPCDispatchGetOutpoint ::
       (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> Maybe BlockHash -> m (Word64, [BlockHash], Word32)
zRPCDispatchGetOutpoint outPoint bhash = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    debug lg $ LG.msg $ val "[dag] zRPCDispatchGetOutpoint"
    let lexKey = getTxShortHash (outPointHash outPoint) 8
    worker <- getRemoteWorker lexKey GetOutpoint
    case worker of
        Nothing
            -- liftIO $ print "zRPCDispatchGetOutpoint - SELF"
         -> do
            debug lg $ LG.msg $ val "[dag] zRPCDispatchGetOutpoint: Worker Nothing"
            val <- validateOutpoint (outPoint) bhash (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
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
                    let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                    mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                    case mex of
                        Right exp -> throw exp
                        Left (_ :: SomeException) -> throw $ ZUnknownException mexStr

zRPCDispatchUpdateOutpoint :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> BlockHash -> Word32 -> m (Word32)
zRPCDispatchUpdateOutpoint outPoint bhash height = do
    lg <- getLogger
    debug lg $ LG.msg $ val "[dag] zRPCDispatchUpdateOutpoint"
    let lexKey = getTxShortHash (outPointHash outPoint) 8
    worker <- getRemoteWorker lexKey GetOutpoint
    case worker of
        Nothing
            -- liftIO $ print "zRPCDispatchGetOutpoint - SELF"
         -> do
            debug lg $ LG.msg $ val "[dag] zRPCDispatchGetOutpoint: Worker Nothing"
            val <- updateOutpoint outPoint bhash height
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
                    let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                    mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                    case mex of
                        Right exp -> throw exp
                        Left (_ :: SomeException) -> throw $ ZUnknownException mexStr

zRPCDispatchBlocksTxsOutputs :: (HasXokenNodeEnv env m, MonadIO m) => [BlockHash] -> m ()
zRPCDispatchBlocksTxsOutputs blockHashes = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
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
                             let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                             mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                             case mex of
                                 Right exp -> throw exp
                                 Left (_ :: SomeException) -> throw $ ZUnknownException mexStr)
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
--                     let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
--                     mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
--                     case mex of
--                         Right exp -> throw exp
--                         Left (_ :: SomeException) -> throw $ ZUnknownException mexStr

zRPCDispatchNotifyNewBlockHeader :: (HasXokenNodeEnv env m, MonadIO m) => [ZBlockHeader] -> BlockNode -> m ()
zRPCDispatchNotifyNewBlockHeader headers bn = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    wrkrs <- liftIO $ readTVarIO $ workerConns bp2pEnv
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
                             let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                             mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                             case mex of
                                 Right exp -> throw exp
                                 Left (_ :: SomeException) -> throw $ ZUnknownException mexStr)
        (wrkrs)

--
zRPCDispatchUnconfirmedTxValidate :: (HasXokenNodeEnv env m, MonadIO m) => (Tx -> m ([TxHash])) -> Tx -> m ([TxHash])
zRPCDispatchUnconfirmedTxValidate selfFunc tx = do
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
                    let mexStr = (fromMaybe (show $ ZUnknownException "zrsErrorData is Nothing") $ zrsErrorData er)
                    mex <- liftIO $ try $ return $ (read mexStr :: BlockSyncException)
                    case mex of
                        Right exp -> throw exp
                        Left (_ :: SomeException) -> throw $ ZUnknownException mexStr

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
