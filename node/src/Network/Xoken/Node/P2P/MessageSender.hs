{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.MessageSender where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.Async.Lifted as LA
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import Control.Monad.Reader
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Serialize as DS
import Network.Socket
import Network.Xoken.Block
import Network.Xoken.Network.Common
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Network.Message
import Network.Xoken.Node.DB
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.Version
import Data.ByteString.Short as BSS
import qualified Network.Socket.ByteString.Lazy as LB ( sendAll)
import Network.Xoken.Transaction
import System.Logger as LG
import Xoken.NodeConfig as NC
import Network.Xoken.Crypto.Hash


sendEncMessage :: MVar () -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = withMVar writeLock (\_ -> LB.sendAll sock msg)

sendRequestMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> Message -> m ()
sendRequestMessages pr msg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    debug lg $ LG.msg $ val "sendRequestMessages - called."
    case bpSocket pr of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right () -> return ()
                Left (e :: SomeException) ->
                    case fromException e of
                        Just (t :: AsyncCancelled) -> throw e
                        otherwise -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"


sendCompactBlockGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
sendCompactBlockGetData pr hash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let gd = GetData [InvVector InvCompactBlock hash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendCompactBlockGetData: " ++ show gd
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right _ -> liftIO $ TSH.insert (ingressCompactBlocks bp2pEnv) (BlockHash hash) True
                Left (e :: SomeException) -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"


sendTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
sendTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let gd = GetData $ [InvVector InvTx txHash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendTxGetData: " ++ show gd
    case bpSocket pr of
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

produceGetHeadersMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m Message
produceGetHeadersMessage = do
    lg <- getLogger
    debug lg $ LG.msg $ val "produceGetHeadersMessage - called."
    bp2pEnv <- getBitcoinP2P
    -- be blocked until a new best-block is updated in DB, or a set timeout.
    LA.race (liftIO $ threadDelay (15 * 1000000)) (liftIO $ takeMVar (bestBlockUpdated bp2pEnv))
    bl <- getBlockLocator
    let gh =
            GetHeaders
                { getHeadersVersion = myVersion
                , getHeadersBL = bl
                , getHeadersHashStop = "0000000000000000000000000000000000000000000000000000000000000000"
                }
    debug lg $ LG.msg ("block-locator: " ++ show bl)
    return (MGetHeaders gh)

sendGetHeaderMessages :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Message -> m ()
sendGetHeaderMessages msg = do
    lg <- getLogger
    debug lg $ LG.msg $ val "sendGetHeaderMessages - called."
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    case msg of
        MGetHeaders hdr -> do
            allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
            let connPeers = L.filter (bpConnected . snd) (M.toList allPeers)
            let fbh = getHash256 $ getBlockHash $ head (getHeadersBL hdr)
                md = BSS.index fbh $ BSS.length fbh - 1
                pds =
                    map
                        (\p -> fromIntegral (md + p) `mod` L.length connPeers)
                        [1 .. fromIntegral (L.length connPeers)]
                indices =
                    case L.length (getHeadersBL hdr) of
                        x
                            | x >= 19 -> take 4 pds -- 2^19 = blk ht 524288
                            | x < 19 -> take 1 pds
            res <-
                liftIO $
                try $
                mapM_
                    (\z -> do
                         let pr = snd $ connPeers !! z
                         case bpSocket pr of
                             Just q -> do
                                 let em = runPut . putMessage net $ msg
                                 liftIO $ sendEncMessage (bpWriteMsgLock pr) q (BSL.fromStrict em)
                                 debug lg $ LG.msg ("sending out GetHeaders: " ++ show (bpAddress pr))
                             Nothing -> debug lg $ LG.msg $ val "Error sending, no connections available")
                    indices
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> err lg $ LG.msg ("Error, sending out data: " ++ show e)
        ___ -> undefined

broadcastToPeers :: (HasXokenNodeEnv env m, MonadIO m) => Message -> m ()
broadcastToPeers msg = do
    liftIO $ putStrLn $ "Broadcasting " ++ show (msgType msg) ++ " to peers"
    bp2pEnv <- getBitcoinP2P
    peerMap <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    mapM_
        (\bp ->
             if bpConnected bp
                 then sendRequestMessages bp msg
                 else return ())
        peerMap
    liftIO $ putStrLn $ "Broadcasted " ++ show (msgType msg) ++ " to peers"

sendcmpt :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
sendcmpt bp = sendRequestMessages bp $ MSendCompact $ SendCompact 0 1

sendCmptBlock :: (HasXokenNodeEnv env m, MonadIO m) => CompactBlock -> BitcoinPeer -> m ()
sendCmptBlock cmpt bp = sendRequestMessages bp $ MCompactBlock cmpt

sendBlockTxn :: (HasXokenNodeEnv env m, MonadIO m) => BlockTxns -> BitcoinPeer -> m ()
sendBlockTxn blktxn bp = sendRequestMessages bp $ MBlockTxns blktxn

sendInv :: (HasXokenNodeEnv env m, MonadIO m) => Inv -> BitcoinPeer -> m ()
sendInv inv bp = sendRequestMessages bp $ MInv inv
