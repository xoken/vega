{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.Message.Reader where

import qualified Control.Concurrent.MSem as MS
import Control.Concurrent.MVar
import Control.Concurrent.STM.TQueue
import Control.Exception
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.Trans.Control
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.IORef
import Data.Int
import qualified Data.List as L
import Data.Maybe
import Data.Serialize as DS
import Data.Time.Clock
import Network.Socket as NS
import Network.Xoken.Block
import Network.Xoken.Constants
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Logger as LG
import Xoken.NodeConfig as NC

readNextMessage' ::
       (HasXokenNodeEnv env m, MonadIO m)
    => BitcoinPeer
    -> MVar (Maybe IngressStreamState)
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage' peer readLock = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        tracker = statsTracker peer
    case bpSocket peer of
        Just sock -> do
            prIss <- liftIO $ takeMVar $ readLock
            let prevIngressState =
                    case prIss of
                        Just pis ->
                            if (binTxTotalCount $ issBlockIngest pis) == (binTxIngested $ issBlockIngest pis)
                                then Nothing
                                else prIss
                        Nothing -> Nothing
            (msg, ingressState) <- readNextMessage net sock prevIngressState
            case ingressState of
                Just iss -> do
                    let ingst = issBlockIngest iss
                    case msg of
                        Just (MBlock blk) -- setup state
                         -> do
                            let hh = headerHash $ defBlockHeader blk
                            mht <- liftIO $ TSH.lookup (blockSyncStatusMap bp2pEnv) (hh)
                            case (mht) of
                                Just x -> return ()
                                Nothing -> do
                                    err lg $ LG.msg $ ("InvalidBlockSyncStatusMapException - " ++ show hh)
                                    throw InvalidBlockSyncStatusMapException
                            let iz = Just (IngressStreamState ingst (Just $ BlockInfo hh (snd $ fromJust mht)))
                            liftIO $ putMVar readLock iz
                        Just (MConfTx ctx) -> do
                            case issBlockInfo iss of
                                Just bi -> do
                                    tm <- liftIO $ getCurrentTime
                                    liftIO $ writeIORef (ptLastTxRecvTime tracker) $ Just tm
                                    if binTxTotalCount ingst == binTxIngested ingst
                                        then do
                                            liftIO $ atomicModifyIORef' (blockFetchWindow bp2pEnv) (\z -> (z - 1, ()))
                                            liftIO $
                                                TSH.insert
                                                    (blockSyncStatusMap bp2pEnv)
                                                    (biBlockHash bi)
                                                    (BlockReceiveComplete tm, biBlockHeight bi)
                                            debug lg $
                                                LG.msg $ ("putMVar readLock Nothing - " ++ (show $ bpAddress peer))
                                            liftIO $ putMVar readLock Nothing
                                        else do
                                            liftIO $
                                                TSH.insert
                                                    (blockSyncStatusMap bp2pEnv)
                                                    (biBlockHash bi)
                                                    ( RecentTxReceiveTime (tm, binTxIngested ingst)
                                                    , biBlockHeight bi -- track receive progress
                                                     )
                                            liftIO $ putMVar readLock ingressState
                                Nothing -> throw InvalidBlockInfoException
                        otherwise -> throw $ UnexpectedDuringBlockProcException "_1_"
                Nothing -> do
                    liftIO $ putMVar readLock ingressState
                    return ()
            return (msg, ingressState)
        Nothing -> throw PeerSocketNotConnectedException

resilientRead ::
       (HasLogger m, MonadBaseControl IO m, MonadIO m) => Socket -> BlockIngestState -> m (([Tx], LC.ByteString), Int64)
resilientRead sock !blin = do
    lg <- getLogger
    let chunkSize = 100 * 1000 -- 100 KB
        !delta =
            if binTxPayloadLeft blin > chunkSize
                then chunkSize - ((LC.length $ binUnspentBytes blin))
                else (binTxPayloadLeft blin) - (LC.length $ binUnspentBytes blin)
    trace lg $ msg (" | Tx payload left " ++ show (binTxPayloadLeft blin))
    trace lg $ msg (" | Bytes prev unspent " ++ show (LC.length $ binUnspentBytes blin))
    trace lg $ msg (" | Bytes to read " ++ show delta)
    nbyt <- recvAll sock delta
    let !txbyt = (binUnspentBytes blin) `LC.append` (nbyt)
    case runGetLazyState (getConfirmedTxBatch) txbyt of
        Left e -> do
            trace lg $ msg $ "1st attempt|" ++ show e
            let chunkSizeFB = 10 * 1000 * 1000 -- 10 MB
                !deltaNew =
                    if binTxPayloadLeft blin > chunkSizeFB
                        then chunkSizeFB - ((LC.length $ binUnspentBytes blin) + delta)
                        else (binTxPayloadLeft blin) - ((LC.length $ binUnspentBytes blin) + delta)
            nbyt2 <- recvAll sock deltaNew
            let !txbyt2 = txbyt `LC.append` (nbyt2)
            case runGetLazyState (getConfirmedTxBatch) txbyt2 of
                Left e -> do
                    err lg $ msg $ "2nd attempt|" ++ show e
                    throw ConfirmedTxParseException
                Right res -> do
                    return (res, LC.length txbyt2)
        Right res -> return (res, LC.length txbyt)

readNextMessage ::
       (HasBitcoinP2P m, HasLogger m, HasDatabaseHandles m, MonadBaseControl IO m, MonadIO m)
    => Network
    -> Socket
    -> Maybe IngressStreamState
    -> m ((Maybe Message, Maybe IngressStreamState))
readNextMessage net sock ingss = do
    p2pEnv <- getBitcoinP2P
    lg <- getLogger
    case ingss of
        Just iss -> do
            let blin = issBlockIngest iss
            ((txns, unused), txbytLen) <- resilientRead sock blin
            debug lg $ msg ("Confirmed-Tx: " ++ (show (L.length txns)) ++ " unused: " ++ show (LC.length unused))
            qe <-
                case (issBlockInfo iss) of
                    Just bf ->
                        if (binTxIngested blin == 0) -- very first Tx
                            then do
                                liftIO $ do
                                    vala <- TSH.lookup (blockTxProcessingLeftMap p2pEnv) (biBlockHash $ bf)
                                    case vala of
                                        Just v -> return ()
                                        Nothing -> do
                                            ar <- TSH.new 1
                                            TSH.insert
                                                (blockTxProcessingLeftMap p2pEnv)
                                                (biBlockHash $ bf)
                                                (ar, (binTxTotalCount blin))
                                qq <- liftIO $ atomically $ newTQueue
                                        -- wait for TMT threads alloc
                                liftIO $ MS.wait (maxTMTBuilderThreadLock p2pEnv)
                                liftIO $ TSH.insert (merkleQueueMap p2pEnv) (biBlockHash $ bf) qq
                                return qq
                            else do
                                valx <- liftIO $ TSH.lookup (merkleQueueMap p2pEnv) (biBlockHash $ bf)
                                case valx of
                                    Just q -> return q
                                    Nothing -> throw MerkleQueueNotFoundException
                    Nothing -> throw MessageParsingException
            let isLastBatch = ((binTxTotalCount blin) == ((L.length txns) + binTxIngested blin))
            let txct = L.length txns
            liftIO $
                atomically $
                mapM_
                    (\(tx, ct) -> do
                         let isLast =
                                 if isLastBatch
                                     then txct == ct
                                     else False
                         writeTQueue qe ((txHash tx), isLast))
                    (zip txns [1 ..])
            let bio =
                    BlockIngestState
                        { binUnspentBytes = unused
                        , binTxPayloadLeft = binTxPayloadLeft blin - (txbytLen - LC.length unused)
                        , binTxTotalCount = binTxTotalCount blin
                        , binTxIngested = (L.length txns) + binTxIngested blin
                        , binBlockSize = binBlockSize blin
                        , binChecksum = binChecksum blin
                        }
            return (Just $ MConfTx txns, Just $ IngressStreamState bio (issBlockInfo iss))
        Nothing -> do
            hdr <- recvAll sock 24
            case (decodeLazy hdr) of
                Left e -> do
                    err lg $ msg ("Error decoding incoming message header: " ++ e)
                    throw MessageParsingException
                Right (MessageHeader headMagic cmd len cks) -> do
                    if headMagic == getNetworkMagic net
                        then do
                            if cmd == MCBlock
                                then do
                                    byts <- recvAll sock (88) -- 80 byte Block header + VarInt (max 8 bytes) Tx count
                                    case runGetLazyState (getDeflatedBlock) (byts) of
                                        Left e -> do
                                            err lg $ msg ("Error, unexpected message header: " ++ e)
                                            throw MessageParsingException
                                        Right (blk, unused) -> do
                                            case blk of
                                                Just b -> do
                                                    trace lg $
                                                        msg
                                                            ("DefBlock: " ++
                                                             show blk ++ " unused: " ++ show (LC.length unused))
                                                    let bi =
                                                            BlockIngestState
                                                                { binUnspentBytes = unused
                                                                , binTxPayloadLeft =
                                                                      fromIntegral (len) - (88 - LC.length unused)
                                                                , binTxTotalCount = fromIntegral $ txnCount b
                                                                , binTxIngested = 0
                                                                , binBlockSize = fromIntegral $ len
                                                                , binChecksum = cks
                                                                }
                                                    return (Just $ MBlock b, Just $ IngressStreamState bi Nothing)
                                                Nothing -> throw DeflatedBlockParseException
                                else do
                                    byts <-
                                        if len == 0
                                            then return hdr
                                            else do
                                                b <- recvAll sock (fromIntegral len)
                                                return (hdr `BSL.append` b)
                                    case runGetLazy (getMessage net) byts of
                                        Left e -> do
                                            debug lg $
                                                msg ("Message parse error' : " ++ (show e) ++ "; cmd: " ++ (show cmd))
                                            throw MessageParsingException
                                        Right mg -> do
                                            debug lg $ msg ("Message recv' : " ++ (show $ msgType mg))
                                            return (Just mg, Nothing)
                        else do
                            err lg $ msg ("Error, network magic mismatch!: " ++ (show headMagic))
                            throw NetworkMagicMismatchException
