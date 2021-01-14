{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.Worker.Common where

import qualified Codec.Serialise as CBOR
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.IO.Class
import Data.Binary as DB
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.Int
import Data.List as L
import Data.Maybe
import Data.Serialize as S
import GHC.Base as GHCB
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as LB (sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Node.DB
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env as NEnv
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Types
import Network.Xoken.Transaction.Common
import Prelude as P
import System.Logger as LG

receiveMessage :: (MonadIO m) => Socket -> m BSL.ByteString
receiveMessage sock = do
    lp <- recvAll sock 4
    case runGetLazy getWord32le lp of
        Right x -> do
            payload <- recvAll sock (fromIntegral x)
            return payload
        Left e -> P.error e

sendMessage :: (MonadIO m) => Socket -> MVar () -> BSL.ByteString -> m ()
sendMessage sock writeLock payload = do
    let len = LC.length payload
        prefix = toLazyByteString $ (word32LE $ fromIntegral len)
    liftIO $
        withMVar
            writeLock
            (\_ -> do
                 LB.sendAll sock prefix
                 LB.sendAll sock payload)

zRPCRequestDispatcher :: (HasXokenNodeEnv env m, MonadIO m) => ZRPCRequestParam -> Worker -> m (ZRPCResponse)
zRPCRequestDispatcher param wrk = do
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

validateOutpoint ::
       (HasXokenNodeEnv env m, HasLogger m, MonadIO m)
    => OutPoint
    -> Maybe BlockHash
    -> Int
    -> m (Word64, [BlockHash], Word32)
validateOutpoint outPoint curBlkHash wait = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    predecessors <- liftIO $ readTVarIO (predecessors bp2pEnv)
    -- TODO get predecessors
    debug lg $
        LG.msg $
        "[dag] validateOutpoint called for (Outpoint,Set BlkHash, Maybe BlkHash, Int) " ++
        (show (outPoint, predecessors, curBlkHash, wait))
    let txSync = txSynchronizer bp2pEnv
        opindx = fromIntegral $ outPointIndex outPoint
        optxid = outPointHash outPoint
    res <- LE.try $ getOutput outPoint
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
                    putOutput outPoint vx
                    --liftIO $ putDBCF rkdb (fromJust cf) (optxid, opindx) vx
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
            err lg $ LG.msg $ "Error: (validateOutpoint) Fetching from outputs: " ++ show e
            throw KeyValueDBInsertException

updateOutpoint :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => OutPoint -> BlockHash -> Word32 -> m (Word32)
updateOutpoint outPoint bhash bht = do
    lg <- getLogger
    debug lg $ LG.msg $ "[dag] updateOutpoint called for (Outpoint,blkHash,blkHeight) " ++ (show (outPoint, bhash, bht))
    res <- LE.try $ getOutput outPoint
    case res of
        Right (op :: Maybe ZtxiUtxo) -> do
            case op of
                Just zu -> do
                    debug lg $ LG.msg $ " ZUT entry found : " ++ (show zu)
                    let bhashes = replaceProvisionals bhash $ zuBlockHash zu
                        zu' = zu {zuBlockHash = bhashes, zuBlockHeight = bht}
                    putOutput outPoint zu'
                    --liftIO $ putDBCF rkdb (fromJust cf) (optxid, opindx) zu'
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
            err lg $ LG.msg $ "Error: Fetching from outputs: " ++ show e
            throw KeyValueDBInsertException

pruneBlocksTxnsOutputs :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => [BlockHash] -> m ()
pruneBlocksTxnsOutputs blockHashes = do
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    dbe <- getDB
    cf <- liftIO $ TSH.lookup (rocksCF dbe) "outputs"
    mapM_
        (\bhash -> do
             opqueue <- liftIO $ TSH.lookup (pruneUtxoQueue bp2pEnv) bhash
            -- oplist <- liftIO $ TSH.toList (que)
             case opqueue of
                 Just opq -> do
                     liftIO $
                         TSH.mapM_
                             (\(op@(OutPoint txId opIndex), ()) -> do
                                  debug lg $ LG.msg $ "Pruning spent-TXO : " ++ show (txId, opIndex)
                                  res <- try $ deleteIO (rocksDB dbe) (fromJust cf) op
                                  case res of
                                      Right () -> return ()
                                      Left (e :: SomeException) -> do
                                          err lg $ LG.msg $ "Error: Deleting from outputs: " ++ show e
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
