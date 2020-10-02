{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.UnconfTxSync
    ( processUnconfTransaction
    , processTxGetData
    , runEpochSwitcher
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race_)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Sequence ((|>))
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.LocalTime
import Data.Word
import qualified Database.RocksDB as R
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig

processTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
processTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    indexUnconfirmedTx <- liftIO $ readTVarIO $ indexUnconfirmedTx bp2pEnv
    if indexUnconfirmedTx == False
        then return ()
        else do
            let net = bitcoinNetwork $ nodeConfig bp2pEnv
            debug lg $ LG.msg $ val "processTxGetData - called."
            bp2pEnv <- getBitcoinP2P
            tuple <-
                liftIO $
                TSH.lookup
                    (unconfirmedTxCache bp2pEnv)
                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
            case tuple of
                Just (st, fh) ->
                    if st == False
                        then do
                            liftIO $ threadDelay (1000000 * 30)
                            tuple2 <-
                                liftIO $
                                TSH.lookup
                                    (unconfirmedTxCache bp2pEnv)
                                    (getTxShortHash (TxHash txHash) (unconfirmedTxCacheKeyBits $ nodeConfig bp2pEnv))
                            case tuple2 of
                                Just (st2, fh2) ->
                                    if st2 == False
                                        then sendTxGetData pr txHash
                                        else return ()
                                Nothing -> return ()
                        else return ()
                Nothing -> sendTxGetData pr txHash

sendTxGetData :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BitcoinPeer -> Hash256 -> m ()
sendTxGetData pr txHash = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let gd = GetData $ [InvVector InvTx txHash]
        msg = MGetData gd
    debug lg $ LG.msg $ "sendTxGetData: " ++ show gd
    case (bpSocket pr) of
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
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"

runEpochSwitcher :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEpochSwitcher =
    forever $ do
        lg <- getLogger
        bp2pEnv <- getBitcoinP2P
        dbe' <- getDB
        tm <- liftIO $ getCurrentTime
        let conn = rocksDB $ dbe'
            hour = todHour $ timeToTimeOfDay $ utctDayTime tm
            minute = todMin $ timeToTimeOfDay $ utctDayTime tm
            epoch =
                case hour `mod` 2 of
                    0 -> True
                    1 -> False
        liftIO $ atomically $ writeTVar (epochType bp2pEnv) epoch
        if minute == 0
            then do
                let (op_cf, tx_cf) =
                        case epoch of
                            True -> ("ep_outputs_odd", "ep_transactions_odd")
                            False -> ("ep_outputs_even", "ep_transactions_even")
                --R.dropCF rkdb op_cf
                --R.dropCF rkdb tx_cf
                --o_ptr <- R.createCF rkdb config op_cf
                --t_ptr <- R.createCF rkdb config op_cf
                liftIO $ threadDelay (1000000 * 60 * 60)
            else liftIO $ threadDelay (1000000 * 60 * (60 - minute))
        return ()

insertEpochTxIdOutputs ::
       (HasLogger m, MonadIO m)
    => R.DB
    -> Bool
    -> (Text, Int32)
    -> Text
    -> C.ByteString
    -> Int64
    -> TSH.TSHashTable String R.ColumnFamily
    -> m ()
insertEpochTxIdOutputs conn epoch (txid, outputIndex) address script value cfs = do
    lg <- getLogger
    cf <- liftIO $ TSH.lookup cfs (getEpochTxCF epoch)
    res <- liftIO $ try $ putDBCF conn (fromJust cf) (txid, outputIndex) (address, script, value)
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: INSERTing into ep_txid_outputs: " ++ show e
            throw KeyValueDBInsertException

processUnconfTransaction :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> m ([BlockHash], [TxHash])
processUnconfTransaction tx = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = rocksDB $ dbe'
        cfs = rocksCF dbe'
    debug lg $ LG.msg $ "Processing unconfirmed transaction: " ++ show (txHash tx)
    --
    let inAddrs = zip (txIn tx) [0 :: Int32 ..]
    let outAddrs =
            zip3
                (map (\y ->
                          case scriptToAddressBS $ scriptOutput y of
                              Left e -> ""
                              Right os ->
                                  case addrToString net os of
                                      Nothing -> ""
                                      Just addr -> addr)
                     (txOut tx))
                (txOut tx)
                [0 :: Int32 ..]
    inputs <-
        mapM
            (\(b, j) -> do
                 val <-
                     liftIO $
                     getSatsValueFromEpochOutpoint
                         conn
                         epoch
                         (txSynchronizer bp2pEnv)
                         lg
                         net
                         (prevOutput b)
                         (txProcInputDependenciesWait $ nodeConfig bp2pEnv)
                         cfs
                 return ((txHashToHex $ outPointHash $ prevOutput b, outPointIndex $ prevOutput b), j, val))
            inAddrs
    let ovs = map (\(a, o, i) -> (fromIntegral $ i, (a, (scriptOutput o), fromIntegral $ outValue o))) outAddrs
    --
    mapM_
        (\(a, o, i) -> do
             let sh = scriptOutput o
             let output = (txHashToHex $ txHash tx, i)
             insertEpochTxIdOutputs conn epoch output a sh (fromIntegral $ outValue o) cfs
             return ())
        outAddrs
    mapM_
        (\((o, i), (a, sh)) -> do
             let prevOutpoint = (txHashToHex $ outPointHash $ prevOutput o, fromIntegral $ outPointIndex $ prevOutput o)
             let output = (txHashToHex $ txHash tx, i)
             let spendInfo = (\ov -> ((txHashToHex $ txHash tx, fromIntegral $ fst ov), i, snd $ ov)) <$> ovs
             insertEpochTxIdOutputs conn epoch prevOutpoint a sh 0 cfs)
        (zip inAddrs (map (\x -> (fst3 $ thd3 x, snd3 $ thd3 x)) inputs))
    --
    let ipSum = foldl (+) 0 $ (\(_, _, (_, _, val)) -> val) <$> inputs
        opSum = foldl (+) 0 $ (\(_, o, _) -> fromIntegral $ outValue o) <$> outAddrs
        fees = ipSum - opSum
    --
    cf <- liftIO $ TSH.lookup cfs (getEpochTxCF epoch)
    liftIO $ debug lg $ LG.msg $ val "[rdb] b processUnconfTx"
    res <- liftIO $ try $ putDBCF conn (fromJust cf) (txHashToHex $ txHash tx) (tx, inputs, fees)
    liftIO $ debug lg $ LG.msg $ val "[rdb] a processUnconfTx"
    case res of
        Right _ -> return ()
        Left (e :: SomeException) -> do
            liftIO $ err lg $ LG.msg $ "Error: INSERTing into 'xoken.ep_transactions':" ++ show e
            throw KeyValueDBInsertException
    --
    vall <- liftIO $ TSH.lookup (txSynchronizer bp2pEnv) (txHash tx)
    case vall of
        Just ev -> liftIO $ EV.signal $ ev
        Nothing -> return ()
    return ([], []) -- TODO: proper response to be set

getSatsValueFromEpochOutpoint ::
       R.DB
    -> Bool
    -> (TSH.TSHashTable TxHash EV.Event)
    -> Logger
    -> Network
    -> OutPoint
    -> Int
    -> TSH.TSHashTable String R.ColumnFamily
    -> IO (Text, C.ByteString, Int64)
getSatsValueFromEpochOutpoint rkdb epoch txSync lg net outPoint waitSecs cfs = do
    cf <- liftIO $ TSH.lookup cfs (getEpochTxCF epoch)
    res <-
        liftIO $
        try $
        getDBCF rkdb (fromJust cf) (txHashToHex $ outPointHash outPoint, fromIntegral $ outPointIndex outPoint :: Int32)
    case res of
        Right Nothing -> do
            debug lg $
                LG.msg $
                "[Unconfirmed] Tx not found: " ++
                (show $ txHashToHex $ outPointHash outPoint) ++ "... waiting for event"
            valx <- liftIO $ TSH.lookup txSync (outPointHash outPoint)
            event <-
                case valx of
                    Just evt -> return evt
                    Nothing -> EV.new
            liftIO $ TSH.insert txSync (outPointHash outPoint) event
            tofl <- waitTimeout event (1000000 * (fromIntegral waitSecs))
            if tofl == False
                then do
                    liftIO $ TSH.delete txSync (outPointHash outPoint)
                    debug lg $
                        LG.msg $ "[Unconfirmed] TxIDNotFoundException: " ++ (show $ txHashToHex $ outPointHash outPoint)
                    throw TxIDNotFoundException
                else getSatsValueFromEpochOutpoint rkdb epoch txSync lg net outPoint waitSecs cfs
        Right (Just sv) -> return sv
        Left (e :: SomeException) -> do
            err lg $ LG.msg $ "Error: getSatsValueFromEpochOutpoint: " ++ show e
            throw e

convertToScriptHash :: Network -> String -> Maybe String
convertToScriptHash net s = do
    let addr = stringToAddr net (T.pack s)
    (T.unpack . txHashToHex . TxHash . sha256 . addressToScriptBS) <$> addr

constructCandidateBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => TxHash -> [BlockHash] -> [TxHash] -> m ()
constructCandidateBlock txHash candBlockHashes depTxHashes = do
    dbe' <- getDB
    bp2pEnv <- getBitcoinP2P
    lg <- getLogger
    epoch <- liftIO $ readTVarIO $ epochType bp2pEnv
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    let conn = rocksDB $ dbe'
        cfs = rocksCF dbe'
    debug lg $ LG.msg $ "Appending Tx to candidate block: " ++ show (txHash)
    mapM_
        (\bhash -> do
             q <- liftIO $ TSH.lookup (candidateBlocks bp2pEnv) (bhash)
             case q of
                 Nothing -> err lg $ LG.msg $ ("did-not-find : " ++ show bhash)
                 Just (seq, htab) -> do
                     res <-
                         mapM
                             (\txid -> do
                                  y <- liftIO $ TSH.lookup (htab) (txHash)
                                  case y of
                                      Nothing -> do
                                          trace lg $ LG.msg $ "parent Tx not found : " ++ show txid
                                          return False
                                      Just () -> return True)
                             depTxHashes
                     if all (\x -> x == True) res
                         then do
                             let __ = seq |> txHash
                             return ()
                         else return ()
                     return ())
        candBlockHashes
