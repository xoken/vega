{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.Service.Merchant where

import Conduit hiding (runResourceT)
import Control.Concurrent.STM
import Control.Exception
import Control.Exception.Lifted as LE
import Data.ByteString
import Data.ByteString.Char8 as BC
import Data.Int
import Data.Maybe
import Data.Serialize
import qualified Data.Serialize as DS (encode)
import qualified Data.Text as DT
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.Format.ISO8601.Compat (formatShow, iso8601Format)
import Data.Word
import Data.Yaml
import qualified Database.Bolt as BT
import qualified Network.Simple.TCP.TLS as TLS
import Network.Xoken.Address.Base58
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers (computeSubsidy)
import Network.Xoken.Block.Merkle
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Node.DB
import Network.Xoken.Node.DB (fetchBestSyncedBlock)
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.Exception
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.UnconfTxSync
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Transaction (makeCoinbaseTx)
import Network.Xoken.Util (encodeHex)
import System.Logger as LG
import System.Random
import Text.Read
import Text.Show
import Xoken
import qualified Xoken.NodeConfig as NC

data SubmitTransactionException
    = DecodeException
    | ValidationException
    deriving (Show, Eq)

instance Exception SubmitTransactionException

submitTransaction :: (HasXokenNodeEnv env m, MonadIO m) => ByteString -> m (String, String, String, String, String, Int)
submitTransaction rawTx = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    case runGetState (getConfirmedTx) (rawTx) 0 of
        Left e -> do
            err lg $
                LG.msg $
                BC.pack $ "[ERROR] Failed to decode raw transaction (submitTransaction/merchant API): " ++ (show e)
            throw DecodeException
        Right res ->
            case fst res of
                Just tx -> do
                    res <- LE.try $ zRPCDispatchUnconfirmedTxValidate processUnconfTransaction tx
                    case res of
                        Right depTxHashes -> do
                            tm <- liftIO $ getCurrentTime
                            bestBlock <- fetchBestBlock
                            let timeStamp = formatShow iso8601Format tm
                                txId = DT.unpack $ txHashToHex $ txHash tx
                                bestBlockHash = DT.unpack $ blockHashToHex $ headerHash $ nodeHeader bestBlock
                                bestBlockHeight = fromIntegral $ nodeHeight bestBlock
                            candBlks <- liftIO $ TSH.toList (candidateBlocks bp2pEnv)
                            addTxCandidateBlocks (txHash tx) (fst <$> candBlks) depTxHashes
                            return (timeStamp, txId, "success", "", bestBlockHash, bestBlockHeight)
                        Left (e :: SomeException) -> do
                            err lg $
                                LG.msg
                                    ("[ERROR] Exception while processing unconfirmed transaction (submitTransaction/merchant API):" ++
                                     show e)
                            throw ValidationException
                Nothing -> do
                    err lg $ LG.msg $ BC.pack $ "[ERROR] Empty raw transaction (submitTransaction/merchant API)"
                    throw DecodeException
