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

module Network.Xoken.Node.Service.Mining where

import Arivi.P2P.MessageHandler.HandlerTypes (HasNetworkConfig, networkConfig)
import Arivi.P2P.P2PEnv
import Arivi.P2P.PubSub.Class
import Arivi.P2P.PubSub.Env
import Arivi.P2P.PubSub.Publish as Pub
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Env
import Arivi.P2P.RPC.Fetch
import Arivi.P2P.Types hiding (msgType)
import Codec.Compression.GZip as GZ
import Codec.Serialise
import Conduit hiding (runResourceT)
import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (AsyncCancelled, mapConcurrently, mapConcurrently_, race_)
import qualified Control.Concurrent.Async.Lifted as LA (async, concurrently, mapConcurrently, wait)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TVar
import qualified Control.Error.Util as Extra
import Control.Exception
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import Data.ByteString.Base64 as B64
import Data.ByteString.Base64.Lazy as B64L
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Short as BSS
import qualified Data.ByteString.UTF8 as BSU (toString)
import Data.Char
import Data.Default
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.IORef
import Data.Int
import Data.List
import qualified Data.List as L
import Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import qualified Data.Serialize as S
import Data.Serialize
import qualified Data.Serialize as DS (decode, encode)
import qualified Data.Set as S
import Data.String (IsString, fromString)
import qualified Data.Text as DT
import qualified Data.Text.Encoding as DTE
import qualified Data.Text.Encoding as E
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.UUID
import Data.Word
import Data.Yaml
import qualified Database.Bolt as BT
import qualified Network.Simple.TCP.TLS as TLS
import Network.Xoken.Address.Base58
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers (computeSubsidy)
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.BlockSync (fetchBestSyncedBlock)
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction (makeCoinbaseTx)
import Network.Xoken.Util (encodeHex)
import Numeric (showHex)
import System.Logger as LG
import System.Logger.Message
import System.Random
import Text.Read
import Xoken
import qualified Xoken.NodeConfig as NC

generateUuid :: IO UUID
generateUuid =
    getStdGen >>= \g -> do
        let (uuid, g') = random g :: (UUID, StdGen)
         in setStdGen g' >> return uuid

getMiningCandidate :: (HasXokenNodeEnv env m, MonadIO m) => m RPCResponseBody
getMiningCandidate = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    nodeCfg <- nodeConfig <$> getBitcoinP2P
    net <- (NC.bitcoinNetwork . nodeConfig) <$> getBitcoinP2P
    rkdb <- rocksDB <$> getDB
    (bestSyncedBlockHash, bestSyncedBlockHeight) <- fetchBestSyncedBlock rkdb net
    debug lg $
        LG.msg $ "getMiningCandidate: got best-synced block: " <> (show (bestSyncedBlockHash, bestSyncedBlockHeight))
    hm <- (liftIO . readTVarIO) $ blockTree bp2pEnv
    debug lg $ LG.msg $ show "getMiningCandidate: got header memory"
    let candidateBlocksTsh = candidateBlocks bp2pEnv
    coinbaseAddress <-
        case stringToAddr (NC.bitcoinNetwork nodeCfg) (DT.pack $ NC.coinbaseTxAddress nodeCfg) of
            Nothing -> do
                err lg $
                    LG.msg $
                    "Error: Failed to decode supplied Coinbase address " <>
                    (show $ DT.pack $ NC.coinbaseTxAddress nodeCfg)
                debug lg $
                    LG.msg $
                    "getMiningCandidate: Failed to decode supplied Coinbase address " <>
                    (show $ DT.pack $ NC.coinbaseTxAddress nodeCfg)
                throw KeyValueDBLookupException
            Just a -> return a
    let cbByUuidTSH = candidatesByUuid bp2pEnv
    candidateBlock <- liftIO $ TSH.lookup candidateBlocksTsh bestSyncedBlockHash
    case candidateBlock of
        Nothing -> do
            err lg $
                LG.msg $
                "Error: Failed to fetch candidate block, previous block: " <>
                (show (bestSyncedBlockHash, bestSyncedBlockHeight))
            throw KeyValueDBLookupException
        Just blk -> do
            (txCount, satVal, bcState, mbCoinbaseTxn) <- liftIO $ DAG.getCurrentPrimaryTopologicalState blk
            uuid <- liftIO generateUuid
            let (merkleBranch, merkleRoot) =
                    (\(b, r) -> (txHashToHex <$> b, fromJust r)) $ computeMerkleBranch bcState (fromJust mbCoinbaseTxn)
                coinbaseTx =
                    DT.unpack $
                    encodeHex $
                    DS.encode $
                    makeCoinbaseTx
                        (1 + fromIntegral bestSyncedBlockHeight)
                        coinbaseAddress
                        (computeSubsidy (NC.bitcoinNetwork nodeCfg) (fromIntegral $ bestSyncedBlockHeight))
            -- persist generated UUID and txCount in memory
            liftIO $ TSH.insert cbByUuidTSH uuid (fromIntegral txCount, merkleRoot)
            timestamp <- liftIO $ (getPOSIXTime :: IO NominalDiffTime)
            let parentBlock = memoryBestHeader hm
                candidateHeader = BlockHeader 0 (BlockHash "") "" (round timestamp) 0 0
                nextWorkRequired = getNextWorkRequired hm net parentBlock candidateHeader
            return $
                GetMiningCandidateResp
                    (toString uuid)
                    (DT.unpack $ blockHashToHex bestSyncedBlockHash)
                    (Just coinbaseTx)
                    (fromIntegral txCount)
                    0x37ffe000
                    (fromIntegral satVal)
                    (fromIntegral nextWorkRequired)
                    (round timestamp)
                    (1 + fromIntegral bestSyncedBlockHeight)
                    (DT.unpack <$> merkleBranch)
