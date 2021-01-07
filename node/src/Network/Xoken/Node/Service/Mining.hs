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

import Conduit hiding (runResourceT)
import Control.Concurrent.STM
import Control.Exception
import Data.Maybe
import qualified Data.Serialize as DS (encode)
import qualified Data.Text as DT
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.UUID
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers (computeSubsidy)
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.DB
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Transaction (makeCoinbaseTx)
import Network.Xoken.Util (encodeHex)
import System.Logger as LG
import System.Random
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
    (bestSyncedBlockHash, bestSyncedBlockHeight) <- fetchBestSyncedBlock
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
