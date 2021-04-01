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
import Data.Int
import qualified Data.Serialize as DS (encode)
import qualified Data.Text as DT
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.UUID
import qualified Data.UUID as UUID (fromString)
import Data.Word
import Data.Yaml
import qualified Database.Bolt as BT
import qualified Network.Simple.TCP.TLS as TLS
import Network.Xoken.Address.Base58
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers (computeSubsidy)
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Exception
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.DB (fetchBestSyncedBlock)
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Transaction (makeCoinbaseTx)
import Network.Xoken.Util (encodeHex)
import System.Logger as LG
import System.Random
import Text.Read
import Text.Show
import Xoken
import qualified Xoken.NodeConfig as NC

compactBlockFromMiningCandidateData :: MiningCandidateData -> CompactBlock
compactBlockFromMiningCandidateData MiningCandidateData {..} = 
    let cbase = fromMaybe mcdCoinbase mcdMinerCoinbase
        txhashes = (txHash cbase : mcdTxHashes)
        bh = BlockHeader
                (fromMaybe mcdVersion mcdMinerVersion)
                mcdPrevHash
                (buildMerkleRoot txhashes)
                (fromMaybe mcdTime mcdMinerTime)
                mcdnBits
                (fromIntegral mcdMinerNonce)
        sidl = fromIntegral $ length $ mcdTxHashes -- shortIds length
        skey = getCompactBlockSipKey bh $ fromIntegral mcdMinerNonce
        pfl = 1
        pftx = [PrefilledTx 0 $ cbase]
        (cbsid:sids) = map (\txid -> txHashToShortId' txid skey) $ txhashes -- shortIds
    in CompactBlock bh (fromIntegral mcdMinerNonce) sidl sids pfl pftx

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
            newCandidateBlockChainTip
            throw KeyValueDBLookupException
        Just blk -> do
            (txHashes, txCount, satVal, bcState, mbCoinbaseTxn) <- liftIO $ DAG.getCurrentPrimaryTopologicalStateWithValue blk
            pts' <- liftIO $ DAG.getPrimaryTopologicalSorted blk
            let cbase = makeCoinbaseTx
                            (1 + fromIntegral bestSyncedBlockHeight)
                            coinbaseAddress
                            (computeSubsidy (NC.bitcoinNetwork nodeCfg) (fromIntegral $ bestSyncedBlockHeight))
                coinbaseTx = DT.unpack $ encodeHex $ DS.encode $ cbase
                pts = txHash cbase : pts
                sibling =
                    if length pts >= 2
                        then Just $ pts !! 1
                        else Nothing
                (merkleBranch', merkleRoot) =
                    (\(b, r) -> (TxHash <$> b, TxHash $ fromJust r)) $ getProof bcState       
                merkleBranch =
                    case sibling of
                        Just s -> (s : merkleBranch')
                        Nothing -> merkleBranch'
            uuid <- liftIO generateUuid
            timestamp <- liftIO $ (getPOSIXTime :: IO NominalDiffTime)
            let parentBlock = memoryBestHeader hm
                candidateHeader = BlockHeader 0 (BlockHash "") "" (round timestamp) 0 0
                nextWorkRequired = getNextWorkRequired hm net parentBlock candidateHeader
            liftIO $
                TSH.insert
                    cbByUuidTSH
                    uuid $
                    MiningCandidateData bestSyncedBlockHash
                                        cbase
                                        (fromIntegral txCount)
                                        0x20000000
                                        (fromIntegral satVal)
                                        (fromIntegral nextWorkRequired)
                                        (round timestamp)
                                        (1 + fromIntegral bestSyncedBlockHeight)
                                        merkleBranch
                                        txHashes
                                        0
                                        Nothing
                                        Nothing
                                        Nothing 
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
                    (fmap (DT.unpack . txHashToHex) merkleBranch)

submitMiningSolution ::
       (HasXokenNodeEnv env m, MonadIO m) => String -> Int32 -> Maybe String -> Maybe Int32 -> Maybe Int32 -> m Bool
submitMiningSolution id nonce coinbase time version = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    nodeCfg <- nodeConfig <$> getBitcoinP2P
    net <- (NC.bitcoinNetwork . nodeConfig) <$> getBitcoinP2P
    rkdb <- rocksDB <$> getDB
    let candidateBlocksByUuid = candidatesByUuid bp2pEnv
        mbUuid = UUID.fromString id
    uuid <-
        case mbUuid of
            Nothing -> do
                throw UuidFormatException
            Just u' -> return u'
    mbCandidateBlockState <- liftIO $ TSH.lookup candidateBlocksByUuid uuid
    miningData <-
        case mbCandidateBlockState of
            Nothing -> throw BlockCandidateIdNotFound
            Just cb -> return cb
    let cmpctblk = compactBlockFromMiningCandidateData
                    $ miningData
                        { mcdMinerNonce = fromIntegral nonce
                        , mcdMinerCoinbase = Nothing
                        , mcdMinerTime = fromIntegral <$> time
                        , mcdMinerVersion = fromIntegral <$> version
                        }
        bh = cbHeader cmpctblk
        bhsh@(BlockHash bhsh') = headerHash bh
    if isValidPOW net bh
        then do
            debug lg $ LG.msg $ "Mined Candidate Block: " ++ show uuid ++ "; Blockhash: " ++ show bhsh ++ "; header: " ++ show bh
            liftIO $ TSH.insert (compactBlocks bp2pEnv) bhsh (cmpctblk,mcdTxHashes miningData)
            newCandidateBlock bhsh
            broadcastToPeers $ MInv $ Inv [InvVector InvBlock bhsh']
            return True
        else do
            debug lg $ LG.msg $ "Invalid POW for candidate id " ++ show uuid ++ "; invalid nonce: " ++ show nonce
            throw InvalidPOW
