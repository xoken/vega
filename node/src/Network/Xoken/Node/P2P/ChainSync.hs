{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.ChainSync
    ( runEgressChainSync
    , processHeaders
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (race)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Extra (mapMaybeM)
import Control.Monad.Reader
import Control.Monad.STM
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Short as BSS
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Serialize as S
import Data.Time.Clock.POSIX
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data
import Network.Xoken.Node.Exception
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.Version
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Node.Worker.Types
import System.Logger as LG
import Xoken.NodeConfig

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

sendGetHeaderMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Message -> m ()
sendGetHeaderMessage msg = do
    lg <- getLogger
    debug lg $ LG.msg $ val "Chain - sendGetHeaderMessage - called."
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

{- UNUSED?
msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT
-}
runEgressChainSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressChainSync = do
    lg <- getLogger
    res1 <- LE.try $ forever $ produceGetHeadersMessage >>= sendGetHeaderMessage
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg $ "[ERROR] runEgressChainSync " ++ show e

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
    L.foldl' (\ac x -> ac && (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) True pairs

getBlockLocator :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m BlockLocator
getBlockLocator = do
    bp2pEnv <- getBitcoinP2P
    bn <- fetchBestBlock
    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
    return $ blockLocator hm bn

processHeaders :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Headers -> m ()
processHeaders hdrs = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    if L.null $ headersList hdrs
        then do
            debug lg $ LG.msg $ val "Nothing to process!"
            throw EmptyHeadersMessageException
        else debug lg $ LG.msg $ "Processing Headers with " ++ show (L.length $ headersList hdrs) ++ " entries."
    if validateChainedBlockHeaders hdrs
        then do
            let net = bitcoinNetwork $ nodeConfig bp2pEnv
                genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
                headPrevBlockHash = prevBlock $ fst $ head $ headersList hdrs
                headPrevHash = blockHashToHex headPrevBlockHash
                hdrHash y = headerHash $ fst y
                validate m = validateWithCheckPoint net (fromIntegral m) (hdrHash <$> headersList hdrs)
            bbn <- fetchBestBlock
            let bb = (headerHash $ nodeHeader bbn, nodeHeight bbn)
            debug lg $ LG.msg $ "Fetched best block: " ++ show bb
            -- TODO: throw exception if it's a bitcoin cash block
            indexed <-
                if blockHashToHex (fst bb) == genesisHash
                    then do
                        debug lg $ LG.msg $ val "First Headers set from genesis"
                        return $ zip [snd bb + 1 ..] (headersList hdrs)
                    else if blockHashToHex (fst bb) == headPrevHash
                             then do
                                 unless (validate (snd bb)) $ throw InvalidBlocksException
                                 debug lg $ LG.msg $ val "Building on current Best block"
                                 return $ zip [snd bb + 1 ..] (headersList hdrs)
                             else
                                 if fst bb == headerHash (fst $ last $ headersList hdrs)
                                     then do
                                         debug lg $ LG.msg $ LG.val "Does not match best-block, redundant Headers msg"
                                         return [] -- already synced
                                     else do
                                         res <- fetchMatchBlockOffset headPrevBlockHash
                                         case res of
                                             Just matchBHt -> do
                                                 unless (validate matchBHt) $ throw InvalidBlocksException
                                                 if snd bb >
                                                     (matchBHt + fromIntegral (L.length $ headersList hdrs) + 12) -- reorg limit of 12 blocks
                                                     then do
                                                         debug lg $
                                                             LG.msg $
                                                             LG.val
                                                                 "Does not match best-block, assuming stale Headers msg"
                                                         return [] -- assuming its stale/redundant and ignore
                                                     else do
                                                         debug lg $
                                                             LG.msg $
                                                             LG.val
                                                                 "Does not match best-block, potential block re-org..."
                                                         let reOrgDiff = zip [(matchBHt + 1) ..] (headersList hdrs)
                                                         bestSynced <- fetchBestSyncedBlock
                                                         if snd bestSynced >= fromIntegral matchBHt
                                                             then do
                                                                 debug lg $
                                                                     LG.msg $
                                                                     "Have synced blocks beyond point of re-org: synced @ " <>
                                                                     show bestSynced <>
                                                                     " versus point of re-org: " <>
                                                                     show (headPrevHash, matchBHt) <>
                                                                     ", re-syncing from thereon"
                                                                 markBestSyncedBlock headPrevHash $
                                                                     fromIntegral matchBHt
                                                                 return reOrgDiff
                                                             else return reOrgDiff
                                             Nothing -> throw BlockHashNotFoundException
            let lenIndexed = L.length indexed
            debug lg $ LG.msg $ "indexed " ++ show lenIndexed
            bns <-
                mapMaybeM
                    (\y -> do
                         let header = fst $ snd y
                             blkht = fst y
                         tm <- liftIO $ floor <$> getPOSIXTime
                         bnm <-
                             liftIO $
                             atomically $
                             stateTVar
                                 (blockTree bp2pEnv)
                                 (\hm ->
                                      case connectBlock hm net tm header of
                                          Right (hm', bn) -> (Just bn, hm')
                                          Left _ -> (Nothing, hm))
                         mapM_ putHeaderMemoryElem bnm
                         return $ (, (header, blkht)) <$> bnm)
                     --liftIO $ TSH.insert (blockTree bp2pEnv) (headerHash header) (fromIntegral blkht, header))
                    indexed
            unless (L.null bns) $ do
                let headers = map (\z -> ZBlockHeader (fst $ snd z) (fromIntegral $ snd $ snd z)) bns
                    bnode = fst $ last bns
                zRPCDispatchNotifyNewBlockHeader headers bnode
                putBestBlockNode bnode
                -- markBestBlock rkdb (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed)
                liftIO $ putMVar (bestBlockUpdated bp2pEnv) True
        else do
            err lg $ LG.msg $ val "Error: BlocksNotChainedException"
            throw BlocksNotChainedException

fetchMatchBlockOffset :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockHash -> m (Maybe BlockHeight)
fetchMatchBlockOffset hash = do
    bp2pEnv <- getBitcoinP2P
    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
    case getBlockHeaderMemory hash hm of
        Nothing -> return Nothing
        Just bn -> return $ Just $ nodeHeight bn
