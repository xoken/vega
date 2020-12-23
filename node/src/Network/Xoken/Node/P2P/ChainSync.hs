{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.P2P.ChainSync
    ( runEgressChainSync
    , processHeaders
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async as CA (async, wait)
import Control.Concurrent.Async.Lifted as LA (async, mapConcurrently_, race)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Error.Util (hush)
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Extra (mapMaybeM)
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.Aeson as A (decode, eitherDecode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize as S
import Data.Store as DSE
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.RocksDB as R
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.DB
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import qualified Network.Xoken.Node.P2P.BlockSync as NXB (fetchBestSyncedBlock, markBestSyncedBlock)
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service.Chain
import Network.Xoken.Node.WorkerDispatcher
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig

produceGetHeadersMessage :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m (Message)
produceGetHeadersMessage = do
    lg <- getLogger
    debug lg $ LG.msg $ val "produceGetHeadersMessage - called."
    bp2pEnv <- getBitcoinP2P
    -- be blocked until a new best-block is updated in DB, or a set timeout.
    LA.race (liftIO $ threadDelay (15 * 1000000)) (liftIO $ takeMVar (bestBlockUpdated bp2pEnv))
    dbe <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
        rkdb = rocksDB dbe
    bl <- getBlockLocator rkdb net
    let gh =
            GetHeaders
                { getHeadersVersion = myVersion
                , getHeadersBL = bl
                , getHeadersHashStop = "0000000000000000000000000000000000000000000000000000000000000000"
                }
    debug lg $ LG.msg ("block-locator: " ++ show bl)
    return (MGetHeaders gh)

sendRequestMessages :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Message -> m ()
sendRequestMessages msg = do
    lg <- getLogger
    debug lg $ LG.msg $ val ("Chain - sendRequestMessages - called.")
    bp2pEnv <- getBitcoinP2P
    dbe' <- getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    case msg of
        MGetHeaders hdr -> do
            allPeers <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
            let connPeers = L.filter (\x -> bpConnected (snd x)) (M.toList allPeers)
            let fbh = getHash256 $ getBlockHash $ (getHeadersBL hdr) !! 0
                md = BSS.index fbh $ (BSS.length fbh) - 1
                pds =
                    map
                        (\p -> (fromIntegral (md + p) `mod` L.length connPeers))
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
                         case (bpSocket pr) of
                             Just q -> do
                                 let em = runPut . putMessage net $ msg
                                 liftIO $ sendEncMessage (bpWriteMsgLock pr) q (BSL.fromStrict em)
                                 debug lg $ LG.msg ("sending out GetHeaders: " ++ show (bpAddress pr))
                             Nothing -> debug lg $ LG.msg $ val "Error sending, no connections available")
                    indices
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    err lg $ LG.msg ("Error, sending out data: " ++ show e)
        ___ -> undefined

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

runEgressChainSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressChainSync = do
    lg <- getLogger
    res1 <- LE.try $ forever $ do produceGetHeadersMessage >>= sendRequestMessages
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg $ "[ERROR] runEgressChainSync " ++ show e

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
    L.foldl' (\ac x -> ac && (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) True pairs

markBestBlock :: (HasLogger m, MonadIO m) => R.DB -> Text -> Int32 -> m ()
markBestBlock rkdb hash height = do
    R.put rkdb "best_chain_tip_hash" $ DTE.encodeUtf8 hash
    R.put rkdb "best_chain_tip_height" $ C.pack $ show height
    --liftIO $ print "MARKED BEST BLOCK FROM ROCKS DB"

getBlockLocator :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => R.DB -> Network -> m ([BlockHash])
getBlockLocator rkdb net = do
    bp2pEnv <- getBitcoinP2P
    bn <- fetchBestBlock
    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
    return $ blockLocator hm bn
    {-
    (hash, ht) <- fetchBestBlock rkdb net
    debug lg $ LG.msg $ val "[rdb] fetchBestBlock from getBlockLocator - after"
    let bl = L.insert ht $ filter (> 0) $ takeWhile (< ht) $ map (\x -> ht - (2 ^ x)) [0 .. 20] -- [1,2,4,8,16,32,64,... ,262144,524288,1048576]
    res <-
        liftIO $
        try $ do
            v <- liftIO $ mapM (\key -> CA.async $ getDB' rkdb key) (bl)
            liftIO $ mapM CA.wait v
    case res of
        Right vs' -> do
            let vs :: [(Text, BlockHeader)]
                vs = catMaybes vs'
            if L.null vs
                then return [headerHash $ getGenesisHeader net]
                else do
                    debug lg $ LG.msg $ "Best-block from ROCKS DB: " ++ show (last $ vs)
                    return $ reverse $ catMaybes $ fmap (hexToBlockHash . fst) vs
        Left (e :: SomeException) -> do
            debug lg $ LG.msg $ "[Error] getBlockLocator: " ++ show e
            throw e
    -}

processHeaders :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Headers -> m ()
processHeaders hdrs = do
    dbe' <- getDB
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    if (L.null $ headersList hdrs)
        then do
            debug lg $ LG.msg $ val "Nothing to process!"
            throw EmptyHeadersMessageException
        else debug lg $ LG.msg $ "Processing Headers with " ++ show (L.length $ headersList hdrs) ++ " entries."
    case validateChainedBlockHeaders hdrs of
        True -> do
            let net = bitcoinNetwork $ nodeConfig bp2pEnv
                genesisHash = blockHashToHex $ headerHash $ getGenesisHeader net
                rkdb = rocksDB dbe'
                cf = rocksCF dbe'
                headPrevHash = (blockHashToHex $ prevBlock $ fst $ head $ headersList hdrs)
                hdrHash y = headerHash $ fst y
                validate m = validateWithCheckPoint net (fromIntegral m) (hdrHash <$> (headersList hdrs))
            bbn <- fetchBestBlock
            let bb = (headerHash $ nodeHeader bbn, nodeHeight bbn)
            -- TODO: throw exception if it's a bitcoin cash block
            indexed <-
                if (blockHashToHex $ fst bb) == genesisHash
                    then do
                        debug lg $ LG.msg $ val "First Headers set from genesis"
                        return $ zip [((snd bb) + 1) ..] (headersList hdrs)
                    else if (blockHashToHex $ fst bb) == headPrevHash
                             then do
                                 unless (validate (snd bb)) $ throw InvalidBlocksException
                                 debug lg $ LG.msg $ val "Building on current Best block"
                                 return $ zip [((snd bb) + 1) ..] (headersList hdrs)
                             else do
                                 if ((fst bb) == (headerHash $ fst $ last $ headersList hdrs))
                                     then do
                                         debug lg $ LG.msg $ LG.val ("Does not match best-block, redundant Headers msg")
                                         return [] -- already synced
                                     else do
                                         res <- fetchMatchBlockOffset rkdb headPrevHash
                                         case res of
                                             Just (matchBHash, matchBHt) -> do
                                                 unless (validate matchBHt) $ throw InvalidBlocksException
                                                 if ((snd bb) >
                                                     (matchBHt + fromIntegral (L.length $ headersList hdrs) + 12) -- reorg limit of 12 blocks
                                                     )
                                                     then do
                                                         debug lg $
                                                             LG.msg $
                                                             LG.val
                                                                 ("Does not match best-block, assuming stale Headers msg")
                                                         return [] -- assuming its stale/redundant and ignore
                                                     else do
                                                         debug lg $
                                                             LG.msg $
                                                             LG.val
                                                                 "Does not match best-block, potential block re-org..."
                                                         let reOrgDiff = zip [(matchBHt + 1) ..] (headersList hdrs)
                                                         bestSynced <- NXB.fetchBestSyncedBlock rkdb net
                                                         if snd bestSynced >= (fromIntegral matchBHt)
                                                             then do
                                                                 debug lg $
                                                                     LG.msg $
                                                                     "Have synced blocks beyond point of re-org: synced @ " <>
                                                                     (show bestSynced) <>
                                                                     " versus point of re-org: " <>
                                                                     (show $ (matchBHash, matchBHt)) <>
                                                                     ", re-syncing from thereon"
                                                                 NXB.markBestSyncedBlock matchBHash $ fromIntegral matchBHt
                                                                 return reOrgDiff
                                                             else return reOrgDiff
                                             Nothing -> throw BlockHashNotFoundException
            let lenIndexed = L.length indexed
            debug lg $ LG.msg $ "indexed " ++ show (lenIndexed)
            bns <- mapMaybeM
                (\y -> do
                     let header = fst $ snd y
                         blkht = fst y
                     tm <- liftIO $ floor <$> getPOSIXTime
                     bnm <- liftIO $ atomically
                                   $ stateTVar
                                        (blockTree bp2pEnv)
                                        (\hm -> case connectBlock hm net tm header of
                                                        Right (hm',bn) -> (Just bn,hm')
                                                        Left _ -> (Nothing,hm))
                     case bnm of
                        Just b -> putHeaderMemoryElem b
                        Nothing -> return ()
                     return $ (\x -> (x,(header,blkht))) <$> bnm)
                     --liftIO $ TSH.insert (blockTree bp2pEnv) (headerHash header) (fromIntegral blkht, header))
                indexed
            unless (L.null bns) $ do
                let headers = map (\z -> ZBlockHeader (fst $ snd z) (fromIntegral $ snd $ snd z)) bns
                zRPCDispatchNotifyNewBlockHeader headers
                putBestBlockNode $ fst $ last bns
                -- markBestBlock rkdb (blockHashToHex $ headerHash $ fst $ snd $ last $ indexed) (fst $ last indexed)
                liftIO $ putMVar (bestBlockUpdated bp2pEnv) True
        False -> do
            err lg $ LG.msg $ val "Error: BlocksNotChainedException"
            throw BlocksNotChainedException

fetchMatchBlockOffset :: (HasLogger m, MonadIO m) => R.DB -> Text -> m (Maybe (Text, BlockHeight))
fetchMatchBlockOffset rkdb hashes = do
    lg <- getLogger
    x <- liftIO $ R.get rkdb (C.pack . T.unpack $ hashes)
    case x of
        Nothing -> return Nothing
        Just h -> return $ Just $ (hashes, fromIntegral $ (read . T.unpack . DTE.decodeUtf8 $ h :: Int32))
