{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.Process.Headers where

import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import Control.Monad.Extra (mapMaybeM)
import Control.Monad.Reader
import Control.Monad.STM
import qualified Data.List as L
import Data.Time.Clock.POSIX
import Network.Xoken.Block
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Node.DB
import Network.Xoken.Node.Env
import Network.Xoken.Node.Exception
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Node.Worker.Types
import System.Logger as LG
import Xoken.NodeConfig as NC

validateChainedBlockHeaders :: Headers -> Bool
validateChainedBlockHeaders hdrs = do
    let xs = headersList hdrs
        pairs = zip xs (drop 1 xs)
    L.foldl' (\ac x -> ac && (headerHash $ fst (fst x)) == (prevBlock $ fst (snd x))) True pairs

processHeaders :: (HasXokenNodeEnv env m, MonadIO m) => Headers -> m ()
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
                             else if fst bb == headerHash (fst $ last $ headersList hdrs)
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
