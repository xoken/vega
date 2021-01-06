{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.DB where

import Control.Monad.Trans
import qualified Database.RocksDB as R
import Data.ByteString as B
import Data.Default
import Data.Serialize as S
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Xoken.NodeConfig
import Network.Xoken.Block.Headers
import Network.Xoken.Block.Common

conf :: R.Config
conf = def {R.createIfMissing = True, R.errorIfExists = False, R.bloomFilter = True, R.prefixLength = Just 3}

cfStr = ["outputs", "ep_outputs_odd", "ep_outputs_even", "ep_transactions_odd", "ep_transactions_even", "tx", "blocktree", "provisional_blockhash"]

columnFamilies = fmap (\x -> (x, conf)) cfStr

withDBCF path = R.withDBCF path conf columnFamilies

putHeaderMemoryElem :: (HasXokenNodeEnv env m, MonadIO m) => BlockNode -> m ()
putHeaderMemoryElem b = do
    dbe' <- getDB
    let rkdb = rocksDB dbe'
        cf = rocksCF dbe'
        sb = S.encode $ shortBlockHash $ headerHash $ nodeHeader b
        bne = S.encode b
    cfhm' <- liftIO $ TSH.lookup cf "blocktree"
    case cfhm' of
        Just cf' -> R.putCF rkdb cf' sb bne
        Nothing -> do
            liftIO $ print "Couldn't get cf"
            return ()

putHeaderMemoryElemIO :: (MonadIO m) => R.DB -> R.ColumnFamily -> BlockNode -> m ()
putHeaderMemoryElemIO rkdb cf b = do
    let sb = S.encode $ shortBlockHash $ headerHash $ nodeHeader b
        bne = S.encode b
    R.putCF rkdb cf sb bne

scanCF db cf = liftIO $ do
    R.withIterCF db cf $ \iter -> do
        R.iterFirst iter
        getNext iter
    where --getNext :: Iterator -> m [(Maybe ByteString,Maybe ByteString)]
          getNext i = do
              valid <- R.iterValid i
              if valid
                  then do
                    kv <- R.iterEntry i
                    R.iterNext i
                    sn <- getNext i
                    return $ case kv of
                                Just (k,v) -> ((k,v):sn)
                                _ -> sn
                  else
                      return []

putBestBlockNode :: (HasXokenNodeEnv env m, MonadIO m) => BlockNode -> m ()
putBestBlockNode b = do
    dbe' <- getDB
    let rkdb = rocksDB dbe'
        bne = S.encode b
    R.put rkdb ("blocknode" :: B.ByteString) bne


getBestBlockNode :: (HasXokenNodeEnv env m, MonadIO m) => m (Maybe BlockNode)
getBestBlockNode = do
    dbe' <- getDB
    let rkdb = rocksDB dbe'
    getBestBlockNodeIO rkdb

getBestBlockNodeIO :: (MonadIO m) => R.DB -> m (Maybe BlockNode)
getBestBlockNodeIO rkdb = do
    bne <- R.get rkdb ("blocknode" :: B.ByteString)
    return $ case S.decode <$> bne of
                Just (Right b) -> Just b
                _ -> Nothing
