{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.DB where

import Control.Monad.Trans
import qualified Database.RocksDB as R
import Data.ByteString as B
import Data.Serialize as S
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Xoken.NodeConfig
import Network.Xoken.Block.Headers
import Network.Xoken.Block.Common

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
        Nothing -> return ()

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
    return $ getBestBlockNodeIO rkdb
    R.get rkdb ("blocknode" :: B.ByteString)

getBestBlockNodeIO :: (MonadIO m) => R.DB -> m (Maybe BlockNode)
getBestBlockNodeIO rkdb = do
    bne <- R.get rkdb ("blocknode" :: B.ByteString)
    return $ case S.decode <$> bne of
                Just (Right b) -> b
                _ -> Nothing
