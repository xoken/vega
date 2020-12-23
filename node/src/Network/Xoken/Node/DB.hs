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
    bp2pEnv <- getBitcoinP2P
    let rkdb = rocksDB dbe'
        cf = rocksCF dbe'
        net = bitcoinNetwork $ nodeConfig bp2pEnv
        sb = S.encode $ shortBlockHash $ headerHash $ nodeHeader b
        bne = S.encode b
    R.put rkdb ("blocknode" :: B.ByteString) bne
    cfhm' <- liftIO $ TSH.lookup cf "blocktree"
    case cfhm' of
        Just cf' -> R.putCF rkdb cf' sb bne
        Nothing -> return ()

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
