{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.P2P.ChainSync
    ( runEgressChainSync
    ) where

import Network.Xoken.Node.P2P.MessageSender
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

runEgressChainSync :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runEgressChainSync = do
    lg <- getLogger
    res1 <- LE.try $ forever $ produceGetHeadersMessage >>= sendGetHeaderMessages
    case res1 of
        Right () -> return ()
        Left (e :: SomeException) -> err lg $ LG.msg $ "[ERROR] runEgressChainSync " ++ show e
