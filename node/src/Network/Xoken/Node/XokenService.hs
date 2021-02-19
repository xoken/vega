{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.XokenService where

import Conduit hiding (runResourceT)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Data.IORef
import Data.Maybe
import qualified Network.Simple.TCP.TLS as TLS
import Network.Xoken.Node.Data
import Network.Xoken.Node.Env
import Network.Xoken.Node.Service.Mining
import System.Logger as LG
import Xoken

data EncodingFormat
    = CBOR
    | JSON
    | DEFAULT

data EndPointConnection =
    EndPointConnection
        { requestQueue :: TQueue XDataReq
        , context :: MVar TLS.Context
        , encodingFormat :: IORef EncodingFormat
        }

goGetResource :: (HasXokenNodeEnv env m, MonadIO m) => RPCMessage -> Network -> m (RPCMessage)
goGetResource msg net = do
    lg <- getLogger
    case rqMethod msg of
        "GET_MINING_CANDIDATE" -> do
            case rqParams msg of
                GetMiningCandidateRequest provideCoinbaseTx -> do
                    resp <- LE.try $ getMiningCandidate
                    case resp of
                        Left (e :: SomeException) -> do
                            err lg $ LG.msg $ "Error: GET_MINING_CANDIDATE: " <> (show e)
                            return $ RPCResponse 400 $ Left $ RPCError INTERNAL_ERROR Nothing
                        Right r' ->
                            return $
                            RPCResponse 200 $
                            Right $
                            Just $
                            if fromMaybe False provideCoinbaseTx
                                then r'
                                else r' {rgmcCoinbase = Nothing}
                _ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_PARAMS Nothing
        _____ -> return $ RPCResponse 400 $ Left $ RPCError INVALID_METHOD Nothing
