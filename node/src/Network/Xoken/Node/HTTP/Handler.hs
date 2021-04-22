{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TupleSections #-}

module Network.Xoken.Node.HTTP.Handler where

import Arivi.P2P.Config (decodeHex, encodeHex)
import Control.Applicative ((<|>))
import qualified Control.Error.Util as Extra
import Control.Exception (SomeException(..), throw, try)
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Identity
import Control.Monad.State.Class
import qualified Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.Either as Either
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.Serialize as S
import qualified Data.Text as DT
import qualified Data.Text.Encoding as DTE
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Data
import Network.Xoken.Node.HTTP.Types
import Network.Xoken.Node.Service.Merchant
import Snap
import System.Logger as LG
import Text.Read (readMaybe)
import qualified Xoken.NodeConfig as NC

-- API functions
submitTransaction' :: MerchantApiReqParams -> Handler App App ()
submitTransaction' (SubmitTransactionRequest rawTx) = do
    res <- LE.try $ submitTransaction rawTx
    case res of
        Left DecodeException -> do
            modifyResponse $ setResponseStatus 400 "Bad Request"
            writeBS "Incorrectly encoded raw transaction"
        Left ValidationException -> do
            modifyResponse $ setResponseStatus 500 "Internal Server Error"
            writeBS "INTERNAL_SERVER_ERROR"
        Right (timeStamp, txId, result, resultDesc, bestBlockHash, bestBlockHeight) -> do
            writeBS $
                BSL.toStrict $
                A.encode $
                SubmitTransactionResponse "v1" timeStamp txId result resultDesc "0" bestBlockHash bestBlockHeight "null"
submitTransaction' _ = throwBadRequest

-- Helper functions
withReq :: A.FromJSON a => (a -> Handler App App ()) -> Handler App App ()
withReq handler = do
    rq <- getRequest
    let ct = getHeader "content-type" rq <|> (getHeader "Content-Type" rq) <|> (getHeader "Content-type" rq)
    if ct == Just "application/json"
        then do
            res <- LE.try $ readRequestBody (1024 * 1024 * 1024) -- 1 GiB size limit for request body
            case res of
                Left (e :: SomeException)
 --                   err lg $ LG.msg $ BC.pack $ "[ERROR] Failed to read POST request body: " <> show e
                 -> do
                    modifyResponse $ setResponseStatus 400 "Bad Request"
                    writeBS $ BC.pack $ "Error: failed to read request body (" <> (show e) <> ")"
                Right req ->
                    case A.eitherDecode req of
                        Right r -> handler r
                        Left err -> do
                            modifyResponse $ setResponseStatus 400 "Bad Request"
                            writeBS "Error: failed to decode request body JSON"
        else throwBadRequest

--    lg <- getLogger
throwChallenge :: Handler App App ()
throwChallenge = do
    modifyResponse $
        (setResponseStatus 401 "Unauthorized") . (setHeader "WWW-Authenticate" "Basic realm=my-authentication")
    writeBS ""

throwDenied :: Handler App App ()
throwDenied = do
    modifyResponse $ setResponseStatus 403 "Access Denied"
    writeBS "Access Denied"

throwBadRequest :: Handler App App ()
throwBadRequest = do
    modifyResponse $ setResponseStatus 400 "Bad Request"
    writeBS "Bad Request"

throwNotFound :: Handler App App ()
throwNotFound = do
    modifyResponse $ setResponseStatus 404 "Not Found"
    writeBS "Not Found"
