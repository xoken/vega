{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE DeriveAnyClass #-}

module Xoken.NodeConfig
    ( module Xoken.NodeConfig
    ) where

import Arivi.P2P.Kademlia.Types
import Codec.Serialise
import Control.Exception
import Control.Monad (guard)
import Crypto.Secp256k1
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Char8 as C
import Data.Hashable
import Data.Int
import Data.Maybe
import Data.Text as T
import qualified Data.Text.Encoding as E
import Data.Word
import Data.Yaml
import GHC.Generics
import Network.Socket
import Network.Xoken.Constants
import Network.Xoken.Constants
import System.Logger

data NodeConfig =
    NodeConfig
        { vegaNode :: Node
        , vegaCluster :: [Node]
        , bitcoinNetwork :: Network
        , logLevel :: Level
        , logFileName :: T.Text
        , bitcoinNodeListenIP :: String
        , bitcoinNodeListenPort :: PortNumber
        , endPointHTTPSListenIP :: String
        , endPointHTTPSListenPort :: PortNumber
        , allegoryVendorSecretKey :: SecKey
        , maxTxProcThreads :: Int
        , maxBitcoinPeerCount :: Int
        , unresponsivePeerConnTimeoutSecs :: Int
        , acivatePeerDiscovery :: Bool
        , maxTMTBuilderThreads :: Int
        , maxTMTQueueSize :: Int
        , txProcInputDependenciesWait :: Int
        , txProcTimeoutSecs :: Int
        , tlsCertificatePath :: FilePath
        , tlsKeyfilePath :: FilePath
        , tlsCertificateStorePath :: FilePath
        , neo4jUsername :: T.Text
        , neo4jPassword :: T.Text
        , allegoryNameUtxoSatoshis :: Int
        , allegoryTxFeeSatsProducerAction :: Int
        , allegoryTxFeeSatsOwnerAction :: Int
        , txOutputValuesCacheKeyBits :: Int
        , unconfirmedTxCacheKeyBits :: Int
        , blockProcessingTimeout :: Int
        , recentTxReceiveTimeout :: Int
        , getDataResponseTimeout :: Int
        , shardingHashSecretSalt :: Word32
        , endPointTLSListenIP :: String
        , endPointTLSListenPort :: PortNumber
        , maxTxProcessingThreads :: Int
        , maxTxProcessingBuffer :: Int
        }
    deriving (Show, Generic)

data Node =
    Node
        { _nodeID :: String
        , _nodeType :: NodeType
        , _nodeRoles :: [NodeRole]
        , _nodeIPAddr :: String
        , _nodePort :: Word16
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data NodeType
    = Master
    | Compute
    deriving (Eq, Show, Read, Hashable, Generic, Serialise)

instance FromJSON NodeType

data NodeRole
    = TxValidation
    | OutputStore
    | GetOutpoint
    | TraceOutputs
    deriving (Eq, Show, Read, Hashable, Generic, Serialise)

instance FromJSON NodeRole

instance FromJSON Node where
    parseJSON (Object o) =
        (Node <$> o .: "nodeID" <*> o .: "nodeType" <*> o .: "nodeRoles" <*> o .: "nodeIPAddr" <*> o .: "nodePort")

instance FromJSON ByteString where
    parseJSON = withText "ByteString" $ \t -> pure $ fromJust (decodeHex t)

instance FromJSON PortNumber where
    parseJSON v = fromInteger <$> parseJSON v

instance FromJSON SecKey where
    parseJSON v = fromJust <$> secKey <$> (parseJSON v :: Parser ByteString)

instance FromJSON Network where
    parseJSON v = fromJust <$> netByName <$> parseJSON v

instance FromJSON Level where
    parseJSON v = read <$> parseJSON v

instance FromJSON NodeConfig

readConfig :: FilePath -> IO NodeConfig
readConfig path = do
    config <- decodeFileEither path :: IO (Either ParseException NodeConfig)
    case config of
        Left e -> throw e
        Right con -> return con

-- | Decode string of human-readable hex characters.
decodeHex :: Text -> Maybe ByteString
decodeHex text =
    let (x, b) = B16.decode (E.encodeUtf8 text)
     in guard (b == BS.empty) >> return x
