{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.P2P.Version where
import Data.Word
import Data.ByteString
import Network.Xoken.Block.Common
import Network.Xoken.Constants
import Network.Xoken.Network.Common

-- | Create version data structure.
buildVersion :: Network -> Word64 -> BlockHeight -> NetworkAddress -> NetworkAddress -> Word64 -> Maybe ByteString -> Version
buildVersion net nonce height loc rmt time assoc =
    Version
        { version = myVersion
        , services = naServices loc
        , timestamp = time
        , addrRecv = rmt
        , addrSend = loc
        , verNonce = nonce
        , userAgent = VarString (getXokenUserAgent net)
        , startHeight = height
        , relay = True
        , assocID = VarString <$> (Just "11")
        }

-- | Our protocol version.
myVersion :: Word32
myVersion = 70015
