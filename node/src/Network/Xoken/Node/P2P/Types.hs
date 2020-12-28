{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE StandaloneDeriving #-}

module Network.Xoken.Node.P2P.Types where

import Codec.Serialise
import Control.Concurrent.MSem as MS
import Control.Concurrent.MSemN as MSN
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM
import Control.Concurrent.STM.TSem
import Control.Monad.IO.Class
import Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import Data.Functor.Identity
import Data.IORef
import Data.Int
import qualified Data.Map.Strict as M
import Data.Pool
import Data.Time.Clock
import Data.Word
import Database.Bolt as BT
import qualified Database.RocksDB as R

import Data.Store
import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Block
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Transaction
import StmContainers.Map as SM
import System.Random
import Text.Read
import Xoken.NodeConfig

-- | Type alias for a combination of hostname and port.
type HostPort = (Host, Port)

-- | Type alias for a hostname.
type Host = String

-- | Type alias for a port number.
type Port = Int

data DatabaseHandles =
    DatabaseHandles
        { rocksDB :: !R.DB
        , rocksCF :: !(TSH.TSHashTable String R.ColumnFamily)
        }

-- | Data structure representing an bitcoin peer.
data BitcoinPeer =
    BitcoinPeer
        { bpAddress :: !SockAddr --  network address
        , bpSocket :: !(Maybe Socket) --  live stream socket
        , bpWriteMsgLock :: !(MVar ()) --  write message lock
        , bpConnected :: !Bool --  peer is connected and ready
        , bpVersion :: !(Maybe Version) -- protocol version
        , bpNonce :: !Word64 -- random nonce sent during handshake
        , statsTracker :: !PeerTracker -- track sync stats
        , blockFetchQueue :: !(MVar (BlockInfo))
        , bpSendcmpt :: !(IORef Bool) -- does the peer ask for CompactBlocks
        }

data PeerTracker =
    PeerTracker
        { ptIngressMsgCount :: !(IORef Int) -- recent msg count for detecting stale peer connections
        , ptLastTxRecvTime :: !(IORef (Maybe UTCTime)) -- last tx recv time
        , ptLastGetDataSent :: !(IORef (Maybe UTCTime)) -- block 'GetData' sent time
        , ptBlockFetchWindow :: !(IORef Int) -- number of outstanding blocks
        -- ptLastPing , Ping :: !(Maybe (UTCTime, Word64)) -- last sent ping time and nonce
        }

getNewTracker :: IO (PeerTracker)
getNewTracker = do
    imc <- liftIO $ newIORef 0
    rc <- liftIO $ newIORef Nothing
    st <- liftIO $ newIORef Nothing
    fw <- liftIO $ newIORef 0
    return $ PeerTracker imc rc st fw

instance Show BitcoinPeer where
    show p = (show $ bpAddress p) ++ " : " ++ (show $ bpConnected p)

data BlockInfo =
    BlockInfo
        { biBlockHash :: !BlockHash
        , biBlockHeight :: !BlockHeight
        }
    deriving (Show)

data MerkleNode =
    MerkleNode
        { node :: !(Maybe Hash256)
        , leftChild :: !(Maybe Hash256)
        , rightChild :: !(Maybe Hash256)
        , isLeft :: !Bool
        }
    deriving (Show, Eq, Ord)

type HashCompute = (M.Map Int8 (MerkleNode), [MerkleNode])

data BranchComputeState =
    BranchComputeState
        { hashCompute :: HashCompute
        , txCount :: Word32
        , lastTxn :: Maybe TxHash
        }
    deriving (Show, Eq, Ord)

emptyHashCompute :: HashCompute
emptyHashCompute = (M.empty, [])

emptyBranchComputeState = BranchComputeState emptyHashCompute 0 Nothing

emptyMerkleNode :: MerkleNode
emptyMerkleNode = MerkleNode {node = Nothing, leftChild = Nothing, rightChild = Nothing, isLeft = False}

data IngressStreamState =
    IngressStreamState
        { issBlockIngest :: !BlockIngestState
        , issBlockInfo :: !(Maybe BlockInfo)
        }
    deriving (Show)

data BlockIngestState =
    BlockIngestState
        { binUnspentBytes :: !LB.ByteString
        , binTxPayloadLeft :: !Int64
        , binTxTotalCount :: !Int
        , binTxIngested :: !Int
        , binBlockSize :: !Int
        , binChecksum :: !CheckSum32
        }
    deriving (Show)

data BlockSyncStatus
    = RequestSent !UTCTime
    | RequestQueued
    | RecentTxReceiveTime !(UTCTime, Int)
    | BlockReceiveComplete !UTCTime
    | BlockProcessingComplete
    deriving (Eq, Ord, Show)

-- |A pool of connections to Neo4j server
data ServerState =
    ServerState
        { pool :: !(Pool Pipe)
        }

data ZtxiUtxo =
    ZtxiUtxo
        { zuTxHash :: !TxHash
        , zuOpIndex :: !Word32
        , zuBlockHash :: ![BlockHash]
        , zuBlockHeight :: !Word32
        , zuInputs :: ![(TxHash, Word32)]
        , zuSpending :: ![Spending]
        , zuSatoshiValue :: !Word64
        , zuOpCount :: !Word32
        }
    deriving (Show, Eq, Ord, Generic, Serialise)

deriving instance Store ZtxiUtxo

data Spending =
    Spending
        { spBlockHash :: !BlockHash
        , spTxShortHash :: !TxHash
        , spInputIndex :: !Word32
        }
    deriving (Show, Eq, Ord, Generic, Serialise)

deriving instance Store Spending

data Worker
    = SelfWorker
          { selfID :: String
          , selfRoles :: [NodeRole]
          }
    | RemoteWorker
          { woID :: !String
          , woIP :: !String
          , woPort :: !Word16
          , woSocket :: !Socket
          , woRoles :: ![NodeRole]
          , woMsgMultiplexer :: !(TSH.TSHashTable Word32 (MVar ZRPCResponse))
          , woMsgCounter :: !(MVar Word32)
          , woWriteLock :: !(MVar ())
          }

instance Show Worker where
    show (SelfWorker id _) = show ("Self", id)
    show (RemoteWorker id ip pt _ _ _ _ _) = show ("Remote", id, ip, pt)
