{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Network.Xoken.Node.Worker.Types where

import Codec.Serialise
import Data.ByteString (ByteString)
import Data.Hashable
import Data.Int
import Data.Maybe
import Data.Word
import GHC.Generics
import Network.Xoken.Node.Data
import Prelude as P
import Xoken as H
import Xoken.NodeConfig

data ZRPCRequest =
    ZRPCRequest
        { zrqId :: !Word32
        , zrqParams :: !ZRPCRequestParam
        }
    deriving (Show, Generic, Eq, Serialise)

data ZRPCResponse =
    ZRPCResponse
        { zrsMatchId :: !Word32
        , zrsPayload :: !(Either ZRPCError (Maybe ZRPCResponseBody))
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

--
-- Legend: (M = Master), (C = Compute)
--
data ZRPCRequestParam
    = ZValidateTx -- M =>> C
          { vtBlockHash :: !BlockHash
          , vtBlockHeight :: !Word32
          , vtTxIndex :: !Word32
          , vtTxSerialized :: !Tx
          }
    | ZPutOutpoint -- C =>> C | C =>> M  | M =>> C 
          { poTxID :: !TxHash
          , poIndex :: !Word32
          , poScript :: !ByteString
          , poValue :: !Word64
          }
    | ZGetOutpoint -- C =>> C | C =>> M  | M =>> C 
          { goTxID :: !TxHash
          , goIndex :: !Word32
          , goBlockHash :: !(Maybe BlockHash)
          --, goPredecessors :: !(DS.Set BlockHash)
          }
    | ZUpdateOutpoint -- C =>> C | C =>> M  | M =>> C 
          { uoTxID :: !TxHash
          , uoIndex :: !Word32
          , uoBlockHash :: !BlockHash
          , uoBlockHeight :: !Word32
          }
    | ZUnspendOutpoint -- C =>> C | C =>> M |  M =>> C 
          { goTxID :: !TxHash
          , goIndex :: !Word32
          }
    | ZTraceOutputs
          { toTxID :: !TxHash
          , toIndex :: !Word32
          , toBlockHash :: !BlockHash
          , toIsPrevFresh :: !Bool
          , htt :: !Int
          }
    | ZGetBlockHeaders -- C =>> M
          { gbBlockHash :: !BlockHash
          }
    | ZNotifyNewBlockHeader -- M =>> C
          { znBlockHeaders :: ![ZBlockHeader]
          , znBlockNode :: !BlockNode
          }
    | ZPruneBlockTxOutputs
          { prBlockHashes :: ![BlockHash]
          }
    | ZValidateUnconfirmedTx -- M =>> C
          { vtTxSerialized :: !Tx
          }
    | ZInvite -- M =>> C | C =>> M | C =>> C
          { cluster :: ![Node]
          , clusterID :: !ByteString
          }
    | ZPing -- M =>> C | C =>> M | C =>> C
    | ZProvisionalBlockHash
          { zpBlockHash :: !BlockHash
          , zpProvisionalBlockHash :: !BlockHash
          }
    deriving (Show, Generic, Eq, Serialise)

data ZBlockHeader =
    ZBlockHeader
        { zBlockHeader :: !BlockHeader
        , zBlockHeight :: !BlockHeight
        }
    deriving (Show, Generic, Eq, Serialise)

data ZRPCError =
    ZRPCError
        { zrsStatusMessage :: !ZRPCErrors
        , zrsErrorData :: !(Maybe String)
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data ZRPCErrors
    = Z_INVALID_METHOD
    | Z_PARSE_ERROR
    | Z_INVALID_PARAMS
    | Z_INTERNAL_ERROR
    | Z_SERVER_ERROR
    | Z_INVALID_REQUEST
    deriving (Show, Generic, Hashable, Eq, Serialise)

data ZRPCResponseBody
    = ZValidateTxResp
          { isValid :: !Bool
          }
    | ZPutOutpointResp
          {
          }
    | ZGetOutpointResp
          { zOutValue :: !Word64
          , zScriptOutput :: !ByteString
          , zBlockHash :: ![BlockHash]
          , zBlockHeight :: !Word32
          }
    | ZUpdateOutpointResp
          { zOutpointUpdated :: !Word32
          }
    | ZUnspendOutpointResp
          {
          }
    | ZTraceOutputsResp
          { ztIsRoot :: !Bool
          }
    | ZGetBlockHeadersResp
          { blockHeaders :: ![(BlockHash, Int32)]
          }
    | ZNotifyNewBlockHeaderResp
          {
          }
    | ZPruneBlockTxOutputsResp
    | ZValidateUnconfirmedTxResp
          { utDependentTransaction :: ![TxHash] -- utCandidateParentBlock :: ![BlockHash]
          }
    | ZOk
    | ZPong
    | ZProvisionalBlockHashResp
          {
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)
