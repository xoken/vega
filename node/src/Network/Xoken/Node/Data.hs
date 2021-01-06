{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}

module Network.Xoken.Node.Data where

import Codec.Compression.GZip as GZ
import Codec.Serialise
import Control.Applicative
import Data.Aeson as A
import Data.ByteString (ByteString)
import Data.ByteString.Base64.Lazy as B64L
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as C
import Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as B.Short
import Data.Char (ord)
import Data.Foldable
import Data.Hashable
import Data.Int
import Data.Maybe
import Data.Serialize as S
import Data.Store
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word
import GHC.Generics
import Prelude as P
import Text.Regex.TDFA
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

deriving instance Store BlockHash

deriving instance Store Tx

deriving instance Store TxIn

deriving instance Store OutPoint

deriving instance Store TxOut

deriving instance Store TxHash

deriving instance Store Hash256

deriving instance Store BlockHeader

deriving instance Store BlockNode

deriving instance Serialise BlockNode

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

--
--
--
encodeShort :: Serialize a => a -> ShortByteString
encodeShort = B.Short.toShort . S.encode

decodeShort :: Serialize a => ShortByteString -> a
decodeShort bs =
    case S.decode (B.Short.fromShort bs) of
        Left e -> P.error e
        Right a -> a

data RPCMessage
    = RPCRequest
          { rqMethod :: String
          , rqParams :: RPCReqParams
          }
    | RPCResponse
          { rsStatusCode :: Int16
          , rsResp :: Either RPCError (Maybe RPCResponseBody)
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data RPCError =
    RPCError
        { rsStatusMessage :: RPCErrors
        , rsErrorData :: Maybe String
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

data XRPCRequest
    = CBORRPCRequest
          { reqId :: Int
          , method :: String
          , params :: RPCReqParams
          }
    | JSONRPCRequest
          { method :: String
          , params :: RPCReqParams
          , jsonrpc :: String
          , id :: Int
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance FromJSON XRPCRequest where
    parseJSON = genericParseJSON (defaultOptions {sumEncoding = UntaggedValue})

data XRPCResponse
    = CBORRPCResponse
          { matchId :: Int
          , statusCode :: Int16
          , statusMessage :: Maybe String
          , respBody :: Maybe RPCResponseBody
          }
    | JSONRPCSuccessResponse
          { jsonrpc :: String
          , result :: Maybe RPCResponseBody
          , id :: Int
          }
    | JSONRPCErrorResponse
          { id :: Int
          , error :: ErrorResponse
          , jsonrpc :: String
          }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON XRPCResponse where
    toJSON = genericToJSON (defaultOptions {sumEncoding = UntaggedValue})

data ErrorResponse =
    ErrorResponse
        { code :: Int
        , message :: String
        , _data :: Maybe String
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON ErrorResponse where
    toJSON (ErrorResponse c m d) = object ["code" .= c, "message" .= m, "data" .= d]

data RPCReqParams
    = GetMiningCandidateRequest
          { gmcrProvideCoinbaseTx :: Maybe Bool
          }
    | SubmitMiningSolutionRequest
          { smsrId :: String
          , smsrNonce :: Int32
          , smsrCoinbase :: Maybe String
          , smsrTime :: Maybe Int32
          , smsrVersion :: Maybe Int32
          }
    deriving (Generic, Show, Hashable, Eq, Serialise, ToJSON)

instance FromJSON RPCReqParams where
    parseJSON (Object o) =
        (GetMiningCandidateRequest <$> o .:? "provide_coinbase_tx") <|>
        (SubmitMiningSolutionRequest <$> o .: "id" <*> o .: "nonce" <*> o .:? "coinbase" <*> o .:? "time" <*>
         o .:? "version")

data RPCResponseBody =
    GetMiningCandidateResp
        { rgmcId :: String
        , rgmcPrevHash :: String
        , rgmcCoinbase :: Maybe String
        , rgmcNumTx :: Int32
        , rgmcVersion :: Int32
        , rgmcCoinbaseValue :: Int64
        , rgmcnBits :: Int32
        , rgmcTime :: Int32
        , rgmcHeight :: Int32
        , rgmcMerkleProof :: [String]
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON RPCResponseBody where
    toJSON (GetMiningCandidateResp id ph cb ntx vr cv nb tm ht mp) =
        object
            [ "id" .= id
            , "prevhash" .= ph
            , "coinbase" .= cb
            , "num_tx" .= ntx
            , "version" .= vr
            , "coinbaseValue" .= cv
            , "nBits" .= nb
            , "time" .= tm
            , "height" .= ht
            , "merkleProof" .= mp
            ]

data ChainInfo =
    ChainInfo
        { ciChain :: String
        , ciChainWork :: String
        , ciHeaders :: Int32
        , ciBlocks :: Int32
        , ciBestBlockHash :: String
        , ciBestSyncedHash :: String
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON ChainInfo where
    toJSON (ChainInfo ch cw hdr blk hs shs) =
        object
            [ "chain" .= ch
            , "chainwork" .= cw
            , "chainTip" .= hdr
            , "blocksSynced" .= blk
            , "chainTipHash" .= hs
            , "syncedBlockHash" .= shs
            ]

data BlockRecord =
    BlockRecord
        { rbHeight :: Int
        , rbHash :: String
        , rbHeader :: BlockHeader'
        , rbNextBlockHash :: String
        , rbSize :: Int
        , rbTxCount :: Int
        , rbGuessedMiner :: String
        , rbCoinbaseMessage :: String
        , rbCoinbaseTx :: C.ByteString
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON BlockRecord where
    toJSON (BlockRecord ht hs hdr nbhs size ct gm cm cb) =
        object
            [ "height" .= ht
            , "hash" .= hs
            , "header" .= hdr
            , "nextBlockHash" .= nbhs
            , "size" .= size
            , "txCount" .= ct
            , "guessedMiner" .= gm
            , "coinbaseMessage" .= cm
            , "coinbaseTx" .= (T.decodeUtf8 . BL.toStrict . B64L.encode . GZ.compress $ cb)
            ]

data BlockHeader' =
    BlockHeader'
        { blockVersion' :: Word32
        , prevBlock' :: BlockHash
        , merkleRoot' :: String
        , blockTimestamp' :: Timestamp
        , blockBits' :: Word32
        , bhNonce' :: Word32
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance FromJSON BlockHeader' where
    parseJSON (Object o) =
        (BlockHeader' <$> o .: "blockVersion" <*> o .: "prevBlock" <*> o .: "merkleRoot" <*> o .: "blockTimestamp" <*>
         o .: "blockBits" <*>
         o .: "bhNonce")

instance ToJSON BlockHeader' where
    toJSON (BlockHeader' v pb mr ts bb bn) =
        object
            [ "blockVersion" .= v
            , "prevBlock" .= pb
            , "merkleRoot" .= (reverse2 mr)
            , "blockTimestamp" .= ts
            , "blockBits" .= bb
            , "nonce" .= bn
            ]

data ChainHeader =
    ChainHeader
        { blockHeight :: Int32
        , blockHash :: String
        , blockHeader :: BlockHeader'
        , txCount :: Int32
        }
    deriving (Generic, Show, Hashable, Eq, Serialise)

instance ToJSON ChainHeader where
    toJSON (ChainHeader ht hs (BlockHeader' v pb mr ts bb bn) txc) =
        object
            [ "blockHeight" .= ht
            , "blockHash" .= hs
            , "blockVersion" .= v
            , "prevBlock" .= pb
            , "merkleRoot" .= (reverse2 mr)
            , "blockTimestamp" .= ts
            , "difficulty" .= (convertBitsToDifficulty bb)
            , "nonce" .= bn
            , "txCount" .= txc
            ]

data RawTxRecord =
    RawTxRecord
        { txId :: String
        , size :: Int32
        , txBlockInfo :: BlockInfo'
        , txSerialized :: C.ByteString
        , txOutputs :: [TxOutput]
        , txInputs :: [TxInput]
        , fees :: Int64
        , txMerkleBranch :: [MerkleBranchNode']
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON RawTxRecord where
    toJSON (RawTxRecord tId sz tBI tS txo txi fee mrkl) =
        object
            [ "txId" .= tId
            , "size" .= sz
            , "txIndex" .= (binfTxIndex tBI)
            , "blockHash" .= (binfBlockHash tBI)
            , "blockHeight" .= (binfBlockHeight tBI)
            , "txSerialized" .= (T.decodeUtf8 . BL.toStrict . B64L.encode . GZ.compress $ tS)
            , "txOutputs" .= txo
            , "txInputs" .= txi
            , "fees" .= fee
            , "merkleBranch" .= mrkl
            ]

data TxRecord =
    TxRecord
        { txId :: String
        , size :: Int32
        , txBlockInfo :: BlockInfo'
        , tx :: Tx'
        , fees :: Int64
        , txMerkleBranch :: [MerkleBranchNode']
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON TxRecord where
    toJSON (TxRecord tId sz tBI tx' fee mrkl) =
        object
            [ "txId" .= tId
            , "size" .= sz
            , "txIndex" .= (binfTxIndex tBI)
            , "blockHash" .= (binfBlockHash tBI)
            , "blockHeight" .= (binfBlockHeight tBI)
            , "tx" .= tx'
            , "fees" .= fee
            , "merkleBranch" .= mrkl
            ]

data Tx' =
    Tx'
        { txVersion :: Word32
        , txOuts :: [TxOutput]
        , txInps :: [TxInput]
        , txLockTime :: Word32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxInput =
    TxInput
        { outpointTxID :: String
        , outpointIndex :: Int32
        , txInputIndex :: Int32
        , address :: String -- decode will succeed for P2PKH txn 
        , value :: Int64
        , unlockingScript :: ByteString -- scriptSig
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxOutput =
    TxOutput
        { outputIndex :: Int32
        , address :: String -- decode will succeed for P2PKH txn 
        , txSpendInfo :: Maybe SpendInfo
        , value :: Int64
        , lockingScript :: ByteString -- Script Pub Key
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxOutputSpendStatus =
    TxOutputSpendStatus
        { isSpent :: Bool
        , spendingTxID :: Maybe String
        , spendingTxBlockHt :: Maybe Int32
        , spendingTxIndex :: Maybe Int32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON TxOutputSpendStatus where
    toJSON (TxOutputSpendStatus tis stxid stxht stxindex) =
        object ["isSpent" .= tis, "spendingTxID" .= stxid, "spendingTxBlockHt" .= stxht, "spendingTxIndex" .= stxindex]

data ResultWithCursor r c =
    ResultWithCursor
        { res :: r
        , cur :: c
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

-- ordering instance for ResultWithCursor
-- imp.: note the FLIP
instance (Ord c, Eq r) => Ord (ResultWithCursor r c) where
    compare rc1 rc2 = flip compare c1 c2
      where
        c1 = cur rc1
        c2 = cur rc2

data AddressOutputs =
    AddressOutputs
        { aoAddress :: String
        , aoOutput :: OutPoint'
        , aoBlockInfo :: BlockInfo'
        , aoSpendInfo :: Maybe SpendInfo
        , aoPrevOutpoint :: [(OutPoint', Int32, Int64)]
        , aoValue :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON AddressOutputs where
    toJSON (AddressOutputs addr out bi ios po val) =
        object
            [ "address" .= addr
            , "outputTxHash" .= (opTxHash out)
            , "outputIndex" .= (opIndex out)
            , "txIndex" .= (binfTxIndex bi)
            , "blockHash" .= (binfBlockHash bi)
            , "blockHeight" .= (binfBlockHeight bi)
            , "spendInfo" .= ios
            , "prevOutpoint" .= po
            , "value" .= val
            ]

data ScriptOutputs =
    ScriptOutputs
        { scScriptHash :: String
        , scOutput :: OutPoint'
        , scBlockInfo :: BlockInfo'
        , scSpendInfo :: Maybe SpendInfo
        , scPrevOutpoint :: [(OutPoint', Int32, Int64)]
        , scValue :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON ScriptOutputs where
    toJSON (ScriptOutputs dh out bi ios po val) =
        object
            [ "scriptHash" .= dh
            , "outputTxHash" .= (opTxHash out)
            , "outputIndex" .= (opIndex out)
            , "txIndex" .= (binfTxIndex bi)
            , "blockHash" .= (binfBlockHash bi)
            , "blockHeight" .= (binfBlockHeight bi)
            , "spendInfo" .= ios
            , "prevOutpoint" .= po
            , "value" .= val
            ]

data OutPoint' =
    OutPoint'
        { opTxHash :: String
        , opIndex :: Int32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, FromJSON, ToJSON)

data BlockInfo' =
    BlockInfo'
        { binfBlockHash :: String
        , binfBlockHeight :: Int32
        , binfTxIndex :: Int32
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data MerkleBranchNode' =
    MerkleBranchNode'
        { nodeValue :: String
        , isLeftNode :: Bool
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data PubNotifyMessage =
    PubNotifyMessage
        { psBody :: ByteString
        }
    deriving (Show, Generic, Eq, Serialise)

data SpendInfo =
    SpendInfo
        { spendingTxId :: String
        , spendindTxIdx :: Int32
        , spendingBlockInfo :: BlockInfo'
        , spendInfo' :: [SpendInfo']
        }
    deriving (Show, Generic, Hashable, Eq, Serialise)

instance ToJSON SpendInfo where
    toJSON (SpendInfo stid stidx bi si) =
        object
            [ "spendingTxId" .= stid
            , "spendingTxIndex" .= stidx
            , "spendingBlockHash" .= (binfBlockHash bi)
            , "spendingBlockHeight" .= (binfBlockHeight bi)
            , "spendData" .= si
            ]

data SpendInfo' =
    SpendInfo'
        { spendingOutputIndex :: Int32
        , outputAddress :: T.Text
        , value :: Int64
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data TxOutputData =
    TxOutputData
        { txid :: T.Text
        , txind :: Int32
        , address :: T.Text
        , value :: Int64
        , blockInfo :: BlockInfo'
        , inputs :: [((T.Text, Int32), Int32, (T.Text, Int64))]
        , spendInfo :: Maybe SpendInfo
        }
    deriving (Show, Generic, Eq, Serialise)

-- Internal message posting --
data XDataReq
    = XDataRPCReq
          { reqId :: Int
          , method :: String
          , params :: RPCReqParams
          , version :: Maybe String
          }
    | XDataRPCBadRequest
    | XCloseConnection
    deriving (Show, Generic, Hashable, Eq, Serialise)

data XDataResp =
    XDataRPCResp
        { matchId :: Int
        , statusCode :: Int16
        , statusMessage :: Maybe String
        , respBody :: Maybe RPCResponseBody
        }
    deriving (Show, Generic, Hashable, Eq, Serialise, ToJSON)

data RPCErrors
    = INVALID_METHOD
    | PARSE_ERROR
    | INVALID_PARAMS
    | INTERNAL_ERROR
    | SERVER_ERROR
    | INVALID_REQUEST
    deriving (Generic, Hashable, Eq, Serialise)

instance Show RPCErrors where
    show e =
        case e of
            INVALID_METHOD -> "Error: Invalid method"
            PARSE_ERROR -> "Error: Parse error"
            INVALID_PARAMS -> "Error: Invalid params"
            INTERNAL_ERROR -> "Error: RPC error occurred"
            SERVER_ERROR -> "Error: Something went wrong"
            INVALID_REQUEST -> "Error: Invalid request"

--getOptPointKey :: TxShortHash -> Int16 -> B.ByteString
--getOptPointKey txh ind = C.toStrict . C.pack . show $ txh + (fromIntegral ind) 
-- can be replaced with Enum instance but in future other RPC methods might be handled then we might have to give different codes
getJsonRPCErrorCode :: RPCErrors -> Int
getJsonRPCErrorCode err =
    case err of
        SERVER_ERROR -> -32000
        INVALID_REQUEST -> -32600
        INVALID_METHOD -> -32601
        INVALID_PARAMS -> -32602
        INTERNAL_ERROR -> -32603
        PARSE_ERROR -> -32700

coinbaseTxToMessage :: C.ByteString -> String
coinbaseTxToMessage s =
    case C.length (C.pack regex) > 6 of
        True ->
            let sig = C.drop 4 $ C.pack regex
                sigLen = fromIntegral . ord . C.head $ sig
                htLen = fromIntegral . ord . C.head . C.tail $ sig
             in C.unpack . C.take (sigLen - htLen - 1) . C.drop (htLen + 2) $ sig
        False -> "False"
  where
    r :: String
    r = "\255\255\255\255[\NUL-\255]+"
    regex = ((C.unpack s) =~ r) :: String

validateEmail :: String -> Bool
validateEmail email =
    let emailRegex = "^[a-zA-Z0-9+._-]+@[a-zA-Z-]+\\.[a-z]+$" :: String
     in (email =~ emailRegex :: Bool) || (null email)

mergeTxInTxInput :: TxIn -> TxInput -> TxInput
mergeTxInTxInput (TxIn {..}) txInput = txInput {unlockingScript = scriptInput}

mergeTxOutTxOutput :: TxOut -> TxOutput -> TxOutput
mergeTxOutTxOutput (TxOut {..}) txOutput = txOutput {lockingScript = scriptOutput}

mergeAddrTxInTxInput :: String -> TxIn -> TxInput -> TxInput
mergeAddrTxInTxInput addr (TxIn {..}) txInput = txInput {unlockingScript = scriptInput, address = addr}

mergeAddrTxOutTxOutput :: String -> TxOut -> TxOutput -> TxOutput
mergeAddrTxOutTxOutput addr (TxOut {..}) txOutput = txOutput {lockingScript = scriptOutput, address = addr}

txToTx' :: Tx -> [TxOutput] -> [TxInput] -> Tx'
txToTx' (Tx {..}) txout txin = Tx' txVersion txout txin txLockTime

{-
type TxIdOutputs = ((T.Text, Int32, Int32), Bool, Set ((T.Text, Int32), Int32, (T.Text, Int64)), Int64, T.Text)


genTxOutputData :: (T.Text, Int32, TxIdOutputs, Maybe TxIdOutputs) -> TxOutputData
genTxOutputData (txId, txIndex, ((hs, ht, ind), _, inps, val, addr), Nothing) =
    TxOutputData txId txIndex addr val (BlockInfo' (T.unpack hs) ht ind) (Q.fromSet inps) Nothing
genTxOutputData (txId, txIndex, ((hs, ht, ind), _, inps, val, addr), Just ((shs, sht, sind), _, oth, _, _)) =
    let other = Q.fromSet oth
        ((stid, _), stidx, _) = head $ other
        si = (\((_, soi), _, (ad, vl)) -> SpendInfo' soi ad vl) <$> other
     in TxOutputData
            txId
            txIndex
            addr
            val
            (BlockInfo' (T.unpack hs) ht ind)
            (Q.fromSet inps)
            (Just $ SpendInfo (T.unpack stid) stidx (BlockInfo' (T.unpack shs) sht sind) si)
            
txOutputDataToOutput :: TxOutputData -> TxOutput
txOutputDataToOutput (TxOutputData {..}) = TxOutput txind (T.unpack address) spendInfo value ""

-}
reverse2 :: String -> String
reverse2 (x:y:xs) = reverse2 xs ++ [x, y]
reverse2 x = x
