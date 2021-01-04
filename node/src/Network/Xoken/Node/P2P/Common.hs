{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.Common where

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import qualified Data.Aeson as A (decode, eitherDecode, encode)
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Base64 as B64
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Pool
import Data.Serialize
import Data.Serialize as S
import Data.Store as DS
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Word
import qualified Database.RocksDB as R
import Network.Socket
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address.Base58 as B58
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common -- (GetData(..), MessageCommand(..), NetworkAddress(..))
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Random
import Text.Format

data BlockSyncException
    = BlocksNotChainedException
    | MessageParsingException
    | KeyValueDBInsertException
    | BlockHashNotFoundException
    | DuplicateBlockHeaderException
    | InvalidMessageTypeException
    | InvalidBlocksException
    | EmptyHeadersMessageException
    | InvalidStreamStateException
    | InvalidBlockIngestStateException
    | InvalidMetaDataException
    | InvalidBlockHashException
    | UnexpectedDuringBlockProcException String
    | InvalidBlockSyncStatusMapException
    | InvalidBlockInfoException
    | OutpointAddressNotFoundException
    | InvalidAddressException
    | TxIDNotFoundException
    | InvalidOutpointException
    | DBTxParseException
    | MerkleTreeComputeException
    | InvalidCreateListException
    | MerkleSubTreeAlreadyExistsException
    | MerkleSubTreeDBInsertException
    | ResourcePoolFetchException
    | DBInsertTimeOutException
    | MerkleTreeInvalidException
    | MerkleQueueNotFoundException
    | BlockAlreadySyncedException
    | OutputAlreadySpentException
    | InvalidTxSatsValueException
    | InvalidDAGEdgeException
    | ZUnknownHandler
    | ZInvalidColumnFamily
    deriving (Show, Read)

instance Exception BlockSyncException

data PeerMessageException
    = SocketReadException
    | SocketConnectException SockAddr
    | DeflatedBlockParseException
    | ConfirmedTxParseException
    | PeerSocketNotConnectedException
    | ZeroLengthSocketReadException
    | NetworkMagicMismatchException
    | UnresponsivePeerException
    deriving (Show)

instance Exception PeerMessageException

data AriviServiceException
    = KeyValueDBLookupException
    | GraphDBLookupException
    | InvalidOutputAddressException
    deriving (Show)

instance Exception AriviServiceException

data WorkerRemoteException =
    WorkerConnectionRejectException
    deriving (Show, Read)

instance Exception WorkerRemoteException

--
--
-- | Create version data structure.
buildVersion :: Network -> Word64 -> BlockHeight -> NetworkAddress -> NetworkAddress -> Word64 -> Version
buildVersion net nonce height loc rmt time =
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
        }

-- | Our protocol version.
myVersion :: Word32
myVersion = 70015

msgOrder :: Message -> Message -> Ordering
msgOrder m1 m2 = do
    if msgType m1 == MCGetHeaders
        then LT
        else GT

sendEncMessage :: MVar () -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = withMVar writeLock (\x -> (LB.sendAll sock msg))

-- | Computes the height of a Merkle tree.
computeTreeHeight ::
       Int -- ^ number of transactions (leaf nodes)
    -> Int8 -- ^ height of the merkle tree
computeTreeHeight ntx
    | ntx < 2 = 0
    | even ntx = 1 + computeTreeHeight (ntx `div` 2)
    | otherwise = computeTreeHeight $ ntx + 1

getTextVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe T.Text
getTextVal (_, _, _, txt) = txt

getBoolVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe Bool
getBoolVal (b, _, _, _) = b

getIntVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe Int32
getIntVal (_, i, _, _) = i

getBigIntVal :: (Maybe Bool, Maybe Int32, Maybe Int64, Maybe T.Text) -> Maybe Int64
getBigIntVal (_, _, ts, _) = ts

divide :: Int -> Int -> Float
divide x y = (a / b)
  where
    a = fromIntegral x :: Float
    b = fromIntegral y :: Float

toInt :: Float -> Int
toInt x = round x

-- OP_RETURN Allegory/AllPay
frameOpReturn :: C.ByteString -> C.ByteString
frameOpReturn opReturn = do
    let prefix = (fst . B16.decode) "006a0f416c6c65676f72792f416c6c506179"
    let len = B.length opReturn
    let xx =
            if (len <= 0x4b)
                then word8 $ fromIntegral len
                else if (len <= 0xff)
                         then mappend (word8 0x4c) (word8 $ fromIntegral len)
                         else if (len <= 0xffff)
                                  then mappend (word8 0x4d) (word16LE $ fromIntegral len)
                                  else if (len <= 0x7fffffff)
                                           then mappend (word8 0x4e) (word32LE $ fromIntegral len)
                                           else word8 0x99 -- error scenario!!
    let bs = LC.toStrict $ toLazyByteString xx
    C.append (C.append prefix bs) opReturn

generateSessionKey :: IO (Text)
generateSessionKey = do
    g <- liftIO $ newStdGen
    let seed = show $ fst (random g :: (Word64, StdGen))
        sdb = B64.encode $ C.pack $ seed
    return $ encodeHex ((S.encode $ sha256 $ B.reverse sdb))

maskAfter :: Int -> String -> String
maskAfter n skey = (\x -> take n x ++ fmap (const '*') (drop n x)) skey

stripScriptHash :: ((Text, Int32), Int32, (Text, Text, Int64)) -> ((Text, Int32), Int32, (Text, Int64))
stripScriptHash (op, ii, (addr, scriptHash, satValue)) = (op, ii, (addr, satValue))

fromBytes :: B.ByteString -> Integer
fromBytes = B.foldl' f 0
  where
    f a b = a `shiftL` 8 .|. fromIntegral b

splitList :: [a] -> ([a], [a])
splitList xs = (f 1 xs, f 0 xs)
  where
    f n a = map fst . filter (odd . snd) . zip a $ [n ..]

fetchBestBlock :: (HasXokenNodeEnv env m, MonadIO m) => m (BlockNode)
fetchBestBlock = do
    bp2pEnv <- getBitcoinP2P
    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
    return $ memoryBestHeader hm
    {-
    case (hash, height) of
        (Just hs, Just ht)
            -- liftIO $
            --     print $
            --     "FETCHED BEST BLOCK FROM DB: " ++
            --     (T.unpack $ DTE.decodeUtf8 hs) ++ " " ++ (T.unpack . DTE.decodeUtf8 $ ht)
         -> do
            case hexToBlockHash $ DTE.decodeUtf8 hs of
                Nothing -> throw InvalidBlockHashException
                Just hs' -> return (hs', read . T.unpack . DTE.decodeUtf8 $ ht :: Int32)
        _ -> do
            debug lg $ LG.msg $ val "Bestblock is genesis."
            return ((headerHash $ getGenesisHeader net), 0)
    -}

-- getTxShortCode :: TxHash -> Word8
-- getTxShortCode txh = sum $ map (\(x, p) -> (fromIntegral x) * (16 ^ p)) list
--   where
--     list = zip (BSS.unpack $ getTxShortHash txh 2) [0 ..] -- 0xff = 255
-- getTxMidCode :: TxHash -> Word32
-- getTxMidCode txh = sum $ map (\(x, p) -> (fromIntegral x) * (16 ^ p)) list
--   where
--     list = zip (BSS.unpack $ getTxShortHash txh 8) [0 ..] -- 0xffffffff = 4294967295
putDB :: (Store a, Store b, MonadIO m) => R.DB -> a -> b -> m ()
putDB rkdb k v = R.put rkdb (DS.encode k) (DS.encode v)

putDBCF :: (Store a, Store b, MonadIO m) => R.DB -> R.ColumnFamily -> a -> b -> m ()
putDBCF rkdb cf k v = R.putCF rkdb cf (DS.encode k) (DS.encode v)

deleteDBCF :: (Store a, MonadIO m) => R.DB -> R.ColumnFamily -> a -> m ()
deleteDBCF rkdb cf k = R.deleteCF rkdb cf (DS.encode k)

getDB' :: (Store a, Store b, MonadIO m) => R.DB -> a -> m (Maybe b)
getDB' rkdb k = do
    res <- R.get rkdb (DS.encode k)
    case DS.decode <$> res of
        Just (Left e) -> do
            liftIO $ print $ "getDB' ERROR: " ++ show e
            throw KeyValueDBLookupException
        Just (Right m) -> return $ Just m
        Nothing -> return Nothing

getDBCF :: (Store a, Store b, MonadIO m) => R.DB -> R.ColumnFamily -> a -> m (Maybe b)
getDBCF rkdb cf k = do
    res <- R.getCF rkdb cf (DS.encode k)
    case DS.decode <$> res of
        Just (Left e) -> do
            liftIO $ print $ "getDBCF ERROR" ++ show e
            throw KeyValueDBLookupException
        Just (Right m) -> return $ Just m
        Nothing -> return Nothing


getDBCF_ :: (Store a, Store b) => R.DB -> R.ColumnFamily -> a -> IO (Maybe b)
getDBCF_ rkdb cf k = do
    res <- R.getCF rkdb cf (DS.encode k)
    case DS.decode <$> res of
        Just (Left e) -> do
            print $ "getDBCF_ ERROR" ++ show e
            throw KeyValueDBLookupException
        Just (Right m) -> return $ Just m
        Nothing -> return Nothing

getEpochTxCF :: Bool -> String
getEpochTxCF True = "ep_transactions_odd"
getEpochTxCF False = "ep_transactions_even"

getEpochTxOutCF :: Bool -> String
getEpochTxOutCF True = "ep_outputs_odd"
getEpochTxOutCF False = "ep_outputs_even"

-- Helper Functions
recvAll :: (MonadIO m) => Socket -> Int64 -> m BSL.ByteString
recvAll sock len = do
    if len > 0
        then do
            res <- liftIO $ try $ LB.recv sock len
            case res of
                Left (e :: IOException) -> throw SocketReadException
                Right mesg ->
                    if BSL.length mesg == len
                        then return mesg
                        else if BSL.length mesg == 0
                                 then throw ZeroLengthSocketReadException
                                 else BSL.append mesg <$> recvAll sock (len - BSL.length mesg)
        else return (BSL.empty)

receiveMessage :: (MonadIO m) => Socket -> m BSL.ByteString
receiveMessage sock = do
    lp <- recvAll sock 4
    case runGetLazy getWord32le lp of
        Right x -> do
            payload <- recvAll sock (fromIntegral x)
            return payload
        Left e -> Prelude.error e

sendMessage :: (MonadIO m) => Socket -> MVar () -> BSL.ByteString -> m ()
sendMessage sock writeLock payload = do
    let len = LC.length payload
        prefix = toLazyByteString $ (word32LE $ fromIntegral len)
    liftIO $
        withMVar
            writeLock
            (\x -> do
                 LB.sendAll sock prefix
                 LB.sendAll sock payload)

mkProvisionalBlockHash :: BlockHash -> BlockHash
mkProvisionalBlockHash b = let bh = S.runGet S.get $ B.append (B.take 16 $ S.runPut $ S.put b) "\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255"  :: Either String BlockHash
                           in either (const b) (Prelude.id) bh

isProvisionalBlockHash :: BlockHash -> Bool
isProvisionalBlockHash = (== "\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255") . (B.drop 16 . S.runPut . S.put)

replaceProvisionals :: BlockHash -> [BlockHash] -> [BlockHash]
replaceProvisionals bh [] = [bh]
replaceProvisionals bh (pbh:bhs) | isProvisionalBlockHash pbh = (bh:filter (not . isProvisionalBlockHash) bhs)
                                 | otherwise = pbh:(replaceProvisionals bh bhs)