{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Xoken.Node.P2P.Common where

import Control.Concurrent.MVar
import Control.Concurrent.Async (AsyncCancelled)
import Control.Exception
import Control.Concurrent.STM.TVar
import Control.Monad.Reader
import Data.Bits
import qualified Data.ByteString as B
import Data.ByteString.Builder
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.Int
import Data.Maybe
import Data.Serialize
import Data.Serialize as S
import Data.Word
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Block.Common
import Network.Xoken.Node.Exception
import Network.Xoken.Node.DB
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.P2P.MerkleBuilder (updateMerkleBranch)
import Network.Xoken.Util
import System.Random
import qualified System.Logger as LG
import System.Logger (debug, err, val)
import Network.Xoken.Script
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Block.Headers
import Network.Xoken.Network.Common
import Network.Xoken.Network.CompactBlock
import Network.Xoken.Network.Message
import Network.Xoken.Transaction.Common
import Network.Xoken.Transaction
import Xoken.NodeConfig

sendEncMessage :: MVar () -> Socket -> BSL.ByteString -> IO ()
sendEncMessage writeLock sock msg = withMVar writeLock (\_ -> LB.sendAll sock msg)

{- UNUSED?
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

splitList :: [a] -> ([a], [a])
splitList xs = (f 1 xs, f 0 xs)
  where
    f n a = map fst . filter (odd . snd) . zip a $ [n ..]
-}
fromBytes :: B.ByteString -> Integer
fromBytes = B.foldl' f 0
  where
    f a b = a `shiftL` 8 .|. fromIntegral b

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
            (\_ -> do
                 LB.sendAll sock prefix
                 LB.sendAll sock payload)

mkProvisionalBlockHashR :: BlockHash -> IO BlockHash
mkProvisionalBlockHashR b = do
    r64 <- randomRIO (minBound, maxBound :: Word64)
    let bh =
            S.runGet S.get $
            B.append (B.take 16 $ S.runPut $ S.put b) (B.append (S.encode r64) "\255\255\255\255\255\255\255\255") :: Either String BlockHash
    return $ either (const b) (Prelude.id) bh

isProvisionalBlockHashR :: BlockHash -> Bool
isProvisionalBlockHashR = (== "\255\255\255\255\255\255\255\255") . (B.drop 24 . S.runPut . S.put)

mkProvisionalBlockHash :: BlockHash -> BlockHash
mkProvisionalBlockHash b =
    let bh =
            S.runGet S.get $
            B.append (B.take 16 $ S.runPut $ S.put b) "\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255" :: Either String BlockHash
     in either (const b) (Prelude.id) bh

isProvisionalBlockHash :: BlockHash -> Bool
isProvisionalBlockHash =
    (== "\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255\255") . (B.drop 16 . S.runPut . S.put)

replaceProvisionals :: BlockHash -> [BlockHash] -> [BlockHash]
replaceProvisionals bh [] = [bh]
replaceProvisionals bh (pbh:bhs)
    | isProvisionalBlockHash pbh = (bh : filter (not . isProvisionalBlockHash) bhs)
    | otherwise = pbh : (replaceProvisionals bh bhs)

error_msg :: (Env, Maybe InterpreterError) -> Maybe ErrorMsg
error_msg (env, error) = go <$> error where
  go x = ErrorMsg { name = x, position, extra = extra x }
  position = take 4 (ops_left env)
  extra SigNullFail    = [hex_stack]
  extra StackUnderflow = [hex_stack]
  extra CleanStack     = [hex_stack]
  extra SigHighS       = [hex_stack]
  extra _              = []
  hex_stack = Stack $ HexElem <$> stack env

applyTillJust :: IO (Maybe a) -> IO a
applyTillJust f = do
    res <- f
    case res of
        Just r -> return r
        Nothing -> applyTillJust f

data ErrorMsg = ErrorMsg
  { name     :: InterpreterError
  , position :: [ScriptOp]
  , extra    :: [ErrorMsgExtra]
  }
  deriving (Eq, Show)

newtype HexElem = HexElem { getElem :: Elem }
  deriving Eq

instance Show HexElem where
  show = show . encodeHex . getElem

data ErrorMsgExtra
  = Stack (Stack HexElem)
  | AltStack (Stack HexElem)
  deriving (Eq, Show)

broadcastToPeers :: (HasXokenNodeEnv env m, MonadIO m) => Message -> m ()
broadcastToPeers msg = do
    liftIO $ putStrLn $ "Broadcasting " ++ show (msgType msg) ++ " to peers"
    bp2pEnv <- getBitcoinP2P
    peerMap <- liftIO $ readTVarIO (bitcoinPeers bp2pEnv)
    mapM_
        (\bp ->
             if bpConnected bp
                 then sendRequestMessages bp msg
                 else return ())
        peerMap
    liftIO $ putStrLn $ "Broadcasted " ++ show (msgType msg) ++ " to peers"

sendcmpt :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> m ()
sendcmpt bp = sendRequestMessages bp $ MSendCompact $ SendCompact 0 1

sendCmptBlock :: (HasXokenNodeEnv env m, MonadIO m) => CompactBlock -> BitcoinPeer -> m ()
sendCmptBlock cmpt bp = sendRequestMessages bp $ MCompactBlock cmpt

sendBlockTxn :: (HasXokenNodeEnv env m, MonadIO m) => BlockTxns -> BitcoinPeer -> m ()
sendBlockTxn blktxn bp = sendRequestMessages bp $ MBlockTxns blktxn

sendInv :: (HasXokenNodeEnv env m, MonadIO m) => Inv -> BitcoinPeer -> m ()
sendInv inv bp = sendRequestMessages bp $ MInv inv

defTxHash = fromJust $ hexToTxHash "0000000000000000000000000000000000000000000000000000000000000000"

newCandidateBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => BlockHash -> BlockHeight -> m ()
newCandidateBlock hash height = do
    bp2pEnv <- getBitcoinP2P
    let cbase = makeCoinbaseTx
                    (1 + fromIntegral height)
                    (coinbaseAddress bp2pEnv)
                    (computeSubsidy (bitcoinNetwork $ nodeConfig bp2pEnv) (fromIntegral $ height))
    tsdag <- liftIO $ DAG.new defTxHash (0 :: Word64) EmptyBranch 16 16
    liftIO $ DAG.coalesce tsdag (txHash cbase) [] 0 (+) updateMerkleBranch
    liftIO $ TSH.insert (candidateBlocks bp2pEnv) hash (tsdag,cbase)

newCandidateBlockChainTip :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
newCandidateBlockChainTip = do
    bp2pEnv <- getBitcoinP2P
    bbn <- fetchBestBlock
    let hash = headerHash $ nodeHeader bbn
        height = nodeHeight bbn
    newCandidateBlock hash height
    
sendRequestMessages :: (HasXokenNodeEnv env m, MonadIO m) => BitcoinPeer -> Message -> m ()
sendRequestMessages pr msg = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    debug lg $ LG.msg $ val "Block - sendRequestMessages - called."
    case (bpSocket pr) of
        Just s -> do
            let em = runPut . putMessage net $ msg
            res <- liftIO $ try $ sendEncMessage (bpWriteMsgLock pr) s (BSL.fromStrict em)
            case res of
                Right () -> return ()
                Left (e :: SomeException) -> do
                    case fromException e of
                        Just (t :: AsyncCancelled) -> throw e
                        otherwise -> debug lg $ LG.msg $ "Error, sending out data: " ++ show e
            debug lg $ LG.msg $ "sending out GetData: " ++ show (bpAddress pr)
        Nothing -> err lg $ LG.msg $ val "Error sending, no connections available"