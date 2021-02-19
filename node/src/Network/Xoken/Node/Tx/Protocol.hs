{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

module Network.Xoken.Node.Tx.Protocol where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently, race_)
import Control.Concurrent.Async.Lifted as LA (async)
import Control.Concurrent.Event as EV
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Logger
import Control.Monad.Reader
import Control.Monad.STM
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.ByteString.Short as BSS
import Data.Function ((&))
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.Int
import qualified Data.IntMap as I
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Sequence ((|>))
import Data.Serialize
import Data.Serialize as S
import Data.String.Conversions
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Time.LocalTime
import Data.Word
import qualified Database.RocksDB as R
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as SB (recv)
import qualified Network.Socket.ByteString.Lazy as LB (recv, sendAll)
import Network.Xoken.Address
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Crypto.Hash
import Network.Xoken.Network.Common
import Network.Xoken.Network.Message
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph as DAG
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.P2P.Sync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.MerkleBuilder
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Worker.Dispatcher
import Network.Xoken.Script.Standard
import Network.Xoken.Transaction.Common
import Network.Xoken.Util
import StmContainers.Map as SM
import Streamly
import Streamly.Prelude ((|:), nil)
import qualified Streamly.Prelude as S
import System.Logger as LG
import System.Logger.Message
import System.Random
import Xoken.NodeConfig
{-

    --Make sure none of the inputs have hash=0, n=-1 (coinbase transactions)
    --Reject if we already have matching tx in the pool, or in a block in the main branch (we don't have a pool; do we have to check in db also?)
    For each input, if the referenced output exists in any other tx in the pool, reject this transaction.[5]
    For each input, look in the main branch and the transaction pool to find the referenced output transaction. If the output transaction is missing for any input, this will be an orphan transaction. Add to the orphan transactions, if a matching transaction is not in there already.
    For each input, if the referenced output does not exist (e.g. never existed or has already been spent), reject this transaction[6]
    
    
    -Verify the scriptPubKey accepts for each input; reject if any are bad
    -For each orphan transaction that uses this one as one of its inputs, run all these steps (including this one) recursively on that orphan

-}

-- Make sure neither in or out lists are empty
areInOutNonEmpty :: Tx -> Bool
areInOutNonEmpty tx = not $ (L.null $ txIn tx) || (L.null $ txOut tx)

-- size in bytes >= 100 AND size in bytes <= MAX_BLOCK_SIZE
isValidSize :: Tx -> Int -> Bool
isValidSize tx maxsize = let txsize = B.length $ runPut $ put tx
                         in txsize >= 100 && txsize <= maxsize

-- Reject if the sum of input values < sum of output values (negatve fees)
-- Reject if transaction fee (defined as sum of input values minus sum of output values) would be too low to get into an empty block
-- Using the referenced output transactions to get input values, check that each input value, as well as the sum, are in legal money range
areValuesLegal :: [Word64] -> [Word64] -> Word64 -> Bool
areValuesLegal invals outvals minfees = let ipsum = sum invals
                                            opsum = sum outvals
                                        in ipsum - opsum >= minfees && all (<=2100000000000000) ([ipsum,opsum] ++ invals ++ outvals)

inputsNotCoinbase :: Tx -> Bool
inputsNotCoinbase tx = all ((/= maxBound) . outPointIndex . prevOutput) (txIn tx)

isCoinbase :: Tx -> Bool
isCoinbase tx = case txIn tx of
                    [x] -> ((== nullOutPoint) . prevOutput) x
                    otherwise -> False

-- preprocessing

validateTxWith :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> [PreprocessedTx -> Bool] -> m Bool
validateTxWith tx f = do
    ptx <- preprocessTx tx
    return $ all (\x -> x ptx) f

data PreprocessedTx = PreprocessedTx {
    pTx :: Tx
   ,pTxInputs :: [(TxIn,Word32)]
   ,pTxOutputs :: [(TxOut,Word32)]
   ,pTxOutpoints :: [OutPoint]
   ,pTxInputValsOutpoints :: [(Word64,(TxShortHash,[BlockHash],TxHash,Word32))]
   ,pTxOpCount :: Word32
   ,pTxIpSum :: Word64
   ,pTxOutSum :: Word64
   ,pTxParentTxns :: [TxHash]
  } deriving (Show,Eq)

preprocessTx :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Tx -> m (PreprocessedTx)
preprocessTx tx = do
    lg <- getLogger
    bp <- getBitcoinP2P
    let net = bitcoinNetwork $ nodeConfig bp
    let inputs = zip (txIn tx) [0 :: Word32 ..]
    let outputs = zip (txOut tx) [0 :: Word32 ..]
    let outpoints =
            map (\(b, _) -> ((outPointHash $ prevOutput b), fromIntegral $ outPointIndex $ prevOutput b)) (inputs)
    let opCount = fromIntegral $ L.length outputs
    bbn <- fetchBestBlock
    let (bsh,bht) = (headerHash $ nodeHeader bbn, nodeHeight bbn)
    inputValsOutpoints <-
        mapM
            (\(b, indx) -> do
                 let opHash = outPointHash $ prevOutput b
                     shortHash = getTxShortHash opHash 20
                 let opindx = fromIntegral $ outPointIndex $ prevOutput b
                 if (outPointHash nullOutPoint) == opHash
                     then do
                         let sval = fromIntegral $ computeSubsidy net $ (fromIntegral bht :: Word32)
                         return (sval, (shortHash, [], opHash, opindx))
                     else do
                         debug lg $
                             LG.msg
                                 ("[dag] processUnconfTransaction: inputValsOutpoint (inputs): " ++ (show $ (b, indx)))
                         zz <- LE.try $ zRPCDispatchGetOutpoint (prevOutput b) (Nothing)
                         case zz of
                             Right (val, bsh, ht) -> do
                                 debug lg $ LG.msg $ "[dag] processUnconfTransaction: zz: " ++ (show $ zz)
                                 return (val, (shortHash, bsh, opHash, opindx))
                             Left (e :: SomeException) -> do
                                 err lg $
                                     LG.msg $
                                     "Error: [pCT calling gSVFO] WHILE Processing Unconf TxID " ++
                                     show (txHashToHex $ txHash tx) ++
                                     ", getting value for dependent input (TxID,Index): (" ++
                                     show (txHashToHex $ outPointHash (prevOutput b)) ++
                                     ", " ++ show (outPointIndex $ prevOutput b) ++ ")" ++ (show e)
                                 throw e)
            inputs
    let inpv = (\(val, _) -> val) <$> inputValsOutpoints
        outv = (\o -> outValue o) <$> (txOut tx)
        ipSum = foldl (+) 0 $ inpv
        opSum = foldl (+) 0 $ outv
    let parentTxns =
            catMaybes $
            map
                (\(_, (_, bsh, ophs, _)) ->
                     case bsh of
                         [] -> Just ophs
                         _ -> Nothing)
                inputValsOutpoints
    let outpts = map (\(tid, idx) -> OutPoint tid idx) outpoints
    return $ PreprocessedTx tx inputs outputs outpts inputValsOutpoints opCount ipSum opSum (parentTxns)
