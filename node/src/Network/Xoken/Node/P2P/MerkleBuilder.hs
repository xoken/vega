{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.MerkleBuilder
    ( updateMerkleBranch
    , getProof
    ) where

import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import Control.Monad.STM
import Control.Monad.State.Strict
import Control.Monad.Trans.Control
import Control.Monad.Writer.Lazy
import Crypto.MAC.SipHash as SH
import qualified Data.Aeson as A (decode, encode)
import qualified Data.ByteString as B
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize as DS
import qualified Data.Text as T
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction

hashPair :: Hash256 -> Hash256 -> Hash256
hashPair a b = doubleSHA256 $ encode a `B.append` encode b

pushHash :: HashCompute -> Hash256 -> Maybe Hash256 -> Maybe Hash256 -> Int8 -> Int8 -> Bool -> HashCompute
pushHash (stateMap, res) nhash left right ht ind final =
    case node prev of
        Just pv ->
            if ind < ht
                then pushHash
                         ( (M.insert ind emptyMerkleNode stateMap)
                         , (insertSpecial
                                (Just pv)
                                (left)
                                (right)
                                True
                                (insertSpecial (Just nhash) (leftChild prev) (rightChild prev) False res)))
                         (hashPair pv nhash)
                         (Just pv)
                         (Just nhash)
                         ht
                         (ind + 1)
                         final
                else throw MerkleTreeInvalidException -- Fatal error, can only happen in case of invalid leaf nodes
        Nothing ->
            if ht == ind
                then (stateMap, (insertSpecial (Just nhash) left right True res))
                else if final
                         then pushHash
                                  (updateState, (insertSpecial (Just nhash) left right True res))
                                  (hashPair nhash nhash)
                                  (Just nhash)
                                  (Just nhash)
                                  ht
                                  (ind + 1)
                                  final
                         else (updateState, res)
  where
    insertSpecial sib lft rht flg lst = L.insert (MerkleNode sib lft rht flg) lst
    updateState = M.insert ind (MerkleNode (Just nhash) left right True) stateMap
    prev =
        case M.lookupIndex (fromIntegral ind) stateMap of
            Just i -> snd $ M.elemAt i stateMap
            Nothing -> emptyMerkleNode

-- | Add a leaf node to Merkle tree, compute and return additions to Merkle branch for leftmost
-- | leaf node, if any.
nextBranchComputeState :: BranchCompute -> Hash256 -> Int8 -> Bool -> (BranchCompute, [Hash256])
nextBranchComputeState (hcState, currRoot) txHash treeHt final =
    let (stateMap, merkleNodes) = pushHash hcState txHash Nothing Nothing treeHt 0 final
        ancestors =
            case merkleNodes of
                [] -> []
                mn ->
                    let getAncestors child merkleNodes ancestors =
                            let parent = searchParent child merkleNodes
                             in if parent == child
                                    then ancestors
                                    else getAncestors parent merkleNodes (parent : ancestors)
                          where
                            searchParent c li =
                                case L.find (\(MerkleNode _ l r _) -> (node c == l) || (node c == r)) li of
                                    Just p -> p
                                    Nothing -> c
                     in (fromJust . node) <$> getAncestors (MerkleNode (Just currRoot) Nothing Nothing True) mn []
        newRoot =
            if L.null ancestors
                then currRoot
                else head ancestors
     in (((stateMap, []), newRoot), L.reverse ancestors)

-- | Process batch of transactions to update Merkle branch for candidate block.
updateMerkleBranch :: IncrementalBranch -> [TxHash] -> IncrementalBranch
updateMerkleBranch branch [] = branch
updateMerkleBranch EmptyBranch txns =
    updateMerkleBranch (Branch [] (emptyHashCompute, getTxHash $ head txns) Nothing 0) txns
updateMerkleBranch Branch {..} txns =
    let nextBcsRunner bcState [] _ = return bcState
        nextBcsRunner bcState (t:ts) treeHeight = do
            let (bcState', branch) = nextBranchComputeState bcState t treeHeight False
             in tell branch >> nextBcsRunner bcState' ts treeHeight :: Writer [Hash256] BranchCompute
        (lastTxn', txBatch) = prepareBatch lastTxn (getTxHash <$> txns)
        txCount' = txCount + (L.length txBatch)
        treeHt = computeTreeHeight txCount'
        (bcState', branch') = runWriter $ nextBcsRunner bcState txBatch (treeHt + 1)
     in Branch (branch ++ branch') bcState' lastTxn' txCount'

-- | Get the Merkle proof for current candidate block state.
getProof :: IncrementalBranch -> ([Hash256], Maybe Hash256)
getProof EmptyBranch = ([], Nothing)
getProof (Branch _ _ Nothing _) = error "bad branch compute state"
getProof (Branch _ _ (Just t) 0) = ([], Just t)
getProof Branch {..} =
    let (_, branch') = nextBranchComputeState bcState (fromJust lastTxn) (computeTreeHeight $ txCount + 1) True
        merklePath = branch ++ branch'
     in (init merklePath, Just $ last merklePath)

-- | Defer the processing of the last transaction in the batch.
-- | Add the last transaction of the previous batch to the start of the
-- | current batch.
prepareBatch :: Maybe Hash256 -> [Hash256] -> (Maybe Hash256, [Hash256])
prepareBatch lastTxn [] = (lastTxn, [])
prepareBatch lastTxn batch =
    ( Just $ last batch
    , (if lastTxn == Nothing
           then []
           else [fromJust lastTxn]) ++
      init batch)

importTxHash :: String -> Hash256
importTxHash = getTxHash . fromJust . hexToTxHash . T.pack

-- | Computes the height of a Merkle tree.
computeTreeHeight ::
       Int -- ^ number of transactions (leaf nodes)
    -> Int8 -- ^ height of the merkle tree
computeTreeHeight ntx
    | ntx < 2 = 0
    | even ntx = 1 + computeTreeHeight (ntx `div` 2)
    | otherwise = computeTreeHeight $ ntx + 1
