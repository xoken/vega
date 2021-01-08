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
    ( computeMerkleBranch
    , nextBcState
    , importTxHash
    ) where

import Control.Exception
import qualified Data.ByteString as B
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Serialize as DS
import qualified Data.Text as T
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.Exception
import Network.Xoken.Node.P2P.Common
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

nextBcState :: BranchComputeState -> [TxHash] -> BranchComputeState
nextBcState bcState [] = bcState
nextBcState bcState txHashes =
    let (hashesToProcess, finalHash) =
            ( case lastTxn bcState of
                  Just l -> l : (L.init txHashes)
                  Nothing -> L.init txHashes
            , Just $ L.last txHashes)
        newTxCount = (txCount bcState) + (fromIntegral $ L.length hashesToProcess)
        treeHeight = computeTreeHeight $ fromIntegral newTxCount
        runPush hashComp [] = hashComp
        runPush hashComp (h:hs) = runPush (pushHash hashComp (getTxHash h) Nothing Nothing treeHeight 0 False) hs
     in BranchComputeState (runPush (hashCompute bcState) hashesToProcess) newTxCount finalHash

computeMerkleBranch :: BranchComputeState -> TxHash -> ([TxHash], Maybe TxHash)
computeMerkleBranch (BranchComputeState hcState _ Nothing) _ = ([], Nothing)
computeMerkleBranch (BranchComputeState hcState txCount (Just finalTxHash)) coinbaseTxHash = do
    let finalHcState =
            pushHash
                hcState
                (getTxHash finalTxHash)
                Nothing
                Nothing
                (computeTreeHeight $ (fromIntegral txCount) + 1)
                0
                True
        parentNodes =
            case (snd finalHcState) of
                [] -> []
                res' ->
                    let getParents child merkleNodes parents =
                            let parent = searchParent child merkleNodes
                             in if parent == child
                                    then parents
                                    else getParents parent merkleNodes (parent : parents)
                          where
                            searchParent c li =
                                case L.find (\(MerkleNode _ l r _) -> (node c == l) || (node c == r)) li of
                                    Just p -> p
                                    Nothing -> c
                     in getParents (MerkleNode (Just $ getTxHash coinbaseTxHash) Nothing Nothing True) res' []
        branch = L.init $ L.reverse $ (TxHash . fromJust . node) <$> parentNodes
     in (branch, Just $ L.last branch)

importTxHash :: String -> Hash256
importTxHash = getTxHash . fromJust . hexToTxHash . T.pack
