{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}

module Network.Xoken.Node.GraphDB where

import Arivi.P2P.P2PEnv as PE hiding (option)
import Codec.Serialise
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Trans (liftIO)
import Control.Monad.Trans.Reader (ReaderT(..))
import Data.Aeson (ToJSON(..), (.=), decode, encode, object)
import qualified Data.ByteString.Base16 as B16 (decode, encode)
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Char
import Data.Hashable
import Data.List ((\\), filter, intersect, nub, nubBy, union, zip4)
import Data.Map.Strict as M (fromList)
import Data.Maybe (fromJust)
import Data.Monoid ((<>))
import Data.Pool (Pool, createPool)
import Data.Text (Text, append, concat, filter, intercalate, isInfixOf, map, null, pack, replace, take, unpack)
import Data.Time.Clock
import Data.Word
import Database.Bolt as BT

import GHC.Generics
import Network.Socket hiding (send)
import Network.Xoken.Crypto.Hash
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction
import System.Random
import Text.Read

data MerkleBranchNode =
    MerkleBranchNode
        { _nodeValue :: Text
        , _isLeftNode :: Bool
        }

instance ToJSON Value where
    toJSON (N _) = toJSON ()
    toJSON (B b) = toJSON b
    toJSON (I i) = toJSON i
    toJSON (F d) = toJSON d
    toJSON (T t) = toJSON t
    toJSON (L l) = toJSON l
    toJSON (M m) = toJSON m
    toJSON _ = undefined -- we do not need Maps and Structures in this example

-- |Create pool of connections (32 stripes, 5000 ms timeout, 64 resource per stripe)
constructState :: BoltCfg -> IO ServerState
constructState bcfg = do
    pool <- createPool (BT.connect bcfg) BT.close 32 5000 64
    return (ServerState pool)

-- | Convert record to MerkleBranchNode
toMerkleBranchNode :: Monad m => Record -> m (MerkleBranchNode)
toMerkleBranchNode r = do
    txid :: Text <- (r `at` "txid") >>= exact
    isLeft :: Bool <- (r `at` "isleft") >>= exact
    return (MerkleBranchNode txid isLeft)

-- | Convert record to Name & ScriptOp
toNameScriptOp :: Monad m => Record -> m ((Text, Text))
toNameScriptOp r = do
    outpoint :: Text <- (r `at` "elem.outpoint") >>= exact
    script :: Text <- (r `at` "elem.script") >>= exact
    return ((outpoint, script))

-- | Filter Null
filterNull :: Monad m => [Record] -> m [Record]
filterNull =
    Control.Monad.filterM
        (\x -> do
             d <- x `at` "elem.outpoint"
             case d of
                 N _ -> pure False
                 _ -> pure True)

-- Fetch the Merkle branch/proof
queryMerkleBranch :: Text -> BoltActionT IO [MerkleBranchNode]
queryMerkleBranch leaf = do
    records <- queryP cypher params
    merkleBranch <- traverse toMerkleBranchNode records
    return merkleBranch
  where
    cypher =
        "MATCH (me:mnode{ v: {leaf} })-[:SIBLING]->(sib)  RETURN sib.v AS txid, sib.l AS isleft  UNION " <>
        "MATCH p=(start:mnode {v: {leaf}})-[:PARENT*]->(end:mnode) WHERE NOT (end)-[:PARENT]->() " <>
        "UNWIND tail(nodes(p)) AS elem  RETURN elem.v as txid, elem.l AS isleft"
    params = fromList [("leaf", T leaf)]

-- |Returns Neo4j DB version
queryGraphDBVersion :: BoltActionT IO [Text]
queryGraphDBVersion = do
    records <- queryP cypher params
    x <- traverse (`at` "version") records
    return $ x >>= exact
  where
    cypher =
        "call dbms.components() yield name, versions, edition unwind versions as version return name, version, edition"
    params = fromList []

-- Fetch the Allegory Name branch
queryAllegoryNameBranch :: Text -> Bool -> BoltActionT IO [Text]
queryAllegoryNameBranch name isProducer = do
    records <- queryP cypher params
    x <- traverse (`at` "outpoint") records
    return $ x >>= exact
  where
    cypher =
        " MATCH p=(pointer:namestate {name: {namestr}})-[:REVISION]-()-[:INPUT*]->(start:nutxo) " <>
        " WHERE NOT (start)-[:INPUT]->() " <> " UNWIND tail(nodes(p)) AS elem " <> " RETURN elem.outpoint as outpoint "
    params =
        if isProducer
            then fromList [("namestr", T (name <> pack "|producer"))]
            else fromList [("namestr", T (name <> pack "|owner"))]

-- Fetch the outpoint & script associated with Allegory name
queryAllegoryNameScriptOp :: Text -> Bool -> BoltActionT IO [(Text, Text)]
queryAllegoryNameScriptOp name isProducer = do
    records <- queryP cypher params >>= filterNull
    x <- traverse toNameScriptOp records
    return x
  where
    cypher =
        " MATCH p=(pointer:namestate {name: {namestr}})-[:REVISION]-(elem:nutxo)  RETURN elem.outpoint , elem.script "
    params =
        if isProducer
            then fromList [("namestr", T (name <> pack "|producer"))]
            else fromList [("namestr", T (name <> pack "|owner"))]

insertMerkleSubTree :: [MerkleNode] -> [MerkleNode] -> BoltActionT IO ()
insertMerkleSubTree leaves inodes = do
    res <- LE.try $ queryP cypher params
    case res of
        Left (e :: SomeException)
            -- liftIO $ print ("[ERROR] insertMerkleSubTree " ++ show e)
            -- liftIO $ print (show leaves)
            -- liftIO $ print (show inodes)
            -- liftIO $ print (show cypher)
            -- liftIO $ print (show params)
         -> do
            throw e
        Right (records) -> return ()
  where
    lefts = Prelude.map (leftChild) inodes
    rights = Prelude.map (rightChild) inodes
    nodes = Prelude.map (node) inodes
    matchReq =
        ((lefts `union` rights) \\ ((lefts `intersect` nodes) `union` (rights `intersect` nodes))) \\
        (Prelude.map (node) leaves)
    matchTemplate = "  (<i>:mnode { v: {<i>}}) "
    createTemplate = " (<i>:mnode { v: {<i>} , l: <f> }) "
    parentRelnTempl = " (<c>)-[:PARENT]->(<p>) "
    siblingRelnTempl = " (<m>)-[:SIBLING]->(<s>) , (<s>)-[:SIBLING]->(<m>)"
    cyCreateLeaves =
        Data.Text.intercalate (" , ") $
        Prelude.map (\(repl, il) -> replace ("<i>") (repl) (replace ("<f>") (il) (pack createTemplate))) $
        zip (vars $ Prelude.map (node) leaves) (Prelude.map (bool2Text . isLeft) leaves)
    cyMatch =
        Data.Text.intercalate (" , ") $
        Prelude.map (\repl -> replace ("<i>") (repl) (pack matchTemplate)) (vars matchReq)
    cyCreateT =
        Data.Text.intercalate (" , ") $
        Prelude.map (\(repl, il) -> replace ("<i>") (repl) (replace ("<f>") (il) (pack createTemplate))) $
        zip (vars nodes) (Prelude.map (bool2Text . isLeft) inodes)
    cyRelationLeft =
        Data.Text.intercalate (" , ") $
        Prelude.map
            (\(rc, rp) -> replace ("<p>") (rp) (replace ("<c>") (rc) (pack parentRelnTempl)))
            (zip (vars lefts) (vars nodes))
    cyRelationRight =
        Data.Text.intercalate (" , ") $
        Prelude.map
            (\(rc, rp) -> replace ("<p>") (rp) (replace ("<c>") (rc) (pack parentRelnTempl)))
            (zip (vars rights) (vars nodes))
    cySiblingReln =
        if length leaves == 2
            then replace
                     ("<m>")
                     (var $ node $ leaves !! 0)
                     (replace ("<s>") (var $ node $ leaves !! 1) (pack siblingRelnTempl))
            else replace -- must be a single lone leaf
                     ("<m>")
                     (var $ node $ leaves !! 0)
                     (replace ("<s>") (var $ node $ leaves !! 0) (pack siblingRelnTempl))
    cyCreate =
        if length leaves > 0
            then if length inodes > 0
                     then Data.Text.intercalate (" , ") $
                          Data.List.filter
                              (not . Data.Text.null)
                              [cyCreateLeaves, cyCreateT, cyRelationLeft, cyRelationRight, cySiblingReln]
                     else Data.Text.intercalate (" , ") $
                          Data.List.filter (not . Data.Text.null) [cyCreateLeaves, cySiblingReln]
            else cyCreateT
    parCreateLeaves = Prelude.map (\(i, x) -> (i, T $ txtTx $ node x)) (zip (vars $ Prelude.map (node) leaves) leaves)
    parCreateSiblingReln =
        Prelude.map
            (\(i, x) -> (i, T $ txtTx $ node x))
            (zip (vars $ Prelude.map (node) leaves) (leaves ++ reverse leaves))
    parCreateArr =
        Prelude.map
            (\(i, j, k, x) -> [(i, T $ txtTx $ node x), (j, T $ txtTx $ leftChild x), (k, T $ txtTx $ rightChild x)])
            (zip4 (vars nodes) (vars lefts) (vars rights) inodes)
    parCreate = Prelude.concat parCreateArr
    params =
        fromList $
        if length leaves > 0
            then parCreateLeaves <> parCreate <> parCreateSiblingReln
            else parCreate
    cypher =
        if length matchReq == 0
            then " CREATE " <> cyCreate
            else " MATCH " <> cyMatch <> " CREATE " <> cyCreate
    txtTx i = txHashToHex $ TxHash $ fromJust i
    vars m = Prelude.map (\x -> Data.Text.filter (isAlpha) $ numrepl $ Data.Text.take 8 $ txtTx x) (m)
    var m = Data.Text.filter (isAlpha) $ numrepl $ Data.Text.take 8 $ txtTx m
    numrepl txt =
        Data.Text.map
            (\x ->
                 case x of
                     '0' -> 'k'
                     '1' -> 'l'
                     '2' -> 'm'
                     '3' -> 'n'
                     '4' -> 'o'
                     '5' -> 'p'
                     '6' -> 'q'
                     '7' -> 'r'
                     '8' -> 's'
                     '9' -> 't'
                     otherwise -> x)
            txt
    bool2Text cond =
        if cond
            then Data.Text.pack " TRUE "
            else Data.Text.pack " FALSE "

deleteMerkleSubTree :: [MerkleNode] -> BoltActionT IO ()
deleteMerkleSubTree inodes = do
    res <- LE.try $ BT.query cypher
    case res of
        Left (e :: SomeException)
            -- liftIO $ print ("[ERROR] deleteMerkleSubTree " ++ show e)
            -- liftIO $ print (show inodes)
            -- liftIO $ print (show cypher)
         -> do
            throw e
        Right (records) -> return ()
  where
    nodes = Prelude.map (node) inodes
    matchTemplate = " MATCH (re:mnode)-[:PARENT*0..8]->(:mnode)<-[:PARENT*0..]-(st:mnode) WHERE re.v IN [ "
    deleteTemplate = " ] DETACH DELETE st"
    inMatch =
        Data.Text.intercalate (" , ") $
        Prelude.map (\repl -> replace ("<h>") (repl) (pack " \'<h>\' ")) (Prelude.map (\z -> txtTx z) nodes)
    cypher = (pack matchTemplate) <> inMatch <> (pack deleteTemplate)
    txtTx i = txHashToHex $ TxHash $ fromJust i
