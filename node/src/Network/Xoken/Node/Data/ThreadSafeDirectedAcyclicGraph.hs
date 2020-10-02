{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE BangPatterns #-}

module Network.Xoken.Node.Data.ThreadSafeDirectedAcyclicGraph
    ( TSDirectedAcyclicGraph(..)
    , new
    , insert
    -- , delete
    -- , Network.Xoken.Node.Data.ThreadSafeHashTable.lookup
    -- , mutate
    -- , Network.Xoken.Node.Data.ThreadSafeHashTable.mapM_
    -- , fromList
    -- , toList
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, race)
import Control.Concurrent.MVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.IO.Class
import Control.Monad.STM
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.Int
import qualified Data.List as L
import Data.Sequence as SQ
import Data.Text as T
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Numeric as N

data DAGException
    = DependentTxNotFound
    | DependentAlreadyQueued
    deriving (Show)

instance Exception DAGException

data TSDirectedAcyclicGraph v =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v ())
        , topologicalSorted :: !(MVar (Seq v))
        , dependentsQueue :: !(TSH.TSHashTable v (MVar ()))
        }

new :: Int16 -> IO (TSDirectedAcyclicGraph v)
new size = do
    vertices <- TSH.new size
    topoSorted <- newMVar (SQ.empty)
    dep <- TSH.new 1
    return $ TSDirectedAcyclicGraph vertices topoSorted dep

insert :: (Eq v, Hashable v) => TSDirectedAcyclicGraph v -> v -> [v] -> IO ()
insert dag v edges = do
    mapM_
        (\dep -> do
             res <- TSH.lookup (vertices dag) dep
             case res of
                 Just p -> return ()
                 Nothing -> do
                     ev <- TSH.lookup (dependentsQueue dag) dep
                     event <-
                         case ev of
                             Just e -> return e
                             Nothing -> newEmptyMVar
                     TSH.insert (dependentsQueue dag) dep event
                     ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (60 * 1000000))
                     case ores of
                         Right () -> throw DependentTxNotFound
                         Left res -> do
                             TSH.delete (dependentsQueue dag) dep
                             return ())
        edges
    modifyMVar_
        (topologicalSorted dag)
        (\seq -> do
             TSH.insert (vertices dag) v ()
             return $ seq |> v)
    return ()
--
-- insertTry ::
-- getNextVertex ::
-- toList ::
-- take ::
--
--    
-- delete :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO ()
-- delete tsh k = do
--     let index = (hash k) `mod` (fromIntegral $ size tsh)
--     withMVar ((hashTableList tsh) !! index) (\hx -> H.delete hx k)
-- lookup :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO (Maybe v)
-- lookup tsh k = do
--     let index = (hash k) `mod` (fromIntegral $ size tsh)
--     withMVar ((hashTableList tsh) !! index) (\hx -> H.lookup hx k)
-- mutate :: (Eq k, Hashable k) => TSHashTable k v -> k -> (Maybe v -> (Maybe v, a)) -> IO a
-- mutate tsh k f = do
--     v <- Network.Xoken.Node.Data.ThreadSafeHashTable.lookup tsh k
--     case f v of
--         (Nothing, a) -> do
--             delete tsh k
--             return a
--         (Just v, a) -> do
--             insert tsh k v
--             return a
-- mapM_ :: ((k, v) -> IO a) -> TSHashTable k v -> IO ()
-- mapM_ f tsh = do
--     traverse (\ht -> liftIO $ withMVar ht (\hx -> H.mapM_ f hx)) (hashTableList tsh)
--     return ()
-- toList :: (Eq k, Hashable k) => TSHashTable k v -> IO [(k, v)]
-- toList tsh = fmap L.concat $ mapM (\x -> withMVar x (\hx -> H.toList hx)) (hashTableList tsh)
-- fromList :: (Eq k, Hashable k) => Int16 -> [(k, v)] -> IO (TSHashTable k v)
-- fromList size kv = do
--     tsh <- new size
--     traverse (\(k, v) -> insert tsh k v) kv
--     return tsh
-- withTSHLock :: (Eq k, Hashable k) => TSHashTable k v -> k -> (k -> IO ()) -> IO ()
-- withTSHLock tsh k func = do
--     let index = (hash k) `mod` (fromIntegral $ size tsh)
--     withMVar ((hashTableList tsh) !! index) (\hx -> func k)
