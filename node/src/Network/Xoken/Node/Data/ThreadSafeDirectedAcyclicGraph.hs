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
import Data.Set as ST
import Data.Text as T
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Numeric as N

data DAGException
    = InsertTimeoutException
    | DependentAlreadyQueued
    deriving (Show)

instance Exception DAGException

data TSDirectedAcyclicGraph v =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v v) -- mapping of vertex to head-of-Sequence
        , topologicalSorted :: !(TSH.TSHashTable v (Seq v)) -- key : value :: head : sequence
        , dependents :: !(TSH.TSHashTable v (MVar v)) -- 
        }

new :: Int16 -> IO (TSDirectedAcyclicGraph v)
new vsize = do
    vertices <- TSH.new vsize
    topoSorted <- TSH.new 1
    dep <- TSH.new 1
    return $ TSDirectedAcyclicGraph vertices topoSorted dep

coalesce :: (Eq v, Hashable v, Ord v) => TSDirectedAcyclicGraph v -> v -> [v] -> IO ()
coalesce dag vt edges = do
    vals <-
        mapM
            (\dep -> do
                 res <- TSH.lookup (vertices dag) dep
                 case res of
                     Just indx -> return indx -- do multi level recursive lookup
                     Nothing -> return dep)
            edges
    let head = vals !! 0
    if L.all (\x -> x == head) vals
        then do
            seq <- TSH.lookup (topologicalSorted dag) (head)
            case seq of
                Just sq -> do
                    etry <- TSH.lookup (vertices dag) vt
                    case etry of
                        Just x -> do
                            hseq <- TSH.lookup (topologicalSorted dag) x
                            case hseq of
                                Just hs -> do
                                    TSH.insert (topologicalSorted dag) head (sq <> hs)
                                Nothing -> do
                                    TSH.insert (topologicalSorted dag) head (sq |> vt)
                        Nothing -> do
                            TSH.insert (topologicalSorted dag) head (sq |> vt)
                            TSH.insert (vertices dag) vt head
                    event <- TSH.lookup (dependents dag) vt
                    case event of
                        Just ev -> liftIO $ putMVar ev head -- value in MVar, vt or head?
                        Nothing -> return ()
                Nothing
                    -- TSH.insert (vertices dag) vt vt -- needed?
                 -> do
                    TSH.insert (topologicalSorted dag) vt (SQ.singleton vt)
        else do
            TSH.insert (topologicalSorted dag) vt (SQ.singleton vt) -- needed?
            -- TSH.insert (vertices dag) vt vt -- needed?
            par <-
                mapM
                    (\dep -> do
                         ev <- TSH.lookup (dependents dag) dep
                         event <-
                             case ev of
                                 Just e -> return e
                                 Nothing -> newEmptyMVar
                         TSH.insert (dependents dag) dep event
                         ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (60 * 1000000))
                         case ores of
                             Right () -> throw DependentTxNotFound
                             Left res -> do
                                 return res)
                    vals
            let uniq = ST.toList $ ST.fromList par
            coalesce dag vt uniq
--
--
--
--            
