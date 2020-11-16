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
    , coalesce
    , consolidate
    , getTopologicalSortedForest
    , getPrimaryTopologicalSorted
    , getOrigEdges
    , rollOver
    ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted as LA (async, race)
import Control.Concurrent.MVar
import Control.Exception
import qualified Control.Exception.Extra as EX
import qualified Control.Exception.Lifted as LE (try)
import Control.Monad.IO.Class
import Control.Monad.Loops
import Control.Monad.STM
import Data.Foldable as FD
import Data.Foldable as F
import Data.Function
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.IORef
import Data.Int
import qualified Data.List as L
import Data.Sequence as SQ
import Data.Set as ST
import Data.Text as T
import qualified Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Numeric as N

data DAGException
    = InsertTimeoutException
    | UnexpectedException
    deriving (Show)

instance Exception DAGException

data TSDirectedAcyclicGraph v a =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v (v, Bool, a)) -- mapping of vertex to head-of-Sequence
        , topologicalSorted :: !(TSH.TSHashTable v (Seq v, a))
        , dependents :: !(TSH.TSHashTable v (MVar ())) -- 
        , baseVertex :: !(v)
        , lock :: !(MVar ())
        , origEdges :: !(TSH.TSHashTable v ([v], a))
        }

new :: (Eq v, Hashable v, Ord v, Show v, Num a) => v -> a -> Int16 -> Int16 -> IO (TSDirectedAcyclicGraph v a)
new def initval vertexParts topSortParts = do
    vertices <- TSH.new vertexParts
    dep <- TSH.new 1
    TSH.insert vertices def (def, True, 0)
    lock <- newMVar ()
    topSort <- TSH.new topSortParts
    TSH.insert topSort def (SQ.empty, initval)
    oedg <- TSH.new vertexParts
    return $ TSDirectedAcyclicGraph vertices topSort dep def lock oedg

getTopologicalSortedForest :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v a -> IO ([(v, Maybe v)])
getTopologicalSortedForest dag = do
    forest <- TSH.toList $ topologicalSorted dag
    return $
        L.concatMap
            (\(vt, (dg, _)) -> do
                 let rt =
                         if vt == baseVertex dag
                             then Nothing
                             else Just vt
                 L.map (\x -> do (x, rt)) (FD.toList dg))
            forest

getPrimaryTopologicalSorted :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v a -> IO ([v])
getPrimaryTopologicalSorted dag = do
    primary <- TSH.lookup (topologicalSorted dag) (baseVertex dag)
    case primary of
        Just (pdag, _) -> return $ FD.toList pdag
        Nothing -> return []

consolidate :: (Eq v, Hashable v, Ord v, Show v, Show a, Num a) => TSDirectedAcyclicGraph v a -> (a -> a -> a) -> IO ()
consolidate dag cumulate = do
    rr <- TSH.toList (topologicalSorted dag)
    let !keys = fst $ L.unzip rr
    mapM
        (\(key) -> do
             res <- TSH.lookup (topologicalSorted dag) key
             case res of
                 Just (seq, val) -> do
                     print ("====>", key, seq, val)
                     newh <- TSH.lookup (vertices dag) key
                     case newh of
                         Just (nhx, _, na) -> do
                             if nhx == key
                                 then return ()
                                 else do
                                     fix
                                         (\recur nh -> do
                                              mx <- TSH.lookup (topologicalSorted dag) nh
                                              case mx of
                                                  Just (m, am) -> do
                                                      print ("inserting", nh, key, ((m <> (key <| seq))))
                                                      TSH.insert
                                                          (topologicalSorted dag)
                                                          nh
                                                          ((m <> (key <| seq)), cumulate val am)
                                                      TSH.delete (topologicalSorted dag) key
                                                  Nothing -> do
                                                      print (" Key-head NOT found !", key, nh)
                                                      yz <- TSH.lookup (vertices dag) nh
                                                      case yz of
                                                          Just (x, _, _) -> recur x
                                                      return ())
                                         nhx
                         Nothing -> do
                             print (" NOT found !", key)
                             return ())
        keys
    return ()

getOrigEdges ::
       (Eq v, Hashable v, Ord v, Show v, Show a, Num a) => TSDirectedAcyclicGraph v a -> v -> IO (Maybe ([v], a))
getOrigEdges dag vt = TSH.lookup (origEdges dag) (vt)

rollOver ::
       (Eq v, Hashable v, Ord v, Show v, Show a, Num a)
    => TSDirectedAcyclicGraph v a
    -> [v]
    -> v
    -> a
    -> Int16
    -> Int16
    -> IO (TSDirectedAcyclicGraph v a)
rollOver olddag filterList def initval vertexParts topSortParts
    -- filterMap <- TSH.fromList 10 filterList
    -- forest <- TSH.toList $ topologicalSorted olddag
    -- let univPre = Prelude.foldl (|>) SQ.empty forest
 = do
    newdag <- new def initval vertexParts topSortParts
    mapM_ (\x -> do TSH.delete (origEdges olddag) x) filterList
    TSH.mapM_ (\(vt, (ed, va)) -> do coalesce newdag vt ed va (+)) (origEdges olddag)
    return newdag

coalesce ::
       (Eq v, Hashable v, Ord v, Show v, Show a, Num a)
    => TSDirectedAcyclicGraph v a
    -> v
    -> [v]
    -> a
    -> (a -> a -> a)
    -> IO ()
coalesce dag vt edges aval cumulate = do
    TSH.mutateIO
        (origEdges dag)
        vt
        (\x ->
             case x of
                 Just _ -> return (x, ())
                 Nothing -> return (Just (edges, aval), ()))
    takeMVar (lock dag)
    vals <-
        mapM
            (\dep -> do
                 fix -- do multi level recursive lookup
                     (\recur n -> do
                          res2 <- TSH.lookup (vertices dag) n
                          case res2 of
                              Just (ix, fl, v2) ->
                                  if n == ix
                                      then return ix
                                      else do
                                          y <- recur (ix)
                                          present <-
                                              TSH.mutateIO
                                                  (vertices dag)
                                                  n
                                                  (\ax ->
                                                       case ax of
                                                           Just (_, ff, aa) ->
                                                               if ff
                                                                   then return (Just (y, True, aa), True)
                                                                   else return (Just (y, True, aa), False)
                                                           Nothing -> return (Just (y, True, 0), False))
                                          frag <-
                                              TSH.mutateIO
                                                  (topologicalSorted dag)
                                                  n
                                                  (\fx ->
                                                       case fx of
                                                           Just f -> return (Nothing, Just f)
                                                           Nothing -> return (Nothing, Nothing))
                                          TSH.mutateIO
                                              (topologicalSorted dag)
                                              (y)
                                              (\mz ->
                                                   case mz of
                                                       Just (z, za) ->
                                                           case frag of
                                                               Just (fg, fa) -> do
                                                                   print ("main", z, "frag:", fg)
                                                                   print
                                                                       (za, fa, v2, "<=>", cumulate (cumulate za fa) v2)
                                                                   return
                                                                       ( Just
                                                                             ( z <> (n <| fg)
                                                                             , cumulate (cumulate za fa) v2)
                                                                       , ())
                                                               Nothing -> do
                                                                   if present
                                                                       then return (Just (z, za), ())
                                                                       else do
                                                                           print (za, v2, "=", cumulate za v2)
                                                                           return (Just (z |> n, cumulate za v2), ())
                                                       Nothing -> return (Just (SQ.singleton n, v2), ()))
                                          return y
                              Nothing -> do
                                  return n)
                     dep)
            edges
    putMVar (lock dag) ()
    if L.null vals
            -- takeMVar (lock dag)
        then do
            TSH.insert (vertices dag) vt (baseVertex dag, True, aval)
            TSH.mutateIO
                (topologicalSorted dag)
                (baseVertex dag)
                (\mz ->
                     case mz of
                         Just (z, za) -> return (Just (z |> vt, cumulate za aval), ())
                         Nothing -> return (Just (SQ.singleton vt, aval), ()))
            event <- TSH.lookup (dependents dag) vt
            case event of
                Just ev -> do
                    putStrLn $ "PUT (no edges)" ++ (show vt)
                    liftIO $ putMVar ev ()
                Nothing -> do
                    putStrLn $ "NOTHING (no edges)" ++ (show vt)
                    return ()
        else do
            let head = vals !! 0
            if L.all (\x -> x == head) vals -- if all are same
                then do
                    seq <- TSH.lookup (vertices dag) (head)
                    case seq of
                        Just (_, _, vv) -> do
                            TSH.insert (vertices dag) vt (head, False, aval)
                            TSH.mutateIO
                                (dependents dag)
                                vt
                                (\evnt ->
                                     case evnt of
                                         Just ev -> do
                                             putStrLn $ "PUT " ++ (show vt)
                                             liftIO $ putMVar ev ()
                                             return (Nothing, ())
                                         Nothing -> do
                                             putStrLn $ "NOTHING " ++ (show vt)
                                             return (Nothing, ()))
                        Nothing -> do
                            TSH.insert (vertices dag) vt (vt, False, aval)
                            vrtx <- TSH.lookup (vertices dag) head
                            case vrtx of
                                Just (vx, fl, _) -> return ()
                                Nothing -> do
                                    event <-
                                        TSH.mutateIO
                                            (dependents dag)
                                            head
                                            (\x ->
                                                 case x of
                                                     Just e -> do
                                                         putStrLn $ "EVENT EXISTS " ++ (show head)
                                                         return (x, e)
                                                     Nothing -> do
                                                         em <- newEmptyMVar
                                                         putStrLn $ "NEW EVENT " ++ (show head)
                                                         return (Just em, em))
                                    -- readMVar event
                                    -- return ()
                                    ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (5 * 1000000))
                                    case ores of
                                        Right () -> do
                                            putStrLn $ "InsertTimeoutException " ++ (show head)
                                            -- throw InsertTimeoutException
                                        Left () -> return ()
                            coalesce dag vt [head] aval cumulate
                else do
                    TSH.insert (vertices dag) vt (vt, False, aval)
                    par <-
                        mapM
                            (\dep -> do
                                 vrtx <- TSH.lookup (vertices dag) dep
                                 case vrtx of
                                     Just (vx, fl, _) -> do
                                         return dep -- vx
                                     Nothing -> do
                                         event <-
                                             TSH.mutateIO
                                                 (dependents dag)
                                                 dep
                                                 (\x ->
                                                      case x of
                                                          Just e -> do
                                                              putStrLn $ "EVENT EXISTS (dep)" ++ (show dep)
                                                              return (x, e)
                                                          Nothing -> do
                                                              putStrLn $ "NEW EVENT (dep)" ++ (show dep)
                                                              em <- newEmptyMVar
                                                              return (Just em, em))
                                            --
                                        --  readMVar event
                                        --  return dep)
                                         ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (5 * 1000000))
                                         case ores of
                                             Right () -> do
                                                 putStrLn $ "InsertTimeoutException (dep) " ++ (show dep)
                                                 --  throw InsertTimeoutException
                                                 return dep
                                             Left () -> do
                                                 return dep)
                            vals
                    let uniq = ST.toList $ ST.fromList par
                    coalesce dag vt uniq aval cumulate
    -- verts <- TSH.toList $ vertices dag
    -- print ("Vertices: ", verts)
--
--
--
--            
