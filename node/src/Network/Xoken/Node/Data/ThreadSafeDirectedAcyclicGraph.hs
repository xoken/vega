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

data DAGException =
    InsertTimeoutException
    deriving (Show)

instance Exception DAGException

data TSDirectedAcyclicGraph v =
    TSDirectedAcyclicGraph
        { vertices :: !(TSH.TSHashTable v (v, Bool)) -- mapping of vertex to head-of-Sequence
        , topologicalSorted :: !(TSH.TSHashTable v (Seq v))
        , dependents :: !(TSH.TSHashTable v (MVar ())) -- 
        , baseVertex :: !(v)
        , lock :: MVar ()
        }

new :: (Eq v, Hashable v, Ord v, Show v) => v -> IO (TSDirectedAcyclicGraph v)
new def = do
    vertices <- TSH.new 100
    dep <- TSH.new 100
    TSH.insert vertices def (def, True)
    lock <- newMVar ()
    topSort <- TSH.new 100
    TSH.insert topSort def (SQ.empty)
    return $ TSDirectedAcyclicGraph vertices topSort dep def lock

consolidate :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v -> IO ()
consolidate dag = do
    mindex <- TSH.lookupIndex (topologicalSorted dag) (baseVertex dag)
    case mindex of
        Just indx -> do
            indxRef <- newIORef (indx, baseVertex dag)
            continue <- newIORef True
            whileM_ (readIORef continue) $ do
                (ix, ky) <- readIORef indxRef
                res <- TSH.nextByIndex (topologicalSorted dag) (ky, ix + 1)
                print ("NEXT: ", res)
                case res of
                    Just (index, kn, val) -> do
                        writeIORef indxRef (index, kn)
                        tsd <- TSH.toList $ topologicalSorted dag
                        mapM (\(h, x) -> do print (h, F.toList x)) tsd
                        print ("===================================================")
                        fix
                            (\recur key -> do
                                 newh <- TSH.lookup (vertices dag) key
                                 case newh of
                                     Just (nh, _) -> do
                                         if nh == key
                                             then return ()
                                             else do
                                                 mx <- TSH.lookup (topologicalSorted dag) nh
                                                 case mx of
                                                     Just m -> do
                                                         TSH.insert (topologicalSorted dag) nh (m <> (kn <| val))
                                                         TSH.delete (topologicalSorted dag) kn
                                                     Nothing -> do
                                                         recur nh
                                     Nothing -> return ())
                            (kn)
                    Nothing -> writeIORef continue False -- end loop
    return ()

coalesce :: (Eq v, Hashable v, Ord v, Show v) => TSDirectedAcyclicGraph v -> v -> [v] -> IO ()
coalesce dag vt edges = do
    takeMVar (lock dag)
    vals <-
        mapM
            (\dep -> do
                 fix -- do multi level recursive lookup
                     (\recur n -> do
                          res2 <- TSH.lookup (vertices dag) n
                          case res2 of
                              Just (ix, fl) ->
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
                                                           Just (_, ff) ->
                                                               if ff
                                                                   then return (Just (y, True), True)
                                                                   else return (Just (y, True), False)
                                                           Nothing -> return (Just (y, True), False))
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
                                                       Just z ->
                                                           case frag of
                                                               Just fg -> do
                                                                   return (Just $ z <> fg, ())
                                                               Nothing -> do
                                                                   if present
                                                                       then return (Just $ z, ())
                                                                       else return (Just $ z |> n, ())
                                                       Nothing -> return (Just $ SQ.singleton n, ()))
                                          return y
                              Nothing -> do
                                  return n)
                     dep)
            edges
    putMVar (lock dag) ()
    if L.null vals
            -- takeMVar (lock dag)
        then do
            TSH.insert (vertices dag) vt (baseVertex dag, True)
            TSH.mutateIO
                (topologicalSorted dag)
                (baseVertex dag)
                (\mz ->
                     case mz of
                         Just z -> return (Just $ z |> vt, ())
                         Nothing -> return (Just $ SQ.singleton vt, ()))
        else do
            let head = vals !! 0
            if L.all (\x -> x == head) vals -- if all are same
                    -- takeMVar (lock dag)
                then do
                    seq <- TSH.lookup (vertices dag) (head)
                    case seq of
                        Just sq -> do
                            TSH.insert (vertices dag) vt (head, False)
                            event <- TSH.lookup (dependents dag) vt
                            case event of
                                Just ev -> liftIO $ putMVar ev ()
                                Nothing -> return ()
                            -- putMVar (lock dag) ()
                        Nothing -> do
                            TSH.insert (vertices dag) vt (vt, False)
                            -- putMVar (lock dag) ()
                            vrtx <- TSH.lookup (vertices dag) head
                            case vrtx of
                                Just (vx, fl) -> do
                                    return head -- vx
                                Nothing -> do
                                    event <-
                                        TSH.mutateIO
                                            (dependents dag)
                                            head
                                            (\x ->
                                                 case x of
                                                     Just e -> return (x, e)
                                                     Nothing -> do
                                                         em <- newEmptyMVar
                                                         return (Just em, em))
                                    ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (60 * 1000000))
                                    case ores of
                                        Right () -> throw InsertTimeoutException
                                        Left () -> do
                                            return head
                            coalesce dag vt [head]
                else do
                    TSH.insert (vertices dag) vt (vt, False)
                    par <-
                        mapM
                            (\dep -> do
                                 vrtx <- TSH.lookup (vertices dag) dep
                                 case vrtx of
                                     Just (vx, fl) -> do
                                         return dep -- vx
                                     Nothing -> do
                                         event <-
                                             TSH.mutateIO
                                                 (dependents dag)
                                                 dep
                                                 (\x ->
                                                      case x of
                                                          Just e -> return (x, e)
                                                          Nothing -> do
                                                              em <- newEmptyMVar
                                                              return (Just em, em))
                                            --
                                         ores <- LA.race (liftIO $ readMVar event) (liftIO $ threadDelay (60 * 1000000))
                                         case ores of
                                             Right () -> throw InsertTimeoutException
                                             Left () -> do
                                                 return dep)
                            vals
                    let uniq = ST.toList $ ST.fromList par
                    coalesce dag vt uniq
    -- verts <- TSH.toList $ vertices dag
    -- print ("Vertices: ", verts)
--
--
--
--            
