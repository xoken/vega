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

module Network.Xoken.Node.Data.ThreadSafeHashTable
    ( TSHashTable(..)
    , new
    , insert
    , delete
    , Network.Xoken.Node.Data.ThreadSafeHashTable.lookup
    , mutateIO
    , Network.Xoken.Node.Data.ThreadSafeHashTable.mapM_
    , fromList
    , toList
    , lookupIndex
    , nextByIndex
    ) where

import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Control.Monad.STM
import qualified Data.HashTable.IO as H
import Data.Hashable
import Data.Int
import qualified Data.List as L
import Data.Text as T
import Numeric as N

type HashTable k v = H.BasicHashTable k v

data TSHashTable k v =
    TSHashTable
        { hashTableList :: ![MVar (HashTable k v)]
        , size :: !Int16
        }

type GenericShortHash = Int

new :: Int16 -> IO (TSHashTable k v)
new size = do
    hlist <-
        mapM
            (\x -> do
                 hm <- H.new
                 newMVar hm)
            [1 .. size]
    return $ TSHashTable hlist size

insert :: (Eq k, Hashable k) => TSHashTable k v -> k -> v -> IO ()
insert tsh k v = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.insert hx k v)

delete :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO ()
delete tsh k = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.delete hx k)

lookup :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO (Maybe v)
lookup tsh k = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.lookup hx k)

mutateIO :: (Eq k, Hashable k) => TSHashTable k v -> k -> (Maybe v -> IO (Maybe v, a)) -> IO a
mutateIO tsh k f = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar
        ((hashTableList tsh) !! index)
        (\hx -> do
             mv <- H.lookup hx k
             (newval, a) <- f mv
             case newval of
                 Just nv -> do
                     H.insert hx k nv
                     return a
                 Nothing -> do
                     H.delete hx k
                     return a)

mapM_ :: ((k, v) -> IO a) -> TSHashTable k v -> IO ()
mapM_ f tsh = do
    traverse (\ht -> liftIO $ withMVar ht (\hx -> H.mapM_ f hx)) (hashTableList tsh)
    return ()

toList :: (Eq k, Hashable k) => TSHashTable k v -> IO [(k, v)]
toList tsh = fmap L.concat $ mapM (\x -> withMVar x (\hx -> H.toList hx)) (hashTableList tsh)

fromList :: (Eq k, Hashable k) => Int16 -> [(k, v)] -> IO (TSHashTable k v)
fromList size kv = do
    tsh <- new size
    traverse (\(k, v) -> insert tsh k v) kv
    return tsh

lookupIndex :: (Eq k, Hashable k) => TSHashTable k v -> k -> IO (Maybe (Word))
lookupIndex tsh k = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.lookupIndex hx k)

nextByIndex :: (Eq k, Hashable k) => TSHashTable k v -> (k, Word) -> IO (Maybe (Word, k, v))
nextByIndex tsh (k, w) = do
    let index = (hash k) `mod` (fromIntegral $ size tsh)
    withMVar ((hashTableList tsh) !! index) (\hx -> H.nextByIndex hx w)
