{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.DB where

import Control.Exception
import Control.Concurrent.STM.TVar
import Control.Monad.Extra (concatMapM)
import Control.Monad.STM
import Control.Monad.Trans
import Data.ByteString as B
import Data.ByteString.Char8 as C
import Data.Default
import Data.Int
import Data.Serialize as S
import Data.Store as DS
import Data.Text as T
import qualified Data.Text.Encoding as DTE
import qualified Database.RocksDB as R
import Network.Xoken.Block.Common
import Network.Xoken.Block.Headers
import Network.Xoken.Constants
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Transaction.Common
import System.Logger as LG
import Xoken.NodeConfig as NC

conf :: R.Config
conf = def {R.createIfMissing = True, R.errorIfExists = False, R.bloomFilter = True, R.prefixLength = Just 3}

cfStr =
    [ "outputs"
    , "ep_transactions_0"
    , "ep_transactions_1"
    , "ep_transactions_2"
    , "blocktree"
    , "provisional_blockhash"
    ]

columnFamilies = fmap (\x -> (x, conf)) cfStr

withDBCF path = R.withDBCF path conf columnFamilies

runEpochSwitcher = forever $ do
    (ep,sl) <- getCurrentEpoch
    -- TODO: empty next epoch
    atomically $ swapTVar (epochType) ep
    threadDelay (1000000 * sl)

getCurrentEpoch :: Integral a => IO (Epoch,a)
getCurrentEpoch = do
    tm <- ceiling <$> getPOSIXTime -- seconds since unix epoch
    let ws = 604800 -- seconds in a week
        (wn,sl) = fmap (ws `subtract`) $ tm `divMod` 604800 -- week number
        ep = case wn `mod` 3 of
            0 -> Epoch0
            1 -> Epoch1
            2 -> Epoch2
    return (ep,sl)

getTxEpochCF :: Epoch -> String
getTxEpochCF Epoch0 = "ep_transactions_0"
getTxEpochCF Epoch1 = "ep_transactions_1"
getTxEpochCF Epoch2 = "ep_transactions_2"

withCF :: (HasXokenNodeEnv env m, MonadIO m) => String -> (R.ColumnFamily -> m a) -> m a
withCF cfs f = do
    cf <- getCF cfs
    case cf of
        Nothing -> throw ColumnFamilyNotFoundException
        Just c -> f c

getCF :: (HasXokenNodeEnv env m, MonadIO m) => String -> m (Maybe R.ColumnFamily)
getCF cfs = do
    dbe <- getDB
    liftIO $ TSH.lookup (rocksCF dbe) cfs

putTx :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> Tx -> m ()
putTx txh tx = do
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    putX (getTxEpochCF epoch) txh tx

getTx :: (HasXokenNodeEnv env m, MonadIO m) => TxHash -> m (Maybe Tx)
getTx txh = do
    bp2pEnv <- getBitcoinP2P
    epoch <- liftIO $ readTVarIO (epochType bp2pEnv)
    epc <- getX (getTxEpochCF epoch) txh
    if epc == Nothing
        then getX (getTxEpochCF $ prevEpoch epoch) txh
        else return $ epc

txFromHash conn cf txh = do
    cftx <- liftIO $ TSH.lookup cf ("tx")
    case cftx of
        Just cftx' -> do
            tx' <- getIO conn cftx' (txh)
            return tx'
        Nothing -> do
            return Nothing -- ideally should be unreachable

putOutput :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> ZtxiUtxo -> m ()
putOutput = putX "outputs"

getOutput :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> m (Maybe ZtxiUtxo)
getOutput = getX "outputs"

deleteOutput :: (HasXokenNodeEnv env m, MonadIO m) => OutPoint -> m ()
deleteOutput = deleteX "outputs"

putProvisionalBlockHash :: (HasXokenNodeEnv env m, MonadIO m) => BlockHash -> BlockHash -> m ()
putProvisionalBlockHash pbh bh = do
    dbe' <- getDB
    pcf <- liftIO $ TSH.lookup (rocksCF dbe') "provisional_blockhash"
    case pcf of
        Just pc -> do
            putProvisionalBlockHashIO (rocksDB dbe') pc pbh bh 
            updatePredecessors
        Nothing -> return ()

putProvisionalBlockHashIO :: (MonadIO m) => R.DB -> R.ColumnFamily -> BlockHash -> BlockHash -> m ()
putProvisionalBlockHashIO rkdb cf pbh bh = do
    R.putCF rkdb cf (DS.encode pbh) (DS.encode bh)
    R.putCF rkdb cf (DS.encode bh) (DS.encode pbh)

fetchBestBlock :: (HasXokenNodeEnv env m, MonadIO m) => m (BlockNode)
fetchBestBlock = do
    bp2pEnv <- getBitcoinP2P
    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
    return $ memoryBestHeader hm

putHeaderMemoryElem :: (HasXokenNodeEnv env m, MonadIO m) => BlockNode -> m ()
putHeaderMemoryElem b = putX "blocktree" (shortBlockHash $ headerHash $ nodeHeader b) b

putHeaderMemoryElemIO :: (MonadIO m) => R.DB -> R.ColumnFamily -> BlockNode -> m ()
putHeaderMemoryElemIO rkdb cf b = do
    let sb = DS.encode $ shortBlockHash $ headerHash $ nodeHeader b
        bne = DS.encode b
    R.putCF rkdb cf sb bne

putBestBlockNode :: (HasXokenNodeEnv env m, MonadIO m) => BlockNode -> m ()
putBestBlockNode b = do
    dbe' <- getDB
    let rkdb = rocksDB dbe'
        bne = DS.encode b
    R.put rkdb ("blocknode" :: B.ByteString) bne

getBestBlockNode :: (HasXokenNodeEnv env m, MonadIO m) => m (Maybe BlockNode)
getBestBlockNode = do
    dbe' <- getDB
    let rkdb = rocksDB dbe'
    getBestBlockNodeIO rkdb

getBestBlockNodeIO :: (MonadIO m) => R.DB -> m (Maybe BlockNode)
getBestBlockNodeIO rkdb = do
    bne <- R.get rkdb ("blocknode" :: B.ByteString)
    return $
        case DS.decode <$> bne of
            Just (Right b) -> Just b
            _ -> Nothing

updatePredecessors :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
updatePredecessors = do
    bp2pEnv <- getBitcoinP2P
    pr <- fetchPredecessors
    liftIO $ atomically $ swapTVar (predecessors bp2pEnv) pr
    return ()

fetchPredecessorsIO :: (MonadIO m) => R.DB -> R.ColumnFamily -> HeaderMemory -> m [BlockHash]
fetchPredecessorsIO rkdb pcf hm =
    concatMapM
        (\x -> do
             let hash = headerHash $ nodeHeader x
             ph <- getIO rkdb pcf hash
             case (ph :: Maybe BlockHash) of
                 Nothing -> return [hash]
                 Just p -> return [hash, p]) $
    getParents hm (10) (memoryBestHeader hm)

fetchPredecessors :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m [BlockHash]
fetchPredecessors = do
    lg <- getLogger
    dbe <- getDB
    bp2pEnv <- getBitcoinP2P
    hm <- liftIO $ readTVarIO (blockTree bp2pEnv)
    let rkdb = rocksDB dbe
        cf = rocksCF dbe
    pcfm <- liftIO $ TSH.lookup cf "provisional_blockhash"
    case pcfm of
        Nothing -> return []
        Just pcf -> fetchPredecessorsIO rkdb pcf hm

-- put, get and delete with XokenNodeEnv and cf input as String

putX :: (HasXokenNodeEnv env m, MonadIO m, Store a, Store b) => String -> a -> b -> m ()
putX cfs k v = do
    dbe' <- getDB
    withCF cfs $ \cf -> putIO (rocksDB dbe') cf k v

getX :: (HasXokenNodeEnv env m, MonadIO m, Store a, Store b) => String -> a -> m (Maybe b)
getX cfs k = do
    dbe' <- getDB
    withCF cfs $ \cf -> getIO (rocksDB dbe') cf k

deleteX :: (HasXokenNodeEnv env m, MonadIO m, Store a) => String -> a -> m ()
deleteX cfs k = do
    dbe' <- getDB
    withCF cfs $ \cf -> deleteIO (rocksDB dbe') cf k

scanX cfs = do
    dbe' <- getDB
    cf' <- liftIO $ TSH.lookup (rocksCF dbe') cfs
    case cf' of
        Just cf -> do
            scanCF (rocksDB dbe') cf
        Nothing -> return []
--

scanCF db cf =
    liftIO $ do
        R.withIterCF db cf $ \iter -> do
            R.iterFirst iter
            getNext iter
          --getNext :: Iterator -> m [(Maybe ByteString,Maybe ByteString)]
  where
    getNext i = do
        valid <- R.iterValid i
        if valid
            then do
                kv <- R.iterEntry i
                R.iterNext i
                sn <- getNext i
                return $
                    case kv of
                        Just (k, v) -> ((k, v) : sn)
                        _ -> sn
            else return []


putDefault :: (Store a, Store b, MonadIO m) => R.DB -> a -> b -> m ()
putDefault rkdb k v = R.put rkdb (DS.encode k) (DS.encode v)

getDefault :: (Store a, Store b, MonadIO m) => R.DB -> a -> m (Maybe b)
getDefault rkdb k = do
    res <- R.get rkdb (DS.encode k)
    case DS.decode <$> res of
        Just (Left e) -> do
            liftIO $ print $ "getDB' ERROR: " ++ show e
            throw KeyValueDBLookupException
        Just (Right m) -> return $ Just m
        Nothing -> return Nothing

deleteDefault :: (Store a, MonadIO m) => R.DB -> a -> m ()
deleteDefault rkdb k = R.delete rkdb (DS.encode k)

putIO :: (Store a, Store b, MonadIO m) => R.DB -> R.ColumnFamily -> a -> b -> m ()
putIO rkdb cf k v = R.putCF rkdb cf (DS.encode k) (DS.encode v)

getIO :: (Store a, Store b, MonadIO m) => R.DB -> R.ColumnFamily -> a -> m (Maybe b)
getIO rkdb cf k = do
    res <- R.getCF rkdb cf (DS.encode k)
    case DS.decode <$> res of
        Just (Left e) -> do
            liftIO $ print $ "getDBCF ERROR" ++ show e
            throw KeyValueDBLookupException
        Just (Right m) -> return $ Just m
        Nothing -> return Nothing

deleteIO :: (Store a, MonadIO m) => R.DB -> R.ColumnFamily -> a -> m ()
deleteIO rkdb cf k = R.deleteCF rkdb cf (DS.encode k)

markBestSyncedBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Text -> Int32 -> m ()
markBestSyncedBlock hash height = do
    rkdb <- rocksDB <$> getDB
    R.put rkdb "best_synced_hash" $ DTE.encodeUtf8 hash
    R.put rkdb "best_synced_height" $ C.pack $ show height
    -- liftIO $ print "MARKED BEST SYNCED INTO ROCKS DB"

checkBlocksFullySynced :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Network -> m Bool
checkBlocksFullySynced net = do
    rkdb <- rocksDB <$> getDB
    bestBlock <- fetchBestBlock
    bestSynced <- fetchBestSyncedBlock
    return $ (headerHash $ nodeHeader bestBlock, fromIntegral $ nodeHeight bestBlock) == bestSynced

checkBlocksFullySynced_ :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => Network -> m Int32
checkBlocksFullySynced_ net = do
    rkdb <- rocksDB <$> getDB
    bestBlock <- fetchBestBlock
    bestSynced <- fetchBestSyncedBlock
    return $ (fromIntegral $ nodeHeight bestBlock) - (snd bestSynced)

fetchBestSyncedBlock :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ((BlockHash, Int32))
fetchBestSyncedBlock = do
    lg <- getLogger
    rkdb <- rocksDB <$> getDB
    hash <- liftIO $ R.get rkdb "best_synced_hash"
    height <- liftIO $ R.get rkdb "best_synced_height"
    case (hash, height) of
        (Just hs, Just ht)
            -- liftIO $
            --     print $
            --     "FETCHED BEST SYNCED FROM ROCKS DB: " ++
            --     (T.unpack $ DTE.decodeUtf8 hs) ++ " " ++ (T.unpack . DTE.decodeUtf8 $ ht)
         -> do
            case hexToBlockHash $ DTE.decodeUtf8 hs of
                Nothing -> throw InvalidBlockHashException
                Just hs' -> return (hs', read . T.unpack . DTE.decodeUtf8 $ ht :: Int32)
        _ -> do
            net <- (bitcoinNetwork . nodeConfig) <$> getBitcoinP2P
            debug lg $ LG.msg $ val "Bestblock is genesis."
            return ((headerHash $ getGenesisHeader net), 0)