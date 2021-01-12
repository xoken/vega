{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Arivi.Crypto.Utils.PublicKey.Signature as ACUPS
import Arivi.Crypto.Utils.PublicKey.Utils
import qualified Arivi.P2P.Config as Config
import Arivi.P2P.Kademlia.Types
import Control.Concurrent
import Control.Concurrent.Async as A (async)
import Control.Concurrent.Async.Lifted as LA (wait, withAsync)
import Control.Concurrent.MSem as MS
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Except
import Control.Monad.Reader
import qualified Control.Monad.STM as CMS (atomically)
import Control.Monad.Trans.Control
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Short as BSS
import Data.Function
import Data.HashMap.Strict as HM
import qualified Data.HashTable.IO as H
import Data.List
import Data.Map.Strict as M
import Data.Maybe as DM
import Data.Serialize as S
import qualified Data.Text as DT
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import Data.Time.Clock
import qualified Database.RocksDB as R hiding (rocksDB)
import Network.Xoken.Block.Headers
import Network.Xoken.Node.DB
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Server
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.PeerManager
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.TLSServer
import Network.Xoken.Node.Worker.Listener
import Options.Applicative
import Prelude as P
import qualified Snap
import System.Directory (doesDirectoryExist, doesFileExist)
import qualified System.Logger as LG
import System.Posix.Daemon
import Xoken
import Xoken.NodeConfig as NC

newtype AppM a =
    AppM (ReaderT ServiceEnv IO a)
    deriving (Functor, Applicative, Monad, MonadReader ServiceEnv, MonadIO, MonadThrow, MonadCatch)

deriving instance MonadBase IO AppM

deriving instance MonadBaseControl IO AppM

instance HasBitcoinP2P AppM where
    getBitcoinP2P = asks (bitcoinP2PEnv . xokenNodeEnv)

instance HasDatabaseHandles AppM where
    getDB = asks (dbHandles . xokenNodeEnv)

instance HasAllegoryEnv AppM where
    getAllegory = asks (allegoryEnv . xokenNodeEnv)

instance HasLogger AppM where
    getLogger = asks (loggerEnv . xokenNodeEnv)

runAppM :: ServiceEnv -> AppM a -> IO a
runAppM env (AppM app) = runReaderT app env

type HashTable k v = H.BasicHashTable k v

defaultConfig :: IO ()
defaultConfig = do
    (sk, _) <- ACUPS.generateKeyPair
    let bootstrapPeer =
            Peer
                ((fst . B16.decode)
                     "a07b8847dc19d77f8ef966ba5a954dac2270779fb028b77829f8ba551fd2f7ab0c73441456b402792c731d8d39c116cb1b4eb3a18a98f4b099a5f9bdffee965c")
                (NodeEndPoint "51.89.40.95" 5678 5678)
    let config =
            Config.Config 5678 5678 sk [bootstrapPeer] (generateNodeId sk) "127.0.0.1" (T.pack "./arivi.log") 20 5 3
    Config.makeConfig config "./arivi-config.yaml"

runThreads :: Config.Config -> NC.NodeConfig -> BitcoinP2P -> LG.Logger -> [FilePath] -> IO ()
runThreads config nodeConf bp2p lg certPaths = do
    withDBCF "xdb" $ \rkdb -> do
        let cfZip = zip cfStr (R.columnFamilies rkdb)
            btcf = snd $ fromJust $ Data.List.find ((== "blocktree") . fst) cfZip
            pcf = snd $ fromJust $ Data.List.find ((== "provisional_blockhash") . fst) cfZip
        hm <- repopulateBlockTree (bitcoinNetwork nodeConf) rkdb btcf
        nh <-
            case hm of
                Just h -> CMS.atomically $ swapTVar (blockTree bp2p) h
                Nothing -> readTVarIO (blockTree bp2p) -- TODO: handled Nothing due to errors
        pred <- fetchPredecessorsIO rkdb pcf nh
        CMS.atomically $ swapTVar (predecessors bp2p) pred
        cfM <- TSH.fromList 1 cfZip
        let dbh = DatabaseHandles rkdb cfM
        let allegoryEnv = AllegoryEnv $ allegoryVendorSecretKey nodeConf
        let xknEnv = XokenNodeEnv bp2p dbh lg allegoryEnv
        let serviceEnv = ServiceEnv xknEnv
        -- start TLS endpoint
        epHandler <- newTLSEndpointServiceHandler
        async $ startTLSEndpoint epHandler (endPointTLSListenIP nodeConf) (endPointTLSListenPort nodeConf) certPaths
        -- start HTTP endpoint
        let snapConfig =
                Snap.defaultConfig & Snap.setSSLBind (DTE.encodeUtf8 $ DT.pack $ endPointHTTPSListenIP nodeConf) &
                Snap.setSSLPort (fromEnum $ endPointHTTPSListenPort nodeConf) &
                Snap.setSSLKey (certPaths !! 1) &
                Snap.setSSLCert (head certPaths) &
                Snap.setSSLChainCert False
        async $ Snap.serveSnaplet snapConfig (appInit xknEnv)
        --
        -- current node
        let node = vegaNode nodeConf
            normalizedClstr =
                sortBy (\(Node a _ _ _ _) (Node b _ _ _ _) -> compare a b) (node:vegaCluster nodeConf)
        --
        -- run vegaCluster
        runAppM
            serviceEnv
            (withAsync (startTCPServer (_nodeIPAddr node) (_nodePort node)) $ \y -> do
                    if (_nodeType node == NC.Master)
                        then do
                            withAsync (initializeWorkers node normalizedClstr) $ \_ -> do
                                withAsync setupSeedPeerConnection $ \_ -> do
                                    withAsync runEgressChainSync $ \_ -> do
                                        withAsync runBlockCacheQueue $ \_ -> do
                                            withAsync (handleNewConnectionRequest epHandler) $ \_ -> do
                                                withAsync runPeerSync $ \_ -> do
                                                    withAsync runSyncStatusChecker $ \_ -> do
                                                        withAsync runEpochSwitcher $ \z -> do
                                                            _ <- LA.wait z
                                                            return ()
                        else LA.wait y)
        liftIO $ putStrLn $ "node recovering from fatal DB connection failure!"
    return ()

runSyncStatusChecker :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runSyncStatusChecker = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    newCandidateBlockChainTip
    -- wait 300 seconds before first check
    liftIO $ threadDelay (30 * 1000000)
    forever $ do
        isSynced <- checkBlocksFullySynced
        LG.debug lg $
            LG.msg $
            LG.val $
            C.pack $
            "Sync Status Checker: All blocks synced? " ++
            if isSynced
                then "Yes"
                else "No"
        mn <-
            if isSynced
                then mineBlockFromCandidate
                else return Nothing
        liftIO $ print $ "Sync status: " ++ show mn
        liftIO $ CMS.atomically $ writeTVar (indexUnconfirmedTx bp2pEnv) isSynced
        liftIO $ threadDelay (60 * 1000000)

runNode :: Config.Config -> NC.NodeConfig -> BitcoinP2P -> [FilePath] -> IO ()
runNode config nodeConf bp2p certPaths = do
    lg <-
        LG.new
            (LG.setOutput
                 (LG.Path $ T.unpack $ NC.logFileName nodeConf)
                 (LG.setLogLevel (logLevel nodeConf) LG.defSettings))
    runThreads config nodeConf bp2p lg certPaths

defBitcoinP2P :: NodeConfig -> IO BitcoinP2P
defBitcoinP2P nodeCnf = do
    g <- newTVarIO M.empty
    bp <- newTVarIO M.empty
    mv <- newMVar True
    hl <- newMVar True
    st <- TSH.new 1
    tl <- TSH.new 1
    epoch <- getCurrentEpoch (epochLength nodeCnf)
    ep <- newTVarIO $ fst epoch
    tc <- TSH.new 1
    rpf <- newEmptyMVar
    rpc <- newTVarIO 0
    mq <- TSH.new 1
    ts <- TSH.new 1
    tbt <- MS.new $ maxTMTBuilderThreads nodeCnf
    iut <- newTVarIO False
    udc <- H.new
    blktr <- newTVarIO $ initialChain $ bitcoinNetwork nodeCnf
    wrkc <- newTVarIO []
    bsb <- newTVarIO Nothing
    ptxq <- TSH.new 1
    cand <- TSH.new 1
    cblk <- TSH.new 1
    cmpct <- TSH.new 1
    pftx <- TSH.new 10
    cbu <- TSH.new 1
    pr <- newTVarIO []
    return $
        BitcoinP2P
            nodeCnf
            g
            bp
            mv
            hl
            st
            tl
            ep
            tc
            (rpf, rpc)
            mq
            ts
            tbt
            iut
            udc
            blktr
            wrkc
            bsb
            ptxq
            cand
            cblk
            cmpct
            pftx
            cbu
            pr

initVega :: IO ()
initVega = do
    putStrLn "Starting Xoken Nexa"
    b <- doesFileExist "arivi-config.yaml"
    unless b defaultConfig
    cnf <- Config.readConfig "arivi-config.yaml"
    nodeCnf <- NC.readConfig "node-config.yaml"
    bp2p <- defBitcoinP2P nodeCnf
    let certFP = tlsCertificatePath nodeCnf
        keyFP = tlsKeyfilePath nodeCnf
        csrFP = tlsCertificateStorePath nodeCnf
    cfp <- doesFileExist certFP
    kfp <- doesFileExist keyFP
    csfp <- doesDirectoryExist csrFP
    unless (cfp && kfp && csfp) $ P.error "Error: missing TLS certificate or keyfile"
    -- launch node --
    runNode cnf nodeCnf bp2p [certFP, keyFP, csrFP]

repopulateBlockTree :: Network -> R.DB -> R.ColumnFamily -> IO (Maybe HeaderMemory)
repopulateBlockTree net rkdb cf = do
    print "Loading BlockTree from memory..."
    t1 <- getCurrentTime
    kv <- scanCF rkdb cf
    if Data.List.null kv
            -- print "BlockTree not found"
        then do
            putHeaderMemoryElemIO rkdb cf $ genesisNode net
            return Nothing
        else do
            t2 <- getCurrentTime
            let kv' =
                    DM.mapMaybe
                        (\(k, v) ->
                             case S.decode k of
                                 Right (k' :: ShortBlockHash) -> Just (k', BSS.toShort v)
                                 Left _ -> Nothing)
                        kv
            t3 <- getCurrentTime
            bn <- getBestBlockNodeIO rkdb
            case bn of
                Nothing
                    -- print "getBestBlockNodeIO returned Nothing"
                 -> return Nothing
                Just bn' -> do
                    putStrLn $ "Loaded " ++ show (length kv') ++ " BlockTree entries"
                    --putStrLn $ "Started scan: " ++ show t1
                    --putStrLn $ "Stopped scan and started decode: " ++ show t2
                    --putStrLn $ "Stopped decode " ++ show t3
                    return $ Just $ HeaderMemory (HM.fromList kv') bn'

relaunch :: IO ()
relaunch =
    forever $ do
        let pid = "/tmp/vega.pid.1"
        running <- isRunning pid
        if running
            then threadDelay (30 * 1000000)
            else do
                runDetached (Just pid) (ToFile "vega.log") initVega
                threadDelay 5000000

main :: IO ()
main = do
    initVega
    -- let pid = "/tmp/vega.pid.0"
    -- runDetached (Just pid) (ToFile "vega.log") relaunch
{-
data Config =
    Config
        { configNetwork :: !Network
        , configDebug :: !Bool
        , configUnconfirmedTx :: !Bool
        }

defPort :: Int
defPort = 3000

defNetwork :: Network
defNetwork = bsvTest

netNames :: String
netNames = intercalate "|" (Data.List.map getNetworkName allNets)
-}
