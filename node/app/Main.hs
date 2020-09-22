{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE BlockArguments #-}

import Arivi.Crypto.Utils.PublicKey.Signature as ACUPS
import Arivi.Crypto.Utils.PublicKey.Utils
import Arivi.Crypto.Utils.Random
import Arivi.Env
import Arivi.Network
import Arivi.P2P
import qualified Arivi.P2P.Config as Config
import Arivi.P2P.Kademlia.Types
import Arivi.P2P.P2PEnv as PE hiding (option)
import Arivi.P2P.PubSub.Types
import Arivi.P2P.RPC.Types
import Arivi.P2P.ServiceRegistry
import qualified Codec.Serialise as CBOR
import Control.Arrow
import Control.Concurrent (threadDelay)
import Control.Concurrent
import Control.Concurrent.Async as A (async)
import Control.Concurrent.Async.Lifted as LA (async, race, wait, withAsync)
import Control.Concurrent.Event as EV
import Control.Concurrent.MSem as MS
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM.TVar
import Control.Exception (throw)
import Control.Monad
import Control.Monad
import Control.Monad.Base
import Control.Monad.Catch
import Control.Monad.Except
import Control.Monad.Logger
import Control.Monad.Loops
import Control.Monad.Reader
import qualified Control.Monad.STM as CMS (atomically)
import Control.Monad.Trans.Control
import Control.Monad.Trans.Maybe
import Data.Aeson.Encoding (encodingToLazyByteString, fromEncoding)
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import Data.ByteString.Base64 as B64
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as CL
import Data.Char
import Data.Default
import Data.Default
import Data.Function
import Data.Functor.Identity
import qualified Data.HashTable.IO as H
import Data.IORef
import Data.Int
import Data.List
import Data.Map.Strict as M
import Data.Maybe
import Data.Maybe
import Data.Pool
import Data.Serialize as Serialize
import Data.Serialize as S
import Data.String.Conv
import Data.String.Conversions
import qualified Data.Text as DT
import qualified Data.Text as T
import qualified Data.Text.Encoding as DTE
import qualified Data.Text.Lazy as TL
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Typeable
import Data.Version
import Data.Word (Word32)
import Data.Word
import qualified Database.Bolt as BT
import qualified Database.RocksDB as R hiding (rocksDB)
import Network.Simple.TCP
import Network.Socket
import Network.Xoken.Node.AriviService
import Network.Xoken.Node.Data
import Network.Xoken.Node.Data.ThreadSafeHashTable as TSH
import Network.Xoken.Node.Env
import Network.Xoken.Node.GraphDB
import Network.Xoken.Node.HTTP.Server
import Network.Xoken.Node.P2P.BlockSync
import Network.Xoken.Node.P2P.ChainSync
import Network.Xoken.Node.P2P.Common
import Network.Xoken.Node.P2P.PeerManager
import Network.Xoken.Node.P2P.Types
import Network.Xoken.Node.Service.Chain
import Network.Xoken.Node.WorkerListener
import Options.Applicative
import Paths_vega as P
import Prelude as P
import qualified Snap as Snap
import StmContainers.Map as SM
import System.Directory (doesDirectoryExist, doesFileExist)
import System.Environment (getArgs)
import System.Exit
import System.FilePath
import System.IO.Unsafe
import qualified System.Logger as LG
import qualified System.Logger.Class as LGC
import System.Posix.Daemon
import System.Random
import System.ZMQ4 as Z
import Text.Read (readMaybe)
import Xoken
import Xoken.Node
import Xoken.NodeConfig as NC

newtype AppM a =
    AppM (ReaderT (ServiceEnv) (IO) a)
    deriving (Functor, Applicative, Monad, MonadReader (ServiceEnv), MonadIO, MonadThrow, MonadCatch)

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

data ConfigException
    = ConfigParseException
    | RandomSecretKeyException
    deriving (Eq, Ord, Show)

instance Exception ConfigException

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

conf :: R.Config
conf = def {R.createIfMissing = True, R.errorIfExists = False, R.bloomFilter = True, R.prefixLength = Just 3}

cfStr = ["outputs", "ep_outputs_odd", "ep_outputs_even", "ep_transactions_odd", "ep_transactions_even", "transactions"]

columnFamilies = fmap (\x -> (x, conf)) cfStr

withDBCF path = R.withDBCF path conf columnFamilies

runThreads :: Config.Config -> NC.NodeConfig -> BitcoinP2P -> LG.Logger -> [FilePath] -> IO ()
runThreads config nodeConf bp2p lg certPaths = do
    withDBCF "xdb" $ \rkdb -> do
        cfM <- TSH.fromList 1 $ zip cfStr (R.columnFamilies rkdb)
        let dbh = DatabaseHandles rkdb cfM
        let allegoryEnv = AllegoryEnv $ allegoryVendorSecretKey nodeConf
        let xknEnv = XokenNodeEnv bp2p dbh lg allegoryEnv
        let serviceEnv = ServiceEnv xknEnv
        --
        -- start HTTP endpoint
        let snapConfig =
                Snap.defaultConfig & Snap.setSSLBind (DTE.encodeUtf8 $ DT.pack $ endPointHTTPSListenIP nodeConf) &
                Snap.setSSLPort (fromEnum $ endPointHTTPSListenPort nodeConf) &
                Snap.setSSLKey (certPaths !! 1) &
                Snap.setSSLCert (head certPaths) &
                Snap.setSSLChainCert False
        LA.async $ Snap.serveSnaplet snapConfig (appInit xknEnv)
        --
        -- current node
        let node = vegaNode nodeConf
            normalizedClstr =
                sortBy (\(Node a _ _ _ _) (Node b _ _ _ _) -> compare a b) ([node] ++ vegaCluster nodeConf)
        --
        -- run vegaCluster
        runAppM
            serviceEnv
            (do bp2pEnv <- getBitcoinP2P
                withAsync (startTCPServer (_nodeIPAddr node) (_nodePort node)) $ \y -> do
                    if (_nodeType node == NC.Master)
                        then do
                            withAsync (initializeWorkers node normalizedClstr) $ \_ -> do
                                withAsync setupSeedPeerConnection $ \_ -> do
                                    withAsync runEgressChainSync $ \_ -> do
                                        withAsync runBlockCacheQueue $ \_ -> do
                                            withAsync runPeerSync $ \_ -> do
                                                withAsync runSyncStatusChecker $ \z -> do
                                                    _ <- LA.wait z
                                                    return ()
                        else LA.wait y)
        liftIO $ putStrLn $ "node recovering from fatal DB connection failure!"
    return ()

runSyncStatusChecker :: (HasXokenNodeEnv env m, HasLogger m, MonadIO m) => m ()
runSyncStatusChecker = do
    lg <- getLogger
    bp2pEnv <- getBitcoinP2P
    rkdb <- rocksDB <$> getDB
    let net = bitcoinNetwork $ nodeConfig bp2pEnv
    -- wait 300 seconds before first check
    liftIO $ threadDelay (300 * 1000000)
    forever $ do
        isSynced <- checkBlocksFullySynced net
        LG.debug lg $
            LG.msg $
            LG.val $
            C.pack $
            "Sync Status Checker: All blocks synced? " ++
            if isSynced
                then "Yes"
                else "No"
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

defBitcoinP2P :: NodeConfig -> IO (BitcoinP2P)
defBitcoinP2P nodeCnf = do
    g <- newTVarIO M.empty
    bp <- newTVarIO M.empty
    mv <- newMVar True
    hl <- newMVar True
    st <- TSH.new 1
    tl <- TSH.new 1
    ep <- newTVarIO False
    tc <- TSH.new 1
    vc <- TSH.new 1
    rpf <- newEmptyMVar
    rpc <- newTVarIO 0
    mq <- TSH.new 1
    ts <- TSH.new 1
    tbt <- MS.new $ maxTMTBuilderThreads nodeCnf
    iut <- newTVarIO False
    udc <- H.new
    tpfa <- newTVarIO 0
    ci <- getChainIndex
    tci <- newTVarIO ci
    wrkc <- newTVarIO []
    bsb <- newTVarIO Nothing
    prntxq <- TSH.new 1
    return $ BitcoinP2P nodeCnf g bp mv hl st tl ep tc vc (rpf, rpc) mq ts tbt iut udc tpfa tci wrkc bsb prntxq

initVega :: IO ()
initVega = do
    putStrLn $ "Starting Xoken Nexa"
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

relaunch :: IO ()
relaunch =
    forever $ do
        let pid = "/tmp/vega.pid.1"
        running <- isRunning pid
        if running
            then threadDelay (30 * 1000000)
            else do
                runDetached (Just pid) (ToFile "vega.log") initVega
                threadDelay (5000000)

main :: IO ()
main = do
    initVega
    -- let pid = "/tmp/vega.pid.0"
    -- runDetached (Just pid) (ToFile "vega.log") relaunch
