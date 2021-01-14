{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Network.Xoken.Node.P2P.Socket where

import Control.Exception
import Control.Monad.Reader
import Network.Socket as NS
import Network.Xoken.Node.Exception

createSocket :: AddrInfo -> IO (Maybe Socket)
createSocket = createSocketWithOptions []

createSocketWithOptions :: [SocketOption] -> AddrInfo -> IO (Maybe Socket)
createSocketWithOptions options addr = do
    sock <- socket AF_INET Stream (addrProtocol addr)
    mapM_ (\option -> when (isSupportedSocketOption option) (setSocketOption sock option 1)) options
    res <- try $ connect sock (addrAddress addr)
    case res of
        Right () -> return $ Just sock
        Left (e :: IOException) -> do
            liftIO $ NS.close sock
            throw $ SocketConnectException (addrAddress addr)

createSocketFromSockAddr :: SockAddr -> IO (Maybe Socket)
createSocketFromSockAddr saddr = do
    ss <-
        case saddr of
            SockAddrInet pn ha -> socket AF_INET Stream defaultProtocol
            SockAddrInet6 pn6 _ ha6 _ -> socket AF_INET6 Stream defaultProtocol
    rs <- try $ connect ss saddr
    case rs of
        Right () -> return $ Just ss
        Left (e :: IOException) -> do
            liftIO $ NS.close ss
            throw $ SocketConnectException (saddr)
