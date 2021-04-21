{-# LANGUAGE OverloadedStrings #-}

module Network.Xoken.Node.HTTP.Server where

import qualified Data.ByteString as B
import Network.Xoken.Node.Env
import Network.Xoken.Node.HTTP.Handler
import Network.Xoken.Node.HTTP.Types
import Snap

appInit :: XokenNodeEnv -> SnapletInit App App
appInit env =
    makeSnaplet "v1" "API's" Nothing $ do
        addRoutes apiRoutes
        return $ App env

apiRoutes :: [(B.ByteString, Handler App App ())]
apiRoutes = [("/v1/mapi/tx", method POST (withReq submitTransaction'))]
