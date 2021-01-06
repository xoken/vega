{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

--
module Network.Xoken.Node.AriviService where-- globalHandlerRpc :: (HasService env m) => RPCMessage -> m (Maybe RPCMessage)
-- globalHandlerRpc msg = do
--     bp2pEnv <- getBitcoinP2P
--     let net = bitcoinNetwork $ nodeConfig bp2pEnv
--     liftIO $ printf "Decoded resp: %s\n" (show msg)
--     st <- goGetResource msg net [] "" True
--     return (Just $ st)
-- globalHandlerPubSub :: (HasService env m) => ServiceTopic -> PubNotifyMessage -> m Status
-- globalHandlerPubSub tpc msg = undefined
-- instance HasNetworkConfig (ServiceEnv m r t rmsg pmsg) NetworkConfig where
--     networkConfig f se =
--         fmap
--             (\nc ->
--                  se
--                      { p2pEnv =
--                            (p2pEnv se)
--                                {nodeEndpointEnv = (nodeEndpointEnv (p2pEnv se)) {Arivi.P2P.P2PEnv._networkConfig = nc}}
--                      })
--             (f ((Arivi.P2P.P2PEnv._networkConfig . nodeEndpointEnv . p2pEnv) se))
-- instance HasTopics (ServiceEnv m r t rmsg pmsg) t where
--     topics = pubSubTopics . psEnv . p2pEnv
-- instance HasSubscribers (ServiceEnv m r t rmsg pmsg) t where
--     subscribers = pubSubSubscribers . psEnv . p2pEnv
-- instance HasNotifiers (ServiceEnv m r t rmsg pmsg) t where
--     notifiers = pubSubNotifiers . psEnv . p2pEnv
-- instance HasPubSubEnv (ServiceEnv m r t rmsg pmsg) t where
--     pubSubEnv = psEnv . p2pEnv
-- instance HasRpcEnv (ServiceEnv m r t rmsg pmsg) r rmsg where
--     rpcEnv = rEnv . p2pEnv
-- instance HasPSGlobalHandler (ServiceEnv m r t rmsg pmsg) m r t rmsg pmsg where
--     psGlobalHandler = psHandler . p2pEnv
-- instance HasRpcGlobalHandler (ServiceEnv m r t rmsg pmsg) m r t rmsg pmsg where
--     rpcGlobalHandler = rHandler . p2pEnv
