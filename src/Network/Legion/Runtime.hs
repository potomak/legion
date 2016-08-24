{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  This module is responsible for the runtime operation of the legion
  framework. This mostly means opening sockets and piping data around to the
  various connected pieces.
-}
module Network.Legion.Runtime (
  forkLegionary,
  runLegionary,
  StartupMode(..),
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (writeChan, newChan, Chan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Monad (void, forever, join)
import Control.Monad.Catch (catchAll, try, SomeException, throwM)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (logWarn, logError, logInfo, LoggingT,
  MonadLoggerIO, runLoggingT, askLoggerIO)
import Control.Monad.Trans.Class (lift)
import Data.Binary (encode)
import Data.Conduit (Source, ($$), (=$=), yield, await, awaitForever,
  transPipe, ConduitM, runConduit)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.Map (Map)
import Data.Text (pack)
import Network.Legion.Admin (runAdmin)
import Network.Legion.Application (LegionConstraints, Legionary,
  RequestMsg)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.ClusterState (ClusterPowerState)
import Network.Legion.Conduit (merge, chanToSink, chanToSource)
import Network.Legion.ConnectionManager (newConnectionManager, send,
  newPeers)
import Network.Legion.Distribution (Peer, newPeer)
import Network.Legion.Fork (forkC)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.Settings (LegionarySettings(LegionarySettings,
  adminHost, adminPort, peerBindAddr, joinBindAddr))
import Network.Legion.StateMachine (stateMachine, LInput(J, P, R,
  A), JoinRequest(JoinRequest), JoinResponse(JoinOk, JoinRejected),
  LOutput(Send, NewPeers), AdminMessage, NodeState, PeerMessage,
  newNodeState)
import Network.Legion.UUID (getUUID)
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), connect, getPeerName, Socket)
import Network.Socket.ByteString.Lazy (sendAll)
import qualified Data.Conduit.List as CL
import qualified Network.Legion.ClusterState as C


{- |
  Run the legion node framework program, with the given user definitions,
  framework settings, and request source. This function never returns
  (except maybe with an exception if something goes horribly wrong).

  For the vast majority of service implementations, you are going to need
  to implement some halfway complex concurrency in order to populate the
  request source, and to handle the responses. Unless you know exactly
  what you are doing, you probably want to use `forkLegionary` instead.
-}
runLegionary :: (LegionConstraints i o s)
  => Legionary i o s
    {- ^ The user-defined legion application to run.  -}
  -> LegionarySettings
    {- ^ Settings and configuration of the legionary framework.  -}
  -> StartupMode
  -> Source IO (RequestMsg i o)
    {- ^ A source of requests, together with a way to respond to the requets. -}
  -> LoggingT IO ()
    {-
      Don't expose 'LIO' here because 'LIO' is a strictly internal
      symbol. 'LoggingT IO' is what we expose to the world.
    -}

runLegionary
    legionary
    settings@LegionarySettings {adminHost, adminPort}
    startupMode
    requestSource
  = do
    peerS <- loggingC =<< startPeerListener settings
    (nodeState, peers) <- makeNodeState settings startupMode
    cm <- newConnectionManager peers
    $(logInfo) . pack
      $ "The initial node state is: " ++ show nodeState
    adminS <- loggingC =<< runAdmin adminPort adminHost
    joinS <- loggingC (joinMsgSource settings)
    runConduit $
      (joinS `merge` (peerS `merge` (requestSource `merge` adminS)))
        =$= CL.map toMessage
        =$= stateMachine legionary nodeState
        =$= handleOutput cm
  where
    handleOutput cm = awaitForever (lift . \case
        Send peer message -> send cm peer message
        NewPeers peers -> newPeers cm peers
      )

    toMessage
      :: Either
          (JoinRequest, JoinResponse -> LIO ())
          (Either
            (PeerMessage i o s)
            (Either (RequestMsg i o) (AdminMessage i o s)))
      -> LInput i o s
    toMessage (Left m) = J m
    toMessage (Right (Left m)) = P m
    toMessage (Right (Right (Left m))) = R m
    toMessage (Right (Right (Right m))) = A m

    {- |
      Turn an LIO-based conduit into an IO-based conduit, so that it
      will work with `merge`.
    -}
    loggingC :: ConduitM i o LIO r -> LIO (ConduitM i o IO r)
    loggingC c = do
      logging <- askLoggerIO
      return (transPipe (`runLoggingT` logging) c)


{- | This defines the various ways a node can be spun up. -}
data StartupMode
  = NewCluster
    {- ^
      Indicates that we should bootstrap a new cluster at startup. The
      persistence layer may be safely pre-populated because the new node
      will claim the entire keyspace.
    -}
  | JoinCluster SockAddr
    {- ^
      Indicates that the node should try to join an existing cluster,
      either by starting fresh, or by recovering from a shutdown or crash.
    -}
  deriving (Show, Eq)


{- |
  Construct a source of incoming peer messages.  We have to start the
  peer listener first before we spin up the cluster management, which
  is why this is an @LIO (Source LIO PeerMessage)@ instead of a
  @Source LIO PeerMessage@.
-}
startPeerListener :: (LegionConstraints i o s)
  => LegionarySettings
  -> LIO (Source LIO (PeerMessage i o s))

startPeerListener LegionarySettings {peerBindAddr} =
    catchAll (do
        (inputChan, so) <- lift $ do
          inputChan <- newChan
          so <- socket (fam peerBindAddr) Stream defaultProtocol
          setSocketOption so ReuseAddr 1
          bindSocket so peerBindAddr
          listen so 5
          return (inputChan, so)
        forkC "peer socket acceptor" $ acceptLoop so inputChan
        return (chanToSource inputChan)
      ) (\err -> do
        $(logError) . pack
          $ "Couldn't start incomming peer message service, because of: "
          ++ show (err :: SomeException)
        throwM err
      )
  where
    acceptLoop :: (LegionConstraints i o s)
      => Socket
      -> Chan (PeerMessage i o s)
      -> LIO ()
    acceptLoop so inputChan =
        catchAll (
          forever $ do
            (conn, _) <- lift $ accept so
            remoteAddr <- lift $ getPeerName conn
            logging <- askLoggerIO
            let runSocket =
                  sourceSocket conn
                  =$= conduitDecode
                  $$ msgSink
            void
              . lift
              . forkIO
              . (`runLoggingT` logging)
              . logErrors remoteAddr
              $ runSocket
        ) (\err -> do
          $(logError) . pack
            $ "error in peer message accept loop: "
            ++ show (err :: SomeException)
          throwM err
        )
      where
        msgSink = chanToSink inputChan

        logErrors :: SockAddr -> LIO () -> LIO ()
        logErrors remoteAddr io = do
          result <- try io
          case result of
            Left err ->
              $(logWarn) . pack
                $ "Incomming peer connection (" ++ show remoteAddr
                ++ ") crashed because of: " ++ show (err :: SomeException)
            Right v -> return v


{- | Figure out how to construct the initial node state.  -}
makeNodeState :: (LegionConstraints i o s)
  => LegionarySettings
  -> StartupMode
  -> LIO (NodeState i o s, Map Peer BSockAddr)

makeNodeState LegionarySettings {peerBindAddr} NewCluster = do
  {- Build a brand new node state, for the first node in a cluster. -}
  self <- newPeer
  clusterId <- getUUID
  let
    cluster = C.new clusterId self peerBindAddr
  nodeState <- newNodeState self cluster
  return (nodeState, C.getPeers cluster)

makeNodeState LegionarySettings {peerBindAddr} (JoinCluster addr) = do
    {-
      Join a cluster by either starting fresh, or recovering from a
      shutdown or crash.
    -}
    $(logInfo) "Trying to join an existing cluster."
    (self, clusterPS) <- joinCluster (JoinRequest (BSockAddr peerBindAddr))
    let
      cluster = C.initProp self clusterPS
    nodeState <- newNodeState self cluster
    return (nodeState, C.getPeers cluster)
  where
    joinCluster :: JoinRequest -> LIO (Peer, ClusterPowerState)
    joinCluster joinMsg = liftIO $ do
      so <- socket (fam addr) Stream defaultProtocol
      connect so addr
      sendAll so (encode joinMsg)
      {-
        using sourceSocket and conduitDecode is easier than building
        a recive/decode state loop, even though we only read a single
        response.
      -}
      sourceSocket so =$= conduitDecode $$ do
        response <- await
        case response of
          Nothing -> fail 
            $ "Couldn't join a cluster because there was no response "
            ++ "to our join request!"
          Just (JoinOk self cps) ->
            return (self, cps)
          Just (JoinRejected reason) -> fail
            $ "The cluster would not allow us to re-join. "
            ++ "The reason given was: " ++ show reason


{- | A source of cluster join request messages.  -}
joinMsgSource
  :: LegionarySettings
  -> Source LIO (JoinRequest, JoinResponse -> LIO ())

joinMsgSource LegionarySettings {joinBindAddr} = join . lift $
    catchAll (do
        (chan, so) <- lift $ do
          chan <- newChan
          so <- socket (fam joinBindAddr) Stream defaultProtocol
          setSocketOption so ReuseAddr 1
          bindSocket so joinBindAddr
          listen so 5
          return (chan, so)
        forkC "join socket acceptor" $ acceptLoop so chan
        return (chanToSource chan)
      ) (\err -> do
        $(logError) . pack
          $ "Couldn't start join request service, because of: "
          ++ show (err :: SomeException)
        throwM err
      )
  where
    acceptLoop :: Socket -> Chan (JoinRequest, JoinResponse -> LIO ()) -> LIO ()
    acceptLoop so chan =
        catchAll (
          forever $ do
            (conn, _) <- lift $ accept so
            logging <- askLoggerIO
            (void . lift . forkIO . (`runLoggingT` logging) . logErrors) (
                sourceSocket conn
                =$= conduitDecode
                =$= attachResponder conn
                $$  chanToSink chan
              )
        ) (\err -> do
          $(logError) . pack
            $ "error in join request accept loop: "
            ++ show (err :: SomeException)
          throwM err
        )
      where
        logErrors :: LIO () -> LIO ()
        logErrors io = do
          result <- try io
          case result of
            Left err ->
              $(logWarn) . pack
                $ "Incomming join connection crashed because of: "
                ++ show (err :: SomeException)
            Right v -> return v

        attachResponder
          :: Socket
          -> ConduitM JoinRequest (JoinRequest, JoinResponse -> LIO ()) LIO ()
        attachResponder conn = awaitForever (\msg -> do
            mvar <- liftIO newEmptyMVar
            yield (msg, lift . putMVar mvar)
            response <- liftIO $ takeMVar mvar
            liftIO $ sendAll conn (encode response)
          )


{- | Guess the family of a `SockAddr`. -}
fam :: SockAddr -> Family
fam SockAddrInet {} = AF_INET
fam SockAddrInet6 {} = AF_INET6
fam SockAddrUnix {} = AF_UNIX
fam SockAddrCan {} = AF_CAN


{- |
  Forks the legion framework in a background thread, and returns a way to
  send user requests to it and retrieve the responses to those requests.
-}
forkLegionary :: (LegionConstraints i o s, MonadLoggerIO io)
  => Legionary i o s
    {- ^ The user-defined legion application to run. -}
  -> LegionarySettings
    {- ^ Settings and configuration of the legionary framework. -}
  -> StartupMode
  -> io (PartitionKey -> i -> IO o)

forkLegionary legionary settings startupMode = do
  logging <- askLoggerIO
  liftIO . (`runLoggingT` logging) $ do
    chan <- liftIO newChan
    forkC "main legion thread" $
      runLegionary legionary settings startupMode (chanToSource chan)
    return (\ key request -> do
        responseVar <- newEmptyMVar
        writeChan chan ((key, request), putMVar responseVar)
        takeMVar responseVar
      )


