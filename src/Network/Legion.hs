{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{- |
  Legion is a framework designed to help people implement large-scale
  distributed stateful services that function using a value-space
  partitioning strategy, sometimes known as "sharding". Examples of
  services that rely on value-space partitioning include ElasticSearch,
  Riak, DynamoDB, and others.

  In other words, this framework is an abstraction over partitioning,
  cluster-rebalancing, node discovery, and request routing, allowing
  the user to focus on request logic and storage strategies.
-}

module Network.Legion (
  -- * Invoking Legion
  -- $invocation
  runLegionary,
  forkLegionary,
  StartupMode(..),
  -- * Service Implementation
  -- $service-implementaiton
  LegionConstraints,
  Legionary(..),
  Persistence(..),
  ApplyDelta(..),
  Bottom(..),
  RequestMsg,
  -- * Fundamental Types
  PartitionKey(..),
  -- * Framework Configuration
  -- $framework-config
  LegionarySettings(..),
  -- * Utils
  newMemoryPersistence,
  diskPersistence
) where

import Prelude hiding (lookup, readFile, writeFile, null)

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Chan (newChan, writeChan, Chan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically, newTVar, modifyTVar, readTVar,
  TVar)
import Control.Exception (throw)
import Control.Monad (void, forever, join, (>=>))
import Control.Monad.Catch (catchAll, try, SomeException, throwM)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger (logDebug, logWarn, logError, logInfo,
  MonadLoggerIO, runLoggingT, askLoggerIO)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary, encode, decode)
import Data.Bool (bool)
import Data.ByteString (readFile, writeFile)
import Data.ByteString.Lazy (toStrict, fromStrict)
import Data.Conduit (Source, Sink, ($$), (=$=), yield, await, awaitForever,
  transPipe, ConduitM)
import Data.Conduit.List (sourceList)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.Either (rights)
import Data.Map (Map, insert, lookup)
import Data.Maybe (fromMaybe)
import Data.Set (member, minView, (\\))
import Data.Text (pack)
import Data.Time.Clock (getCurrentTime)
import Data.UUID (UUID)
import Data.UUID.V1 (nextUUID)
import GHC.Generics (Generic)
import Network.Legion.Admin (runAdmin, AdminMessage(GetState, GetPart))
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.Bottom (Bottom, bottom)
import Network.Legion.ClusterState (ClusterPowerState, claimParticipation,
  ClusterPropState, getPeers, getDistribution)
import Network.Legion.Conduit (merge, chanToSink, chanToSource)
import Network.Legion.ConnectionManager (PeerMessage(PeerMessage,
  source, messageId, payload), PeerMessagePayload(PartitionMerge,
  ForwardRequest, ForwardResponse, ClusterMerge), send, forward, newPeers,
  newConnectionManager)
import Network.Legion.Constraints (LegionConstraints)
import Network.Legion.Distribution (Peer, rebalanceAction,
  RebalanceAction(Invite))
import Network.Legion.Fork (forkC)
import Network.Legion.KeySet (union)
import Network.Legion.LIO (LIO)
import Network.Legion.NodeState (NodeState(NodeState), self, cm, cluster,
  forwarded, propStates, migration, Forwarded(F), unF)
import Network.Legion.PartitionKey (PartitionKey(K, unkey), toHex, fromHex)
import Network.Legion.PartitionState(PartitionPowerState, PartitionPropState)
import Network.Legion.PowerState (ApplyDelta(apply))
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), connect, getPeerName, Socket)
import Network.Socket.ByteString.Lazy (sendAll)
import Network.Wai.Handler.Warp (HostPreference, Port)
import System.Directory (removeFile, doesFileExist, getDirectoryContents)
import qualified Data.Conduit.List as CL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.ClusterState as C
import qualified Network.Legion.KeySet as KS
import qualified Network.Legion.PartitionState as P

-- $invocation
-- Notes on invocation.

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
    -- ^ The user-defined legion application to run.
  -> LegionarySettings
    -- ^ Settings and configuration of the legionary framework.
  -> StartupMode
  -> Source IO (RequestMsg i o)
    -- ^ A source of requests, together with a way to respond to the requets.
  -> LIO ()

runLegionary
    legionary
    settings@LegionarySettings {adminHost, adminPort}
    startupMode
    requestSource
  = do
    peerS <- loggingC =<< startPeerListener settings
    nodeState <- makeNodeState settings startupMode
    $(logInfo) . pack
      $ "The initial node state is: " ++ show nodeState
    adminS <- loggingC =<< runAdmin adminPort adminHost
    joinS <- loggingC (joinMsgSource settings)
    (joinS `merge` (peerS `merge` (requestSource `merge` adminS)))
      =$= CL.map toMessage
      $$  requestSink settings legionary nodeState
  where

    toMessage
      :: Either
          (JoinRequest, JoinResponse -> LIO ())
          (Either
            (PeerMessage i o s)
            (Either (RequestMsg i o) (AdminMessage i o s)))
      -> Message i o s
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


{- | Figure out how to construct the initial node state.  -}
makeNodeState :: (LegionConstraints i o s)
  => LegionarySettings
  -> StartupMode
  -> LIO (NodeState i o s)

makeNodeState LegionarySettings {peerBindAddr} NewCluster = do
  {- Build a brand new node state, for the first node in a cluster. -}
  self <- getUUID
  clusterId <- getUUID
  cm <- newConnectionManager self (Map.singleton self peerBindAddr)
  let
    cluster :: ClusterPropState
    cluster = C.new clusterId self peerBindAddr
  return NodeState {
      self,
      cm,
      cluster,
      forwarded = F Map.empty,
      propStates = Map.empty,
      migration = KS.empty
    }

makeNodeState LegionarySettings {peerBindAddr} (JoinCluster addr) = do
    -- Join a cluster by either starting fresh, or recovering from a
    -- shutdown or crash.
    $(logInfo) "Trying to join an existing cluster."
    (self, clusterPS)
      <- joinCluster (JoinRequest (BSockAddr peerBindAddr))
    let cluster = C.initProp self clusterPS
    cm <- newConnectionManager self (Map.singleton self peerBindAddr)
    newPeers cm (getPeers cluster)
    return NodeState {
        self,
        cluster,
        cm,
        forwarded = F Map.empty,
        propStates = Map.empty,
        migration = KS.empty
      }
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


-- $service-implementaiton
-- The only thing required to implement a legion service is to provide a
-- request handler and a persistence layer. Both of these things have the
-- obvious semantics. The request handler is a function that transforms
-- a request and a possibly non-existent partition state into a response
-- and a new, possibly non-existent, partition state.
-- 
-- The persistence layer provides the framework with a way to store
-- the various partition states. This allows you to choose any number
-- of persistence strategies, including only in memory, on disk, or in
-- some external database.
--
-- See `newMemoryPersistence` and `diskPersistence` if you need to get
-- started quickly with an in-memory persistence layer.


{- |
  This is the type of a user-defined Legion application. Implement this and
  allow the Legion framework to manage your cluster.
-}
data Legionary i o s = Legionary {
    {- |
      The request handler, implemented by the user to service requests.

      Returns a response to the request, together with the new partitoin
      state.
    -}
    handleRequest :: PartitionKey -> i -> s -> o,
    {- |
      The user-defined persistence layer implementation.
    -}
    persistence :: Persistence i s
  }


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See `newMemoryPersistence` or `diskPersistence`
  if you need to get started quicky.
-}
data Persistence i s = Persistence {
     getState :: PartitionKey -> IO (Maybe (PartitionPowerState i s)),
    saveState :: PartitionKey -> Maybe (PartitionPowerState i s) -> IO (),
         list :: Source IO (PartitionKey, PartitionPowerState i s)
      {- ^
        List all the keys known to the persistence layer. It is important
        that the implementation do the right thing with regard to
        `Data.Conduit.addCleanup`, because there are cases where the
        conduit is terminated without reading the entire list.
      -}
  }


{- |
  This is how requests are packaged when they are sent to the legion framework
  for handling. It includes the request information itself, a partition key to
  which the request is directed, and a way for the framework to deliver the
  response to some interested party.

  Unless you know exactly what you are doing, you will have used
  `forkLegionary` instead of `runLegionary` to run the framework, in
  which case you can safely ignore the existence of this type.
-}
type RequestMsg i o = ((PartitionKey, i), o -> IO ())


-- $framework-config
-- The legion framework has several operational parameters which can
-- be controlled using configuration. These include the address binding
-- used to expose the cluster management service endpoint and what file
-- to use for cluster state journaling.

{- | Settings used when starting up the legion framework.  -}
data LegionarySettings = LegionarySettings {
    peerBindAddr :: SockAddr,
      {- ^
        The address on which the legion framework will listen for
        rebalancing and cluster management commands.
      -}
    joinBindAddr :: SockAddr,
      {- ^
        The address on which the legion framework will listen for cluster
        join requests.
      -}
    adminHost :: HostPreference,
      {- ^
        The host address on which the admin service should run.
      -}
    adminPort :: Port
      {- ^
        The host port on which the admin service should run.
      -}
  }


{- |
  A convenient memory-based persistence layer. Good for testing or for
  applications (like caches) that don't have durability requirements.
-}
newMemoryPersistence :: IO (Persistence i s)

newMemoryPersistence = do
    cacheT <- atomically (newTVar Map.empty)
    return Persistence {
        getState = fetchState cacheT,
        saveState = saveState_ cacheT,
        list = list_ cacheT
      }
  where
    saveState_
      :: TVar (Map PartitionKey (PartitionPowerState i s))
      -> PartitionKey
      -> Maybe (PartitionPowerState i s)
      -> IO ()
    saveState_ cacheT key (Just state) =
      (atomically . modifyTVar cacheT . insert key) state

    saveState_ cacheT key Nothing =
      (atomically . modifyTVar cacheT . Map.delete) key

    fetchState
      :: TVar (Map PartitionKey (PartitionPowerState i s))
      -> PartitionKey
      -> IO (Maybe (PartitionPowerState i s))
    fetchState cacheT key = atomically $
      lookup key <$> readTVar cacheT
    
    list_
      :: TVar (Map PartitionKey (PartitionPowerState i s))
      -> Source IO (PartitionKey, PartitionPowerState i s)
    list_ cacheT =
      sourceList . Map.toList =<< lift (atomically (readTVar cacheT))


{- | A convenient way to persist partition states to disk.  -}
diskPersistence :: (Binary i, Binary s)
  => FilePath
    -- ^ The directory under which partition states will be stored.
  -> Persistence i s

diskPersistence directory = Persistence {
      getState,
      saveState,
      list
    }
  where
    getState :: (Binary i, Binary s)
      => PartitionKey
      -> IO (Maybe (PartitionPowerState i s))
    getState key =
      let path = toPath key in
      doesFileExist path >>= bool
        (return Nothing)
        ((Just . decode . fromStrict) <$> readFile path)

    saveState :: (Binary i, Binary s)
      => PartitionKey
      -> Maybe (PartitionPowerState i s)
      -> IO ()
    saveState key (Just state) =
      writeFile (toPath key) (toStrict (encode state))
    saveState key Nothing =
      let path = toPath key in
      doesFileExist path >>= bool
        (return ())
        (removeFile path)

    list :: (Binary i, Binary s)
      => Source IO (PartitionKey, PartitionPowerState i s)
    list = do
        keys <- lift $ readHexList <$> getDirectoryContents directory
        sourceList keys =$= fillData
      where 
        fillData = awaitForever (\key -> do
            let path = toPath key
            state <- lift ((decode . fromStrict) <$> readFile path)
            yield (key, state)
          )
        readHexList = rights . fmap fromHex . filter notSys
        notSys = not . (`elem` [".", ".."])

    {- |
      Convert a key to a path
    -}
    toPath :: PartitionKey -> FilePath
    toPath key = directory ++ "/" ++ toHex key


handlePeerMessage :: (LegionConstraints i o s)
  => Legionary i o s
  -> NodeState i o s
  -> PeerMessage i o s
  -> LIO (NodeState i o s)

handlePeerMessage -- PartitionMerge
    Legionary {
        persistence
      }
    nodeState@NodeState {self, propStates, cluster}
    msg@PeerMessage {
        source,
        payload = PartitionMerge key ps
      }
  = do
    propState <- maybe
      (getStateL persistence self cluster key)
      return
      (lookup key propStates)
    let
      owners = C.findPartition key cluster
    case P.mergeEither source ps propState of
      Left err -> do
        $(logWarn) . pack
          $ "Can't apply incomming partition action message "
          ++ show msg ++ "because of: " ++ show err
        return nodeState
      Right newPropState -> do
        saveStateL persistence key (
            if P.participating newPropState
              then Just (P.getPowerState newPropState)
              else Nothing
          )
        return nodeState {
            propStates = if newPropState == P.new key self owners
              then Map.delete key propStates
              else insert key newPropState propStates
          }

handlePeerMessage -- ForwardRequest
    Legionary {handleRequest, persistence}
    nodeState@NodeState {self, cm, cluster}
    msg@PeerMessage {
        payload = ForwardRequest key request,
        source,
        messageId
      }
  = do
    let owners = C.findPartition key cluster
    if self `member` owners
      then do
        let
          respond = void
            . send cm source
            . ForwardResponse messageId

        -- TODO 
        --   - figure out some slick concurrency here, by maintaining
        --       a map of keys that are currently being accessed or
        --       something
        -- 
        either (respond . rethrow) respond =<< try (do 
            prop <- getStateL persistence self cluster key
            let response = handleRequest key request (P.ask prop)
                newProp = P.delta request prop
            saveStateL persistence key (Just (P.getPowerState newProp))
            $(logInfo) . pack
              $ "Handling user request: " ++ show request
            $(logDebug) . pack
              $ "Request details request: " ++ show prop ++ " ++ "
              ++ show request ++ " --> " ++ show (response, newProp)
            return response
          )
      else
        -- we don't own the key after all, someone was wrong to forward us this
        -- request.
        case minView owners of
          Nothing -> $(logError) . pack
            $ "Can't find any owners for the key: " ++ show key
          Just (peer, _) ->
            forward cm peer msg
    return nodeState
  where
    {- |
      rethrow is just a reification of `throw`.
    -}
    rethrow :: SomeException -> a
    rethrow = throw

handlePeerMessage -- ForwardResponse
    Legionary {}
    nodeState@NodeState {forwarded}
    msg@PeerMessage {
        payload = ForwardResponse messageId response
      }
  = do
    case lookup messageId (unF forwarded) of
      Nothing -> $(logWarn) . pack
        $  "This peer received a response for a forwarded request that it "
        ++ "didn't send. The only time you might expect to see this is if "
        ++ "this peer recently crashed and was immediately restarted. If "
        ++ "you are seeing this in other circumstances then probably "
        ++ "something is very wrong at the network level. The message was: "
        ++ show msg
      Just respond ->
        respond response
    return nodeState {
        forwarded = F . Map.delete messageId . unF $ forwarded
      }

handlePeerMessage -- ClusterMerge
    Legionary {}
    nodeState@NodeState {
        migration,
        cluster
      }
    msg@PeerMessage {
        source,
        payload = ClusterMerge ps
      }
  =
    case C.mergeEither source ps cluster of
      Left err -> do
        $(logWarn) . pack
          $ "Can't apply incomming cluster action message "
          ++ show msg ++ "because of: " ++ show err
        return nodeState
      Right (newCluster, newMigration) ->
        return nodeState {
            migration = migration `union` newMigration,
            cluster = newCluster
          }


{- | This is the type of a join request message.  -}
data JoinRequest = JoinRequest BSockAddr
  deriving (Generic, Show)
instance Binary JoinRequest


{- | The response to a JoinRequst message -}
data JoinResponse
  = JoinOk Peer ClusterPowerState
  | JoinRejected String
  deriving (Generic)
instance Binary JoinResponse


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
                  $$  msgSink
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


{- | Guess the family of a `SockAddr`.  -}
fam :: SockAddr -> Family
fam SockAddrInet {} = AF_INET
fam SockAddrInet6 {} = AF_INET6
fam SockAddrUnix {} = AF_UNIX
fam SockAddrCan {} = AF_CAN


{- | This `Sink` is what actually handles all peer messages and user input.  -}
requestSink :: (LegionConstraints i o s)
  => LegionarySettings
  -> Legionary i o s
  -> NodeState i o s
  -> Sink (Message i o s) LIO ()

requestSink s l n = awaitForever $
    lift . handleMessage l n
    >=> lift . heartbeat
    >=> lift . migrate l
    >=> lift . propagate
    >=> lift . rebalance l
    >=> logState
    >=> requestSink s l
  where
    logState n2 = lift $ logNodeState n2 >> return n2


{- |
  Migrate partitions based on new cluster state information.

  TODO: this migration algorithm is super naive. It just gos ahead and migrates
  everything in one pass, which is going to be terrible for performance.
-}
migrate :: (LegionConstraints i o s)
  => Legionary i o s
  -> NodeState i o s
  -> LIO (NodeState i o s)
migrate
    Legionary{persistence}
    nodeState@NodeState {self, cluster, propStates}
  =
    listL persistence $$ accum nodeState {migration = KS.empty}
  where
    accum ns = await >>= \case
      Nothing -> return ns
      Just (key, ps) -> 
        let
          origProp = fromMaybe (P.initProp self ps) (lookup key propStates)
          newPeers_ = C.findPartition key cluster \\ P.projParticipants origProp
          newProp = foldr P.participate origProp (Set.toList newPeers_)
        in do
          lift (saveStateL persistence key (Just (P.getPowerState newProp)))
          accum ns {
              propStates = Map.insert key newProp propStates
            }


{- | Update all of the propagation states with the current time.  -}
heartbeat :: NodeState i o s -> LIO (NodeState i o s)
heartbeat ns@NodeState {cluster, propStates} = do
  now <- lift getCurrentTime
  return ns {
      cluster = C.heartbeat now cluster,
      propStates = Map.fromAscList [
          (k, P.heartbeat now p)
          | (k, p) <- Map.toAscList propStates
        ]
    }


{- |
  Handle all cluster and partition state propagation actions, and return
  an updated node state.
-}
propagate :: (LegionConstraints i o s)
  => NodeState i o s
  -> LIO (NodeState i o s)
propagate ns@NodeState {cm, cluster, propStates, self} = do
    let (peers, ps, cluster2) = C.actions cluster
    $(logDebug) . pack $ "Cluster Actions: " ++ show (peers, ps)
    mapM_ (doClusterAction ps) (Set.toList peers)
    propStates2 <- mapM doPartitionActions (Map.toList propStates)
    return ns {
        cluster = cluster2,
        propStates = Map.fromAscList [
            (k, p)
            | (k, p) <- propStates2
            , p /= P.initProp self (P.getPowerState p)
          ]
      }
  where
    doClusterAction ps peer =
      send cm peer (ClusterMerge ps)

    doPartitionActions (key, propState) = do
        let (peers, ps, propState2) = P.actions propState
        mapM_ (perform ps) (Set.toList peers)
        return (key, propState2)
      where
        perform ps peer =
          send cm peer (PartitionMerge key ps)


{- | Handle one incomming message.  -}
handleMessage :: (LegionConstraints i o s)
  => Legionary i o s
  -> NodeState i o s
  -> Message i o s
  -> LIO (NodeState i o s)

handleMessage l nodeState@NodeState {cm, forwarded, cluster} msg = do
    $(logDebug) . pack $ "Receiving: " ++ show msg
    case msg of
      P peerMsg@PeerMessage {source} ->
        if known source
          then handlePeerMessage l nodeState peerMsg
          else do
            $(logWarn) . pack
              $ "Dropping message from unknown peer: " ++ show source
            return nodeState
      R ((key, request), respond) ->
        case minView (C.findPartition key cluster) of
          Nothing -> do
            $(logError) . pack
              $ "Keyspace does not contain key: " ++ show key ++ ". This "
              ++ "is a very bad thing and probably means there is a bug, "
              ++ "or else this node has not joined a cluster yet."
            return nodeState
          Just (peer, _) -> do
            mid <- send cm peer (ForwardRequest key request)
            return nodeState {
                forwarded = F . insert mid (lift . respond) . unF $ forwarded
              }
      J m -> handleJoinRequest nodeState m
      A m -> handleAdminMessage l nodeState m
  where
    {- |
      Return `True` if the peer is a known peer, false otherwise.
    -}
    known peer = peer `member` C.allParticipants cluster


{- |
  Handle a message from the admin service.
-}
handleAdminMessage
  :: Legionary i o s
  -> NodeState i o s
  -> AdminMessage i o s
  -> LIO (NodeState i o s)
handleAdminMessage _ ns (GetState respond) =
  respond ns >> return ns
handleAdminMessage Legionary {persistence} ns (GetPart key respond) = do
  partitionVal <- lift (getState persistence key)
  respond partitionVal
  return ns


{- | Handle a join request message -}
handleJoinRequest
  :: NodeState i o s
  -> (JoinRequest, JoinResponse -> LIO ())
  -> LIO (NodeState i o s)

handleJoinRequest
    ns@NodeState {cluster, cm}
    (JoinRequest peerAddr, respond)
  = do
    peer <- getUUID
    let newCluster = C.joinCluster peer peerAddr cluster
    newPeers cm (getPeers newCluster)
    respond (JoinOk peer (C.getPowerState newCluster))
    return ns {cluster = newCluster}


{- |
  Figure out if any rebalancing actions must be taken by this node, and kick
  them off if so.
-}
rebalance :: (LegionConstraints i o s)
  => Legionary i o s
  -> NodeState i o s
  -> LIO (NodeState i o s)

rebalance _ ns@NodeState {self, cluster} = do
    let action = rebalanceAction self allPeers dist
    $(logDebug) . pack $ "The rebalance action is: " ++ show action
    return ns {
        cluster = case action of
          Nothing -> cluster
          Just (Invite ks) -> claimParticipation self ks cluster
      }
  where
    allPeers = (Set.fromList . Map.keys . getPeers) cluster
    dist = getDistribution cluster


{- | This defines the various ways a node can be spun up.  -}
data StartupMode
  = NewCluster
    -- ^ Indicates that we should bootstrap a new cluster at startup. The
    --   persistence layer may be safely pre-populated because the new
    --   node will claim the entire keyspace. Future plans include
    --   implementing some safeguards to make sure only one node in
    --   a cluster was started using this startup mode, but for now,
    --   we are counting on you, the user, to do the right thing.
  | JoinCluster SockAddr
    -- ^ Indicates that the node should try to join an existing cluster,
    --   either by starting fresh, or by recovering from a shutdown
    --   or crash.
  deriving (Show, Eq)


{- | The different types of messages handled by this process.  -}
data Message i o s
  = P (PeerMessage i o s)
  | R (RequestMsg i o)
  | J (JoinRequest, JoinResponse -> LIO ())
  | A (AdminMessage i o s)

instance (Show i, Show o, Show s) => Show (Message i o s) where
  show (P m) = "(P " ++ show m ++ ")"
  show (R ((p, _), _)) = "(R ((" ++ show p ++ ", _), _))"
  show (J (jr, _)) = "(J (" ++ show jr ++ ", _))"
  show (A a) = "(A (" ++ show a ++ "))"


{- | A helper function to log the state of the node: -}
logNodeState :: (LegionConstraints i o s) => NodeState i o s -> LIO ()
logNodeState ns = $(logDebug) . pack
    $ "The current node state is: " ++ show ns


{- | A utility function that makes a UUID, no matter what.  -}
getUUID :: (MonadIO io) => io UUID

getUUID = liftIO nextUUID >>= maybe (wait >> getUUID) return
  where
    wait = liftIO (threadDelay oneMillisecond)
    oneMillisecond = 1000


{- | Like `getState`, but in LIO, and provides the correct bottom value.  -}
getStateL :: (ApplyDelta i s, Bottom s)
  => Persistence i s
  -> Peer
  -> ClusterPropState
  -> PartitionKey
  -> LIO (PartitionPropState i s)

getStateL p self cluster key =
  {- dp == default participants -}
  let dp = C.findPartition key cluster
  in maybe
      (P.new key self dp)
      (P.initProp self)
      <$> lift (getState p key)


{- | Like `saveState`, but in LIO.  -}
saveStateL
  :: Persistence i s
  -> PartitionKey
  -> Maybe (PartitionPowerState i s) -> LIO ()
saveStateL p k = lift . saveState p k


{- | Like `list`, but in LIO.  -}
listL :: Persistence i s -> Source LIO (PartitionKey, PartitionPowerState i s)
listL p = transPipe lift (list p)


