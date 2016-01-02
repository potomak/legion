{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
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
  Legionary(..),
  Persistence(..),
  RequestMsg,
  -- * Fundamental Types
  PartitionKey(..),
  PartitionState(..),
  -- * Framework Configuration
  -- $framework-config
  LegionarySettings(..),
  -- * Utils
  newMemoryPersistence,
  diskPersistence
) where

import Prelude hiding (lookup, readFile, writeFile, null)

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (newChan, writeChan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TVar (newTVar, modifyTVar, readTVar)
import Control.Exception (throw, try, SomeException, catch)
import Control.Monad (void, forever, join)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary, encode, decode)
import Data.Bool (bool)
import Data.ByteString (readFile, writeFile)
import Data.ByteString.Lazy (toStrict, fromStrict)
import Data.Conduit (Source, Sink, ($$), ($=), yield, await, awaitForever)
import Data.Conduit.List (sourceList)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.HexString (hexString, fromBytes, toBytes)
import Data.Map (Map, insert, delete, lookup, singleton)
import Data.Set (Set, fromList, toDescList, toList)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr), getAddr)
import Network.Legion.ClusterManagement (self, recover, newCluster,
  Cluster, distribution, current, PeerMessage(PeerMessage, source,
  messageId, payload), PeerMessagePayload(StoreState, StoreAck,
  ForwardRequest, ForwardResponse, UpdateCluster), send, forward,
  MessageId, ClusterState, requestJoin, peers, initManager, claim)
import Network.Legion.Conduit (merge, chanToSink, chanToSource)
import Network.Legion.Distribution (KeySet, findPartition, Peer,
  PartitionKey(K, unkey), rebalanceAction, RebalanceAction(Move), Replica,
  PartitionState(PartitionState, unstate))
import Network.Legion.Fork (forkC)
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), connect, getPeerName)
import Network.Socket.ByteString.Lazy (sendAll)
import System.Directory (removeFile, doesFileExist, getDirectoryContents)
import qualified Data.ByteString.Char8 as B (pack)
import qualified Data.Conduit.List as CL (map, filter, consume)
import qualified Data.HexString as Hex (toText)
import qualified Data.Map as Map (keys, empty, member)
import qualified Data.Set as Set (delete, null, member)
import qualified Data.Text as T (unpack)
import qualified Network.Legion.Distribution as KD (member)
import qualified Network.Legion.ClusterManagement as CM (merge)
import qualified System.Log.Logger as L (debugM, warningM, errorM, infoM)

-- $invocation
-- Notes on invocation.

{- |
  Run the legion node framework program, with the given user definitions,
  framework settings, and request source. This function never returns
  (except maybe with an exception if something goes horribly wrong).

  For the vast majority of service implementations, you are going to need
  to implement some halfway complex concurrency in order to populate the
  request source, and to handle the responses. Unless you know exactly
  what you are doing, you probably want to use `forkLegionary` instead.
-}
runLegionary :: (Binary response, Binary request)
  => Legionary request response
    -- ^ The user-defined legion application to run.
  -> LegionarySettings
    -- ^ Settings and configuration of the legionary framework.
  -> StartupMode
  -> Source IO (RequestMsg request response)
    -- ^ A source of requests, together with a way to respond to the requets.
  -> IO ()
runLegionary
    legionary
    settings@LegionarySettings {}
    startupMode
    requestSource
  = do
    peerS <- startPeerListener settings
    nodeState <- makeNodeState settings startupMode
    infoM ("The initial node state is: " ++ show nodeState)
    let joinS = joinMsgSource settings
    (joinS `merge` (peerS `merge` requestSource))
      $= CL.map toMessage
      $$ requestSink legionary nodeState
  where
    toMessage (Left m) = J m
    toMessage (Right (Left m)) = P m
    toMessage (Right (Right m)) = R m


{- |
  Figure out how to construct the initial node state.
-}
makeNodeState
  :: LegionarySettings
  -> StartupMode
  -> IO (NodeState response)
makeNodeState LegionarySettings {journal, peerBindAddr} NewCluster = do
  -- Build a brand new node state, for the first node in a cluster.
  cluster <- newCluster journal peerBindAddr
  cs <- current cluster
  infoM ("This is a new cluster. The new cluster state is: " ++ show cs)
  return NodeState {
      cluster,
      handoff = Nothing,
      forwarded = Map.empty
    }

makeNodeState LegionarySettings {journal, peerBindAddr} (JoinCluster addr) = do
    -- Join a cluster by either starting fresh, or recovering from a
    -- shutdown or crash.
    mj <- recover journal
    infoM "Trying to join an existing cluster."
    clusterState <- case mj of
      Nothing -> joinCluster
      Just lastCS ->
        rejoinCluster lastCS
    cluster <- initManager journal clusterState
    return NodeState {
        cluster,
        handoff = Nothing,
        forwarded = Map.empty
      }
  where
    joinCluster = do
      so <- socket (fam addr) Stream defaultProtocol
      connect so addr
      sendAll so (encode (JoinRequest (BSockAddr peerBindAddr)))
      -- using sourceSocket and conduitDecode is easier than building
      -- a recive/decode state loop, even though we only read a single
      -- response.
      sourceSocket so $= conduitDecode $$ do
        response <- await
        case response of
          Nothing -> error 
            $ "Couldn't join a cluster because there was no response "
            ++ "to our join request!"
          Just (JoinResponse clusterState) ->
            return clusterState
    rejoinCluster = error "rejoinCluster undefined"


{- |
  Forks the legion framework in a background thread, and returns a way to
  send user requests to it and retrieve the responses to those requests.
-}
forkLegionary :: (Binary response, Binary request)
  => Legionary request response
    -- ^ The user-defined legion application to run.
  -> LegionarySettings
    -- ^ Settings and configuration of the legionary framework.
  -> StartupMode
  -> IO (PartitionKey -> request -> IO response)
forkLegionary legionary settings startupMode = do
  chan <- newChan
  forkC "main legion thread" $
    runLegionary legionary settings startupMode (chanToSource chan)
  return (\ key request -> do
      responseVar <- newEmptyMVar
      writeChan chan ((key, request), putMVar responseVar)
      takeMVar responseVar
    )


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
data Legionary request response = Legionary {
    {- |
      The request handler, implemented by the user to service requests.

      Returns a response to the request, together with the new partitoin
      state.
    -}
    handleRequest
      :: PartitionKey
      -> request
      -> Maybe PartitionState
      -> (response, Maybe PartitionState),
    {- |
      The user-defined persistence layer implementation.
    -}
    persistence :: Persistence
  }


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See `newMemoryPersistence` or `diskPersistence`
  if you need to get started quicky.
-}
data Persistence = Persistence {
    getState :: PartitionKey -> IO (Maybe PartitionState),
    saveState :: PartitionKey -> Maybe PartitionState -> IO (),
    listKeys :: Source IO PartitionKey
      -- ^ List all the keys known to the persistence layer. It is
      --   important that the implementation do the right thing with
      --   regard to `Data.Conduit.addCleanup`, because there are
      --   cases where the conduit is terminated without reading the
      --   entire list.
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
type RequestMsg request response = ((PartitionKey, request), response -> IO ())


-- $framework-config
-- The legion framework has several operational parameters which can
-- be controlled using configuration. These include the address binding
-- used to expose the cluster management service endpoint and what file
-- to use for cluster state journaling.

{- |
  Settings used when starting up the legion framework.
-}
data LegionarySettings = LegionarySettings {
    peerBindAddr :: SockAddr,
      -- ^ The address on which the legion framework will listen for
      --   rebalancing and cluster management commands.
    joinBindAddr :: SockAddr,
      -- ^ The address on which the legion framework will listen for
      --   cluster join requests.
    journal :: FilePath
  }


{- |
  A convenient memory-based persistence layer. Good for testing or for
  applications (like caches) that don't have durability requirements.
-}
newMemoryPersistence :: IO Persistence
newMemoryPersistence = do
    cacheT <- atomically (newTVar Map.empty)
    return Persistence {
        getState = fetchState cacheT,
        saveState = saveState_ cacheT,
        listKeys = listKeys_ cacheT
      }
  where
    saveState_ cacheT key (Just state) =
      (atomically . modifyTVar cacheT . insert key) state

    saveState_ cacheT key Nothing =
      (atomically . modifyTVar cacheT . delete) key

    fetchState cacheT key = atomically $
      lookup key <$> readTVar cacheT
    
    listKeys_ cacheT =
      sourceList . Map.keys =<< lift (atomically (readTVar cacheT))


{- |
  A convenient way to persist partition states to disk.
-}
diskPersistence
  :: FilePath
    -- ^ The directory under which partition states will be stored.
  -> Persistence
diskPersistence directory = Persistence {
      getState,
      saveState,
      listKeys
    }
  where
    getState key =
      let path = toPath key in
      doesFileExist path >>= bool
        (return Nothing)
        ((Just . PartitionState . fromStrict) <$> readFile path)

    saveState key (Just state) =
      writeFile (toPath key) (toStrict (unstate state))
    saveState key Nothing =
      let path = toPath key in
      doesFileExist path >>= bool
        (return ())
        (removeFile path)

    listKeys = do
        keys <- lift $ readHexList <$> getDirectoryContents directory
        sourceList keys
      where 
        readHexList = fmap fromHex . filter notSys
        notSys = not . (`elem` [".", ".."])

    {- |
      Convert a key to a path
    -}
    toPath :: PartitionKey -> FilePath
    toPath key = directory ++ "/" ++ toHex key

    {- |
      Convert a partition key to a hexidecimal string.
    -}
    toHex :: PartitionKey -> String
    toHex = T.unpack . Hex.toText . fromBytes . toStrict . encode

    fromHex :: String -> PartitionKey
    fromHex = decode . fromStrict . toBytes . hexString . B.pack


handlePeerMessage :: (Binary response, Binary request)
  => Legionary request response
  -> NodeState response
  -> PeerMessage
  -> IO (NodeState response)

handlePeerMessage -- StoreState
    Legionary {persistence}
    nodeState@NodeState {cluster}
    PeerMessage {source, messageId, payload = StoreState key state}
  = do
    saveState persistence key state
    void $ send cluster source (StoreAck messageId)
    return nodeState

handlePeerMessage -- ForwardRequest
    Legionary {handleRequest, persistence}
    nodeState@NodeState {cluster, handoff}
    msg@PeerMessage {
        payload = ForwardRequest key request,
        source,
        messageId
      }
  | handingOff = do
    owners <- toList . findPartition key <$> distribution cluster
    case filter (/= self cluster) owners of
      [] -> infoM 
        -- TODO queue these messages up for later.
        $ "Dropping message on the floor, because we are handing off "
        ++ "the target key. Message was: " ++ show msg
      peer:_ ->
        forward cluster peer msg
    return nodeState
  | otherwise = do
    owners <- findPartition key <$> distribution cluster
    if self cluster `Set.member` owners
      then do
        let respond = void
              . send cluster source
              . ForwardResponse messageId
              . encode
        
        -- TODO 
        --   - figure out some slick concurrency here, by maintaining
        --       a map of keys that are currently being accessed or
        --       something
        --   - partitioning, balancing, etc.
        -- 
        either (respond . rethrow) respond =<< try (do 
            state <- getState persistence key
            let (response, newState) = handleRequest key (decode request) state
            saveState persistence key newState
            return response
          )
      else case toList owners of
        -- we don't own the key after all, someone was wrong to forward us this
        -- request.
        [] -> errorM
          $ "Can't find any owners for the key: " ++ show key
        peer:_ ->
          forward cluster peer msg
    return nodeState
  where
    handingOff :: Bool
    handingOff = 
      case handoff of
        Just HandoffState {handoffRange} | key `KD.member` handoffRange -> True
        _ -> False
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
    case lookup messageId forwarded of
      Nothing -> warningM
        $  "This peer received a response for a forwarded request that it "
        ++ "didn't send. The only time you might expect to see this is if "
        ++ "this peer recently crashed and was immediately restarted. If "
        ++ "you are seeing this in other circumstances then probably "
        ++ "something is very wrong at the network level. The message was: "
        ++ show msg
      Just respond ->
        respond (decode response)
    return nodeState {
        forwarded = delete messageId forwarded
      }

handlePeerMessage -- StoreAck
    Legionary {persistence = Persistence {listKeys, saveState}}
    nodeState@NodeState {
        cluster,
        handoff = Just handoff@HandoffState {
            expectedAcks,
            targetPeer,
            handoffReplica,
            handoffRange
          }
      }
    PeerMessage {
        payload = StoreAck messageId
      }
  = do
    let newAcks = Set.delete messageId expectedAcks
    if Set.null newAcks
      then do
        claim cluster targetPeer (singleton handoffReplica handoffRange)
        listKeys $= CL.filter (`KD.member` handoffRange)
          $$ awaitForever (lift . (`saveState` Nothing))
        return nodeState {
            handoff = Nothing
          }
      else
        return nodeState {
            handoff = Just handoff {
                expectedAcks = newAcks
              }
          }

handlePeerMessage -- StoreAck
    Legionary {}
    nodeState@NodeState {
        handoff = Nothing
      }
    msg@PeerMessage {
        payload = StoreAck _
      }
  = do
    warningM
      $ "We received an unexpected StoreAck. Probably this is because "
      ++ "this node crashed during a handoff and got restarted, but "
      ++ "there is an outside chance that it means something is very "
      ++ "wrong at the network level. The message was: " ++ show msg
    return nodeState

handlePeerMessage -- UpdateCluster
    Legionary {}
    nodeState@NodeState {
        cluster
      }
    PeerMessage {
        payload = UpdateCluster cs
      }
  = do
    CM.merge cluster cs
    return nodeState
    


{- |
  Defines the local state of a node in the cluster.
-}
data NodeState response = NodeState {
    cluster :: Cluster,
    handoff :: Maybe HandoffState,
    forwarded :: Map MessageId (response -> IO ())
  }

instance Show (NodeState response) where
  show NodeState {handoff} =
    "(NodeState {cluster = _, handoff = " ++ show handoff
    ++ ", forwarded = _})"

{- |
  Defines the state of a handoff.
-}
data HandoffState = HandoffState {
    expectedAcks :: Set MessageId,
    handoffReplica :: Replica,
    handoffRange :: KeySet,
    targetPeer :: Peer
  } deriving (Show)


{- |
  This is the type of a join request message.
-}
data JoinRequest = JoinRequest BSockAddr deriving (Generic, Show)
instance Binary JoinRequest


{- |
  The response to a JoinRequst message
-}
data JoinResponse = JoinResponse ClusterState deriving (Generic)
instance Binary JoinResponse


{- |
  A source of cluster join request messages.
-}
joinMsgSource
  :: LegionarySettings
  -> Source IO (JoinRequest, JoinResponse -> IO ())
joinMsgSource LegionarySettings {joinBindAddr} = join . lift $
    catch (do
        chan <- newChan
        so <- socket (fam joinBindAddr) Stream defaultProtocol
        setSocketOption so ReuseAddr 1
        bindSocket so joinBindAddr
        listen so 5
        forkC "join socket acceptor" $ acceptLoop so chan
        return (chanToSource chan)
      ) (\err -> do
        errorM
          $ "Couldn't start join request service, because of: "
          ++ show (err :: SomeException)
        throw err
      )
  where
    acceptLoop so chan =
        catch (
          forever $ do
            (conn, _) <- accept so
            (void . forkIO . logErrors) (
                sourceSocket conn
                $= conduitDecode
                $= logJoinMessage
                $= attachResponder conn
                $$ chanToSink chan
              )
        ) (\err -> do
          errorM
            $ "error in join request accept loop: "
            ++ show (err :: SomeException)
          throw err
        )
      where
        logJoinMessage = awaitForever $ \msg -> do
          debugM ("Got JoinRequest: " ++ show msg)
          yield msg
          
        logErrors :: IO () -> IO ()
        logErrors io = do
          result <- try io
          case result of
            Left err ->
              warningM
                $ "Incomming join connection crashed because of: "
                ++ show (err :: SomeException)
            Right v -> return v

        attachResponder conn = awaitForever (\msg -> do
            mvar <- liftIO newEmptyMVar
            yield (msg, putMVar mvar)
            response <- liftIO $ takeMVar mvar
            liftIO $ sendAll conn (encode response)
          )


{- |
  Construct a source of incoming peer messages.
  We have to start the peer listener first before we spin up the cluster
  management, which is why this is an @IO (Source IO PeerMessage)@ instead of a
  @Source IO PeerMessage@.
-}
startPeerListener :: LegionarySettings -> IO (Source IO PeerMessage)
startPeerListener LegionarySettings {peerBindAddr} =
    catch (do
        inputChan <- newChan
        so <- socket (fam peerBindAddr) Stream defaultProtocol
        setSocketOption so ReuseAddr 1
        bindSocket so peerBindAddr
        listen so 5
        forkC "peer socket acceptor" $ acceptLoop so inputChan
        return (chanToSource inputChan)
      ) (\err -> do
        errorM
          $ "Couldn't start incomming peer message service, because of: "
          ++ show (err :: SomeException)
        throw err
      )
  where
    acceptLoop so inputChan =
        catch (
          forever $ do
            (conn, _) <- accept so
            remoteAddr <- getPeerName conn
            (void . forkIO . logErrors remoteAddr) (
                sourceSocket conn
                $= conduitDecode
                $$ msgSink
              )
        ) (\err -> do
          errorM
            $ "error in peer message accept loop: "
            ++ show (err :: SomeException)
          throw err
        )
      where
        msgSink = chanToSink inputChan

        logErrors :: SockAddr -> IO () -> IO ()
        logErrors remoteAddr io = do
          result <- try io
          case result of
            Left err ->
              warningM
                $ "Incomming peer connection (" ++ show remoteAddr
                ++ ") crashed because of: " ++ show (err :: SomeException)
            Right v -> return v


{- |
  Guess the family of a `SockAddr`.
-}
fam :: SockAddr -> Family
fam SockAddrInet {} = AF_INET
fam SockAddrInet6 {} = AF_INET6
fam SockAddrUnix {} = AF_UNIX
fam SockAddrCan {} = AF_CAN


{- |
  Shorthand logging.
-}
warningM :: (MonadIO io) => String -> io ()
warningM = liftIO . L.warningM "legion"


{- |
  Shorthand logging.
-}
debugM :: (MonadIO io) => String -> io ()
debugM = liftIO . L.debugM "legion"


{- |
  Shorthand logging.
-}
infoM :: String -> IO ()
infoM = L.infoM "legion"


{- |
  This `Sink` is what actually handles all peer messages and user input.
-}
requestSink :: (Binary response, Binary request)
  => Legionary request response
  -> NodeState response
  -> Sink (Message request response) IO ()
requestSink l nodeState = do
    logNodeState nodeState
    nodeState2@NodeState {forwarded, cluster}
      <- lift $ rebalance l nodeState
    msg <- await
    debugM ("Receiving: " ++ show msg)
    case msg of
      Just (P peerMsg@PeerMessage {source}) ->
        known cluster source >>= bool
          (do
            warningM ("Dropping message from unknown peer: " ++ show source)
            requestSink l nodeState2
          ) (do
            newNodeState <- lift $ handlePeerMessage l nodeState2 peerMsg
            requestSink l newNodeState
          )
      Just (R ((key, request), respond)) -> do
        dist <- liftIO $ distribution cluster
        debugM ("Routing against: " ++ show dist)
        case toDescList (findPartition key dist) of
          [] -> do
            errorM
              $ "Keyspace does not contain key: " ++ show key ++ ". This "
              ++ "is a very bad thing and probably means there is a bug, "
              ++ "or else this node has not joined a cluster yet."
            requestSink l nodeState2
          peer:_ -> do
            mid <- lift $
              send cluster peer (ForwardRequest key (encode request))
            requestSink l nodeState2 {forwarded = insert mid respond forwarded}
      Just (J m) ->
        lift (handleJoinRequest nodeState2 m) >>= requestSink l
        
      Nothing ->
        return ()
  where
    {- |
      Return `True` if the peer is a known peer, false otherwise.
    -}
    known cluster peer = liftIO $ (peer `Map.member`) <$> peers cluster


{- |
  Handle a join request message
-}
handleJoinRequest
  :: NodeState response
  -> (JoinRequest, JoinResponse -> IO ())
  -> IO (NodeState response)
handleJoinRequest
    ns@NodeState {cluster}
    (JoinRequest peerAddr, respond)
  = do
    peerCS <- requestJoin cluster (getAddr peerAddr)
    respond (JoinResponse peerCS)
    return ns


{- |
  Figure out if any rebalancing actions must be taken by this node, and kick
  them off if so.
-}
rebalance
  :: Legionary request response
  -> NodeState response
  -> IO (NodeState response)
-- don't start a handoff if there is one already in progress.
rebalance _ ns@NodeState {handoff = Just _} = return ns
rebalance l ns@NodeState {cluster} = do
    dist <- distribution cluster
    let action = rebalanceAction (self cluster) dist
    debugM ("The rebalancing action is: " ++ show action)
    case action of
      Nothing -> return ns
      Just (Move targetPeer replica keys) -> do
        allKeys <- 
          listKeys (persistence l) $= CL.filter (`KD.member` keys) $$ CL.consume
        messageIds <- mapM (sendState (persistence l) targetPeer) allKeys
        case messageIds of 
          [] -> do
            -- There weren't actually any keys to migrate, so short
            -- circuit the handoff.
            claim cluster targetPeer (singleton replica keys)
            return ns
          _ ->
            return ns {
                handoff = Just HandoffState {
                    expectedAcks = fromList messageIds,
                    handoffReplica = replica,
                    handoffRange = keys,
                    targetPeer
                  }
              }
  where
    sendState persist peer key = do
      state <- getState persist key
      send cluster peer (StoreState key state)


{- |
  This defines the various ways a node can be spun up.
-}
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


{- |
  The different types of messages handled by this process.
-}
data Message request response
  = P PeerMessage
  | R (RequestMsg request response)
  | J (JoinRequest, JoinResponse -> IO ())

instance Show (Message request response) where
  show (P m) = "(P " ++ show m ++ ")"
  show (R ((p, _), _)) = "(R ((" ++ show p ++ ", _), _))"
  show (J (jr, _)) = "(J (" ++ show jr ++ ", _))"


{- |
  Shorthand logging.
-}
errorM :: (MonadIO io) => String -> io ()
errorM = liftIO . L.errorM "legion"


{- |
  A helper function to log the state of the node:
-}
logNodeState :: (MonadIO io) => NodeState response -> io ()
logNodeState ns@NodeState {cluster} = liftIO $ do
  debugM $ "The current node state is: " ++ show ns
  cs <- current cluster
  dist <- distribution cluster
  debugM $ "The current cluster state is: " ++ show cs
  debugM $ "The current distribution state is: " ++ show dist


