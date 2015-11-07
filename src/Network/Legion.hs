{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{- |
  Legion is a framework designed to help people implement large-scale
  distributed stateful services that function using a value-space
  partitioning strategy, sometimes known as "sharding". Examples of
  services that rely on value-space partitioning include ElasticSearch,
  Riak, DynamoDB, and others.

  In other words, this framework is an abstraction over partitioning,
  cluster-rebalancing,  node discovery, and request routing, allowing
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
  DiscoverySettings(..),
  MulticastDiscovery(..),
  CustomDiscovery(..),
  -- * Utils
  newMemoryPersistence,
  diskPersistence
) where

import Prelude hiding (lookup, readFile, writeFile, null)

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TVar (newTVar, modifyTVar, readTVar, writeTVar)
import Control.Exception (throw, try, SomeException, catch, evaluate)
import Control.Monad (void, forever, join, (>=>))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Data.Aeson (FromJSON)
import Data.Binary (Binary, encode, decode)
import Data.Bool (bool)
import Data.ByteString (readFile, writeFile)
import Data.ByteString.Lazy (ByteString, toStrict, fromStrict)
import Data.Conduit (Source, Sink, ($$), await, ($=), yield, await)
import Data.Conduit.List (sourceList)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.HexString (hexString, fromBytes, toBytes)
import Data.Map (Map, insert, delete, lookup, singleton, alter)
import Data.Maybe (fromJust)
import Data.Set (Set, fromList)
import Data.UUID.V1 (nextUUID)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr, getAddr))
import Network.Legion.Conduit (merge, chanToSink, chanToSource)
import Network.Legion.Distribution (peerOwns, KeySet, KeyDistribution,
  update, fromRange, findKey, Peer, PartitionKey(K, unkey),
  rebalanceAction, RebalanceAction(Move))
import Network.Legion.MessageId (MessageId)
import Network.Multicast (multicastReceiver, multicastSender)
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), close, connect, HostName,
  PortNumber, Socket)
import Network.Socket.ByteString (recvFrom, sendTo)
import Network.Socket.ByteString.Lazy (sendAll)
import System.Directory (removeFile, doesFileExist, getDirectoryContents)
import qualified Data.ByteString.Char8 as B (pack)
import qualified Data.Conduit.List as CL (map, filter, consume)
import qualified Data.HexString as Hex (toText)
import qualified Data.Map as Map (keys, empty)
import qualified Data.Set as Set (delete, null, member, singleton, insert)
import qualified Data.Text as T (unpack)
import qualified Data.UUID as UUID (toText)
import qualified Network.Legion.Distribution as KD (empty, delete, member)
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
    settings@LegionarySettings {peerBindAddr, discovery}
    startupMode
    requestSource
  = do
    nodeState@NodeState {self} <- makeNodeState legionary startupMode
    infoM ("The initial node state is: " ++ show nodeState)
    let bindAddr = BSockAddr peerBindAddr
    cm <- initConnectionManager self bindAddr
    updateClaimState <- forkClaimProc cm nodeState
    let discS = discoverySource discovery self bindAddr
        peerS = peerMsgSource settings
    (discS `merge` (peerS `merge` requestSource))
      $= CL.map toMessage
      $$ requestSink legionary nodeState cm updateClaimState
  where
    toMessage (Left m) = D m
    toMessage (Right (Left m)) = P m
    toMessage (Right (Right m)) = R m


{- |
  Fork the process that periodically sends out claim messages from this node.
-}
forkClaimProc
  :: ConnectionManager
  -> NodeState response
  -> IO (NodeState response -> IO ())
forkClaimProc cm initState = do
    nodeStateT <- atomically (newTVar initState)
    void . forkIO . forever $ do
      threadDelay fiveSeconds
      NodeState {self, keyspace} <- atomically (readTVar nodeStateT)
      broadcast cm (Claim (peerOwns self keyspace))
    return (atomically . writeTVar nodeStateT)
  where
    fiveSeconds = 5000000 -- in microseconds


{- |
  Figure out how to construct the initial node state.
-}
makeNodeState
  :: Legionary request response
  -> StartupMode
  -> IO (NodeState response)
makeNodeState _ NewCluster = do
  -- Build a brand new node state, for the first node in a cluster.
  self <- UUID.toText . fromJust <$> nextUUID
  infoM ("My node id is: " ++ show self)
  infoM "This is a new cluster."
  return NodeState {
      handoff = Nothing,
      keyspace = update self (fromRange minBound maxBound) KD.empty,
      self,
      forwarded = Map.empty,
      knownPeers = Set.singleton self
    }

makeNodeState l JoinCluster = do
  -- Build a brand new node state, for a fresh node joining the cluster.
  listKeys (persistence l) $$ (do
      msg <- await
      case msg of
        Nothing -> return ()
        Just _ -> error
          $ "This is a new node attempting to join an existing cluster. "
          ++ "Therefore, the persistence layer must be empty or else data "
          ++ "corruption is pretty much garanteed to happen, but `listKeys` "
          ++ "indicates that it is not. Maybe you really want to start up in "
          ++ "node recovery mode."
    )
  self <- UUID.toText . fromJust <$> nextUUID
  infoM ("My node id is: " ++ show self)
  infoM "Trying to join an existing cluster."
  return NodeState {
      handoff = Nothing,
      keyspace = KD.empty,
      self,
      forwarded = Map.empty,
      knownPeers = Set.singleton self
    }


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
  (void . forkIO) $
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


{- |
  This is the mutable state associated with a particular key. In a key/value
  system, this would be the value.
  
  The partition state is represented as an opaque byte string, and it
  is up to the service implementation to make sure that the binary data
  is encoded and decoded into whatever form the service needs.
-}
newtype PartitionState = PartitionState {
    unstate :: ByteString
  }
  deriving (Show, Generic)

instance Binary PartitionState


-- $framework-config
-- The legion framework has several operational parameters which can
-- be controlled using configuration. These include the address binding
-- used to expose the cluster management service endpoint, and details
-- about the automatic node discovery strategy.
--
-- The node discovery strategy dictates how the legion framework goes
-- about finding other nodes on the network with which it can form a
-- cluster. UDP multicast works well if you are running your own hardware,
-- but it is not supported by most cloud environments like Amazon's EC2.


{- |
  Settings used when starting up the legion framework.
-}
data LegionarySettings = LegionarySettings {
    peerBindAddr :: SockAddr,
      -- ^ The address on which the legion framework will listen for
      --   rebalancing and cluster management commands.
    discovery :: DiscoverySettings
  }


{- |
  Configuration of how to discover peers.
-}
data DiscoverySettings
  = Multicast MulticastDiscovery
  | Custom CustomDiscovery


{- |
  Multicast Discovery supports a simple, built-in, discovery mechanism
  based on UDP Multicast operations.
-}
data MulticastDiscovery = MulticastDiscovery {
    multicastHost :: HostName,
    multicastPort :: PortNumber
  } 


{- |
  Not implemented yet. This is a way to allow users to provide their
  own discovery mechanisms. For instance, in EC2 (which disallows udp
  traffic) some people use third party service discovery tools like
  Consul or Eureka.  Integrating with all of these tools is beyond
  the scope of this package, but some integrations may be provided by
  supplemental packages such as @legion-consul@ or @legion-eureka@.
-}
data CustomDiscovery = CustomDiscovery {}


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
  -> ConnectionManager
  -> PeerMessage
  -> IO (NodeState response)

handlePeerMessage -- StoreState
    Legionary {persistence}
    nodeState
    cm
    PeerMessage {source, messageId, payload = StoreState key state}
  = do
    saveState persistence key state
    void $ send cm source (StoreAck messageId)
    return nodeState

handlePeerMessage -- ForwardRequest
    Legionary {handleRequest, persistence}
    nodeState
    cm
    PeerMessage {
        payload = ForwardRequest key request,
        source,
        messageId
      }
  = do
    let respond = void . send cm source . ForwardResponse messageId . encode
    
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
    _cm
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

handlePeerMessage -- Claim
    Legionary {}
    nodeState@NodeState {keyspace, self}
    _cm
    PeerMessage {
        payload = Claim keys,
        source
      }
  = do

    -- don't allow someone else to overwrite our ownership.
    let ours = peerOwns self keyspace
    let newKeyspace = update self ours (update source keys keyspace)
    return nodeState {
        keyspace = newKeyspace
      }

handlePeerMessage -- StoreAck
    Legionary {}
    nodeState@NodeState {
        handoff = Just handoff@HandoffState {
            expectedAcks,
            targetPeer,
            handoffRange
          },
        keyspace
      }
    cm
    PeerMessage {
        payload = StoreAck messageId
      }
  = do
    let newAcks = Set.delete messageId expectedAcks
    if Set.null newAcks
      then do
        (void . send cm targetPeer . Handoff) handoffRange
        return nodeState {
            handoff = Nothing,
            keyspace = update targetPeer handoffRange keyspace
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
    _cm
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

handlePeerMessage -- Handoff
    Legionary {}
    nodeState@NodeState {keyspace, self}
    cm
    PeerMessage {
        payload = Handoff keys
      }
  = do
    broadcast cm (Claim keys)
    return nodeState {
        keyspace = update self keys keyspace
      }
    

{- |
  Defines the local state of a node in the cluster.
-}
data NodeState response = NodeState {
    keyspace :: KeyDistribution,
    handoff :: Maybe HandoffState,
    self :: Peer,
    forwarded :: Map MessageId (response -> IO ()),
    knownPeers :: Set Peer
  }

instance Show (NodeState response) where
  show (NodeState a b c _ d) = "(NodeState " ++ show (a, b, c, d) ++ ")"

{- |
  Defines the state of a handoff.
-}
data HandoffState = HandoffState {
    expectedAcks :: Set MessageId,
    handoffRange :: KeySet,
    targetPeer :: Peer
  } deriving (Show)


{- |
  The type of messages sent to us from other peers.
-}
data PeerMessage = PeerMessage {
    source :: Peer,
    messageId :: MessageId,
    payload :: PeerMessagePayload
  }
  deriving (Generic, Show)

instance Binary PeerMessage


{- |
  The data contained within a peer message.

  TODO: Think about distinguishing the broadcast messages `NewPeer` and
  `Claim`. Those messages are particularly important because they
  are the only ones for which it is necessary that every node eventually
  receive a copy of the message.

  When we get around to implementing durability and data replication,
  the sustained inability to confirm that a node has received one of
  these messages should result in the ejection of that node from the
  cluster and the blacklisting of that node so that it can never re-join.
-}
data PeerMessagePayload
  = StoreState PartitionKey (Maybe PartitionState)
    -- ^ Tell the receiving node to store the key/state information in
    --   its persistence layer in preparation for a key range ownership
    --   handoff. The receiving node should NOT take ownership of this
    --   key, or start fielding user requests for this key.
  | StoreAck MessageId
    -- ^ Acknowledge the successful handling of a `StoreState` message.
  | Handoff KeySet
    -- ^ Tell the receiving node that we would like it to take over the
    --   identified key range, which should have already been transmitted
    --   using a series of `StoreState` messages.
  | Claim KeySet
    -- ^ Announce that the sending node claims the set of keys as its own,
    --   either in response to a `Handoff` message, or else as a general
    --   periodic notification to keep the cluster up to date.
  | ForwardRequest PartitionKey ByteString
    -- ^ Forward a binary encoded user request to the receiving node.
  | ForwardResponse MessageId ByteString
    -- ^ Respond to the forwarded request, identified by MessageId,
    --   with the binary encoded user response.
  deriving (Generic, Show)

instance Binary PeerMessagePayload


{- |
  This is the type of message sent to us by the node discovery mechanism.
-}
data DiscoveryMessage
  = NewPeer Peer BSockAddr
    -- ^ Tell the receiving node that a new peer has shown up in the
    --   cluster.
  deriving (Show, Generic)

instance Binary DiscoveryMessage


{- |
  Construct a source of incoming peer messages.
-}
peerMsgSource :: LegionarySettings -> Source IO PeerMessage
peerMsgSource LegionarySettings {peerBindAddr} = join . lift $
    catch (do
        inputChan <- newChan
        so <- socket (fam peerBindAddr) Stream defaultProtocol
        setSocketOption so ReuseAddr 1
        bindSocket so peerBindAddr
        listen so 5
        (void . forkIO) $ acceptLoop so inputChan
        return (chanToSource inputChan)
      ) (\err -> do
        errorM
          $ "Couldn't start incomming peer message service, because of: "
          ++ show (err :: SomeException)
        -- the following is a cute trick to forward exceptions downstream
        -- using a thunk.
        (return . yield . throw) err
      )
  where
    acceptLoop so inputChan =
        catch (
          forever $ do
            (conn, _) <- accept so
            (void . forkIO . logErrors)
              (sourceSocket conn $= conduitDecode $$ msgSink)
        ) (\err -> do
          errorM $ "error in accept loop: " ++ show (err :: SomeException)
          yield (throw err) $$ msgSink
        )
      where
        msgSink = chanToSink inputChan
        logErrors io = do
          result <- try io
          case result of
            Left err ->
              warningM
                $ "Incomming peer connection crashed because of: "
                ++ show (err :: SomeException)
            Right v -> return v


{- |
  Initialize the node discovery process.
-}
discoverySource
  :: DiscoverySettings
  -> Peer
  -> BSockAddr
  -> Source IO DiscoveryMessage
discoverySource (
      Multicast MulticastDiscovery {multicastHost, multicastPort}
    )
    self
    selfAddy
  = do
    -- "rso" means "receiver socket"
    rso <- lift $ multicastReceiver multicastHost multicastPort
    lift . void . forkIO $ do
      -- "sso" means "sender socket"
      (sso, addr) <- multicastSender multicastHost multicastPort
      let newPeerMsg = toStrict (encode (self, NewPeer self selfAddy))
      forever $ do
        threadDelay 10000000
        sendTo sso newPeerMsg addr
        
    forever $ do
      result <- (lift . try . getMessage) rso
      case result of
        Left err -> warningM 
          $ "Bad message on UDP awareness socket, causing "
          ++ "exception: " ++ show (err :: SomeException)
        -- ignore messages from ourself.
        Right (peer, _) | peer == self -> return ()
        Right (_, msg) -> yield msg
  where
    getMessage :: Socket -> IO (Peer, DiscoveryMessage)
    getMessage so = do
      bytes <- (fromStrict . fst) <$> recvFrom so 50000
      evaluate (decode bytes)

discoverySource (Custom _) _ _ = error "custom discovery not implemented"


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
errorM :: (MonadIO io) => String -> io ()
errorM = liftIO . L.errorM "legion"


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
  -> ConnectionManager
  -> (NodeState response -> IO ())
  -> Sink (Message request response) IO ()
requestSink l nodeState cm updateClaims = do
  debugM ("Node State: " ++ show nodeState)
  nodeState2@NodeState {forwarded, knownPeers}
    <- lift $ rebalance l cm nodeState
  lift $ updateClaims nodeState2
  msg <- await
  debugM ("Receiving: " ++ show msg)
  case msg of
    Just (P PeerMessage {source}) | not (source `Set.member` knownPeers) -> do
      warningM "Dropping message from unknown peer."
      requestSink l nodeState2 cm updateClaims
    Just (P peerMsg) -> do
      newNodeState
        <- lift $ handlePeerMessage l nodeState2 cm peerMsg
      requestSink l newNodeState cm updateClaims
    Just (R ((key, request), respond)) ->
      case findKey key (keyspace nodeState2) of
        Nothing -> do
          errorM
            $ "Keyspace does not contain key: " ++ show key ++ ". This "
            ++ "is a very bad thing and probably means a handoff got "
            ++ "messed up. We are dropping the request on the floor."
          requestSink l nodeState2 cm updateClaims
        Just peer -> do
          mid <- lift $ send cm peer (ForwardRequest key (encode request))
          requestSink
            l
            nodeState2 {forwarded = insert mid respond forwarded}
            cm
            updateClaims
    Just (D discMsg) -> do
      newNodeState <- lift $ handleDiscoveryMessage nodeState2 cm discMsg
      requestSink l newNodeState cm updateClaims
    Nothing ->
      return ()


{- |
  Handle discovery messages
-}
handleDiscoveryMessage
  :: NodeState response
  -> ConnectionManager
  -> DiscoveryMessage
  -> IO (NodeState response)
handleDiscoveryMessage
    nodeState@NodeState {knownPeers}
    cm
    msg@(NewPeer peer addy)
  = do
    debugM ("Received NewPeer: " ++ show msg)
    -- add the peer to the list
    updatePeer cm peer addy
    return nodeState {
        knownPeers = Set.insert peer knownPeers
      }


{- |
  Figure out if any rebalancing actions must be taken by this node, and kick
  them off if so.
-}
rebalance
  :: Legionary request response
  -> ConnectionManager
  -> NodeState response
  -> IO (NodeState response)
-- don't start a handoff if there is one already in progress.
rebalance _ _ ns@NodeState {handoff = Just _} = return ns
rebalance l cm ns@NodeState {self, keyspace} = do
    let action = rebalanceAction self keyspace
    debugM ("The rebalancing action is: " ++ show action)
    case action of
      Nothing -> return ns
      Just (Move targetPeer keys) -> do
        allKeys <- 
          listKeys (persistence l) $= CL.filter (`KD.member` keys) $$ CL.consume
        messageIds <- mapM (sendState (persistence l) targetPeer) allKeys
        case messageIds of 
          [] -> do
            -- There weren't actually any keys to migrate, so short
            -- circuit the handoff.
            (void . send cm targetPeer . Handoff) keys
            return ns {
                keyspace = update targetPeer keys keyspace
              }
          _ -> 
            return ns {
                handoff = Just HandoffState {
                    expectedAcks = fromList messageIds,
                    handoffRange = keys,
                    targetPeer
                  },
                keyspace = KD.delete keys keyspace
              }
  where
    sendState persist peer key = do
      state <- getState persist key
      send cm peer (StoreState key state)


{- |
  Initialize the connection manager based on the node state.
-}
initConnectionManager
  :: Peer
  -> BSockAddr
  -> IO ConnectionManager
initConnectionManager self selfAddy = do
    cmChan <- newChan
    let cmState = CMState {
            cmPeers = singleton self (Nothing, selfAddy),
            nextId = minBound
          }
    (void . forkIO . void) (runManager cmChan cmState)
    return ConnectionManager {cmChan}
  where
    {- |
      Try to send the payload over the socket, and if that fails, then try to
      create a new socket and retry sending the payload. Return whatever the
      "working" socket is.
    -}
    sendWithRetry :: Maybe Socket -> BSockAddr -> ByteString -> IO Socket
    sendWithRetry Nothing addy payload = do
      so <- socket (fam (getAddr addy)) Stream defaultProtocol
      connect so (getAddr addy)
      sendWithRetry (Just so) addy payload

    sendWithRetry (Just so) addy payload = do
      result <- try (sendAll so payload)
      case result of
        Left err -> do
          infoM
            $ "Socket to " ++ show addy ++ " died. Retrying on a new "
            ++ "socket. The error was: " ++ show (err :: SomeException)
          void (try (close so) :: IO (Either SomeException ()))
          so2 <- socket (fam (getAddr addy)) Stream defaultProtocol
          connect so2 (getAddr addy)
          sendAll so2 payload
          return so2
        Right _ ->
          return so

      
    {- |
      This is the function that implements the actual connection manager.
    -}
    runManager :: Chan CMMessage -> CMState -> IO CMState
    runManager chan = (foldr1 (>=>) . repeat) (
        \s@CMState {cmPeers, nextId} -> do
          msg <- readChan chan
          case msg of
            Send peer payload respond -> do
              -- respond with the message id as soon as we know it,
              -- which is immediately
              respond nextId
              newState <- case lookup peer cmPeers of
                Nothing -> do
                  logNoPeer peer payload cmPeers
                  return s
                -- "mso" means Maybe Socket
                Just (mso, addy) -> catch (do
                    let peerMsg = PeerMessage {
                            source = self,
                            messageId = nextId,
                            payload
                          }
                    debugM ("Sending: " ++ show peerMsg)
                    so <- sendWithRetry mso addy (encode peerMsg)
                    return s {
                        cmPeers = insert peer (Just so, addy) cmPeers
                      }
                  ) (\e -> do
                    errorM
                      $ "Some kind of error happend while trying to send a "
                      ++ "message to a peer: " ++ show peer
                      ++ ". The message was: " ++ show payload
                      ++ ". The error was: " ++ show (e :: SomeException)
                      ++ ". The peer table was: " ++ show cmPeers
                    return s {
                        cmPeers = insert peer (Nothing, addy) cmPeers
                      }
                  )
              return newState {nextId = succ nextId}
            UpdatePeer peer addy ->
              -- TODO: `updatePeerEntry` does the right thing in the
              -- case where the peer address didn't change, but in the
              -- case where it did exist and was changed, I'm only hoping
              -- that garbage collected sockets get closed. I'm not too
              -- worried because a peer changing addresses is unlikely
              -- to say the least, and impossible to say the most.
              return s {
                cmPeers = alter (updatePeerEntry addy) peer cmPeers
              }
            Broadcast payload -> do
              let mkSend peer = Send peer payload (const (return ()))
              mapM_ (writeChan chan . mkSend) (Map.keys cmPeers)
              return s
      )

    updatePeerEntry addy val@(Just (_, addy2)) | addy == addy2 = val
    updatePeerEntry addy _ = Just (Nothing, addy)

    logNoPeer peer msg peers_ = errorM
      $ "Trying to send a message to the unknown peer " ++ show peer
      ++ ". Not sure how this can happen. Our internal peer table must "
      ++ "have gotten corrupted somehow. This is a bug. We are dropping "
      ++ "the message on the floor and continuing as if nothing happened. "
      ++ "The message payload was: " ++ show msg ++ ". The peer table is: "
      ++ show peers_


{- |
  A value of this type provides a handle to a connection manager
  instances.
-}
data ConnectionManager =
  ConnectionManager {
    cmChan :: Chan CMMessage
  }


{- |
  This is the internal state of the connection manager.
-}
data CMState =
  CMState {
    cmPeers :: Map Peer (Maybe Socket, BSockAddr),
    nextId :: MessageId
  }


{- |
  This is the type of message understood by the connection manager.
-}
data CMMessage
  = Send Peer PeerMessagePayload (MessageId -> IO ())
  | UpdatePeer Peer BSockAddr
  | Broadcast PeerMessagePayload


{- |
  Sends a peer message using the connection manager. Returns the messageId
  of the sent message.
-}
send :: ConnectionManager -> Peer -> PeerMessagePayload -> IO MessageId
send cm peer payload = do
  mvar <- newEmptyMVar
  writeChan (cmChan cm) (Send peer payload (putMVar mvar))
  takeMVar mvar


{- |
  Update the connection manager with some new peer information.
-}
updatePeer :: ConnectionManager -> Peer -> BSockAddr -> IO ()
updatePeer cm peer addy = writeChan (cmChan cm) (UpdatePeer peer addy)


{- |
  Broadcast a message to all peers.
-}
broadcast :: ConnectionManager -> PeerMessagePayload -> IO ()
broadcast cm payload = writeChan (cmChan cm) (Broadcast payload)


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
  | JoinCluster
    -- ^ Indicates that a new, virgin, empty node has been spun up and
    --   should join an existing cluster. It is extremely important that
    --   nodes being started with this startup mode have a totally empty
    --   persistence layer.
  deriving (Generic)
instance FromJSON StartupMode


{- |
  The different types of messages handled by this process.
-}
data Message request response
  = P PeerMessage
  | R (RequestMsg request response)
  | D DiscoveryMessage

instance Show (Message request response) where
  show (P m) = "(P " ++ show m ++ ")"
  show (R ((p, _), _)) = "(R ((" ++ show p ++ ", _), _))"
  show (D m) = "(D " ++ show m ++ ")"


-- tv str a = trace (str ++ ": " ++ show a) a

