{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{- |
  Legion is a framework designed to help people implement large-scale
  distributed services that function using a value-space partitioning
  strategy, sometimes known as "sharding". Examples of services that
  rely on value-space partitioning include ElasticSearch, Riak, DynamoDB,
  and others.

  In other words, this framework is an abstraction over partitioning,
  cluster-rebalancing,  node discovery, and request routing, allowing
  the user to focus on request logic and storage strategies.

  In its current alpha state, this framework does not provide data
  replication, but future milestones do include that goal.
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
  AddressDescription,
  DiscoverySettings(..),
  ConfigDiscovery,
  MulticastDiscovery(..),
  CustomDiscovery(..),
  -- * Utils
  newMemoryPersistence,
  diskPersistence
) where

import Prelude hiding (lookup, mapM, readFile, writeFile)

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TVar (newTVar, modifyTVar, readTVar)
import Control.Exception (throw, try, SomeException, catch)
import Control.Monad (void, forever, join, (>=>))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary(put, get), encode, decode)
import Data.Bool (bool)
import Data.ByteString.Lazy (ByteString, readFile, writeFile, toStrict,
  fromStrict)
import Data.Conduit (Source, Sink, ($$), await, ($=), yield, await)
import Data.Conduit.List (sourceList)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.HexString (hexString, fromBytes, toBytes)
import Data.List.Split (splitOn)
import Data.Map (Map, empty, insert, delete, lookup, singleton)
import Data.Maybe (fromJust)
import Data.Text (Text)
import Data.UUID.V1 (nextUUID)
import Data.Word (Word8, Word64)
import GHC.Generics (Generic)
import Network.Legion.Distribution (peerOwns, KeySet, KeyDistribution,
  update, fromRange, findKey, Peer, PartitionKey(K, unkey))
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bindSocket,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), addrAddress, getAddrInfo,
  close, connect)
import Network.Socket.ByteString.Lazy (sendAll)
import System.Directory (removeFile, doesFileExist, getDirectoryContents)
import qualified Data.ByteString.Char8 as B (pack)
import qualified Data.Conduit.List as CL (map)
import qualified Data.HexString as Hex (toText)
import qualified Data.Map as Map (keys)
import qualified Data.Text as T (unpack)
import qualified Data.UUID as UUID (toText)
import qualified Network.Legion.Distribution as KD (empty)
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
runLegionary legionary settings NewCluster requestSource = do
    nodeState <- makeNewFirstNode
    bindAddr <- BSockAddr <$> resolveAddr (peerBindAddr settings)
    cm <- initConnectionManager (self nodeState) bindAddr
    (peerMsgSource settings `merge` requestSource) $= CL.map toMessage
      $$ requestSink legionary nodeState empty cm
  where
    toMessage (Left m) = P m
    toMessage (Right m) = R m

{- |
  Build a brand new node state, for the first node in a cluster.
-}
makeNewFirstNode :: (MonadIO io) => io NodeState
makeNewFirstNode = liftIO $ do
  self <- UUID.toText . fromJust <$> nextUUID
  infoM ("My node id is: " ++ show self)
  return NodeState {
      keyspace = update self (fromRange minBound maxBound) KD.empty,
      self
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
    peerBindAddr :: AddressDescription,
      -- ^ The address on which the legion framework will listen for
      --   rebalancing and cluster management commands.
    discovery :: DiscoverySettings
  }


{- |
  An address description is really just an synonym for a formatted string.

  The only currently supported address address family is: @ipv4@

  Examples: @"ipv4:0.0.0.0:8080"@, @"ipv4:www.google.com:80"@,
-}
type AddressDescription = String


{- |
  Configuration of how to discover peers.
-}
data DiscoverySettings
  = Config ConfigDiscovery
  | Multicast MulticastDiscovery
  | Custom CustomDiscovery


{- |
  Peer discovery based on static configuration.
-}
type ConfigDiscovery = Map Peer AddressDescription


{- |
  Not implemented yet.
-}
data MulticastDiscovery = MulticastDiscovery {} 


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
    cacheT <- atomically (newTVar empty)
    return Persistence {
        getState = fetchState cacheT,
        saveState = saveState_ cacheT,
        listKeys = return ()
      }
  where
    saveState_ cacheT key (Just state) =
      (atomically . modifyTVar cacheT . insert key) state

    saveState_ cacheT key Nothing =
      (atomically . modifyTVar cacheT . delete) key

    fetchState cacheT key = atomically $
      lookup key <$> readTVar cacheT


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
        ((Just . PartitionState) <$> readFile path)
        (return Nothing)

    saveState key (Just state) = writeFile (toPath key) (unstate state)
    saveState key Nothing = removeFile (toPath key)

    listKeys = do
      keys <- lift $ fmap fromHex <$> getDirectoryContents directory
      sourceList keys

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
  -> NodeState
  -> Map MessageId (response -> IO ())
  -> ConnectionManager
  -> PeerMessage
  -> IO NodeState

handlePeerMessage -- StoreState
    Legionary {persistence}
    nodeState
    _forwarded
    cm
    msg@PeerMessage {source, messageId, payload = StoreState key state}
  = do
    debugM ("Received StoreState: " ++ show msg)
    saveState persistence key (Just state)
    void $ send cm source (StoreAck messageId)
    return nodeState

handlePeerMessage -- NewPeer
    Legionary {}
    nodeState
    _forwarded
    cm
    msg@PeerMessage {payload = NewPeer _clusterId peer addy}
  = do
    debugM ("Received NewPeer: " ++ show msg)
    -- add the peer to the list
    updatePeer cm peer addy
    return nodeState

handlePeerMessage -- ForwardRequest
    Legionary {handleRequest, persistence}
    nodeState
    _forwarded
    cm
    msg@PeerMessage {
        payload = ForwardRequest key request,
        source,
        messageId
      }
  = do
    let respond = void . send cm source . ForwardResponse messageId . encode
    debugM ("Received ForwardRequest: " ++ show msg)
    
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
    nodeState
    forwarded
    _cm
    msg@PeerMessage {
        payload = ForwardResponse messageId response
      }
  = do
    debugM ("Received ForwardResponse: " ++ show msg)
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
    return nodeState

handlePeerMessage -- Claim
    Legionary {}
    nodeState@NodeState {keyspace, self}
    _forwarded
    _cm
    msg@PeerMessage {
        payload = Claim keys,
        source
      }
  = do
    debugM ("Received Claim: " ++ show msg)

    -- don't allow someone else to overwrite our ownership.
    let ours = peerOwns self keyspace
    let newKeyspace = update self ours (update source keys keyspace)
    return nodeState {
        keyspace = newKeyspace
      }

{- |
  Merge two sources into one source. This is a concurrency abstraction.
  The resulting source will produce items from either of the input sources
  as they become available. As you would expect from a multi-producer,
  single-consumer concurrency abstraction, the ordering of items produced
  by each source is consistent relative to other items produced by
  that same source, but the interleaving of items from both sources
  is nondeterministic.
-}
merge :: Source IO a -> Source IO b -> Source IO (Either a b)
merge left right = do
  chan <- lift newChan
  (lift . void . forkIO) (left $= CL.map Left $$ chanToSink chan)
  (lift . void . forkIO) (right $= CL.map Right $$ chanToSink chan)
  chanToSource chan


{- |
  Resolve an address description into an actual socket addr.
-}
resolveAddr :: AddressDescription -> IO SockAddr
resolveAddr desc =
  case splitOn ":" desc of
    ["ipv4", name, port] ->
      addrAddress . head <$> getAddrInfo Nothing (Just name) (Just port)
    _ -> error ("Invalid address description: " ++ show desc)


{- |
  Defines the local state of a node in the cluster.
-}
data NodeState = NodeState {
    keyspace :: KeyDistribution,
    self :: Peer
  }


type MessageId = Word64


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
  = StoreState PartitionKey PartitionState
    -- ^ Tell the receiving node to store the key/state information in
    --   its persistence layer in preparation for a key range ownership
    --   handoff. The receiving node should NOT take ownership of this
    --   key, or start fielding user requests for this key.
  | StoreAck MessageId
    -- ^ Acknowledge the successful handling of a `StoreState` message.
  | NewPeer ClusterId Peer BSockAddr
    -- ^ Tell the receiving node that a new peer has shown up in the
    --   cluster.  This message should initiate a handoff of some portion
    --   of the receiving node's keyspace to the new peer.
  | Handoff PartitionKey PartitionKey
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
  Construct a source of incoming peer messages.
-}
peerMsgSource :: LegionarySettings -> Source IO PeerMessage
peerMsgSource LegionarySettings {peerBindAddr} = join . lift $
    catch (do
        bindAddr <- resolveAddr peerBindAddr
        inputChan <- newChan
        so <- socket (fam bindAddr) Stream defaultProtocol
        setSocketOption so ReuseAddr 1
        bindSocket so bindAddr
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
warningM :: String -> IO ()
warningM = L.warningM "legion"


{- |
  Shorthand logging.
-}
errorM :: (MonadIO io) => String -> io ()
errorM = liftIO . L.errorM "legion"


{- |
  Shorthand logging.
-}
debugM :: String -> IO ()
debugM = L.debugM "legion"


{- |
  Shorthand logging.
-}
infoM :: String -> IO ()
infoM = L.infoM "legion"


{- |
  A type useful only for creating a `Binary` instance of `SockAddr`.
-}
newtype BSockAddr = BSockAddr {getAddr :: SockAddr} deriving (Show)

instance Binary BSockAddr where
  put (BSockAddr addr) =
    case addr of
      SockAddrInet p h -> do
        put (0 :: Word8)
        put (fromEnum p, h)
      SockAddrInet6 p f h s -> do
        put (1 :: Word8)
        put (fromEnum p, f, h, s)
      SockAddrUnix s -> do
        put (2 :: Word8)
        put s
      SockAddrCan a -> do
        put (3 :: Word8)
        put a

  get = BSockAddr <$> do
    c <- get
    case (c :: Word8) of
      0 -> do
        (p, h) <- get
        return (SockAddrInet (toEnum p) h)
      1 -> do
        (p, f, h, s) <- get
        return (SockAddrInet6 (toEnum p) f h s)
      2 -> SockAddrUnix <$> get
      3 -> SockAddrCan <$> get
      _ ->
        fail
          $ "Can't decode BSockAddr because the constructor tag "
          ++ "was not understood. Probably this data is representing "
          ++ "something else."


{- |
  Convert a chanel into a Source.
-}
chanToSource :: Chan a -> Source IO a
chanToSource chan = forever $ yield =<< lift (readChan chan)


{- |
 Convert an chanel into a Sink.
-}
chanToSink :: Chan a -> Sink a IO ()
chanToSink chan = do
  val <- await
  case val of
    Nothing -> return ()
    Just v -> do
      lift (writeChan chan v)
      chanToSink chan


{- |
  This `Sink` is what actually handles all peer messages and user input.
-}
requestSink :: (Binary response, Binary request)
  => Legionary request response
  -> NodeState
  -> Map MessageId (response -> IO ())
  -> ConnectionManager
  -> Sink (Message request response) IO ()
requestSink l nodeState forwarded cm = do
  msg <- await
  case msg of
    Just (P peerMsg) -> do
      newNodeState <-
        lift $ handlePeerMessage l nodeState forwarded cm peerMsg
      requestSink l newNodeState forwarded cm
    Just (R ((key, request), respond)) ->
      case findKey key (keyspace nodeState) of
        Nothing -> do
          errorM
            $ "Keyspace does not contain key: " ++ show key ++ ". This "
            ++ "is a very bad thing and probably means a handoff got "
            ++ "messed up. We are dropping the request on the floor."
          requestSink l nodeState forwarded cm
        Just peer -> do
          mid <- lift $ send cm peer (ForwardRequest key (encode request))
          requestSink l nodeState (insert mid respond forwarded) cm
    Nothing ->
      return ()


{- |
  Initialize the connection manager based on the node state.
-}
initConnectionManager :: (MonadIO io)
  => Peer
  -> BSockAddr
  -> io ConnectionManager
initConnectionManager self selfAddy = liftIO $ do
    cmChan <- newChan
    -- FIXME `nextId = minBound` here is not sufficient!! We are
    -- not allowed to ever re-use a message id or else we risk data
    -- corruption. This is a relatively low probability bug so I'm
    -- punting for now until I figure out how I want to fix it. Probably
    -- by making message id be a combination of a startup-generated uuid
    -- and a number, or something like that.
    let cmState = CMState {cmPeers = singleton self selfAddy, nextId = minBound}
    (void . forkIO . void) (runManager cmChan cmState)
    return ConnectionManager {cmChan}
  where
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
              case getAddr <$> lookup peer cmPeers of
                Nothing -> logNoPeer peer payload cmPeers
                Just addy -> do
                  -- send a message the hard way
                  -- TODO: reuse socket connections.
                  so <- socket (fam addy) Stream defaultProtocol
                  connect so addy
                  sendAll so (encode PeerMessage {
                      source = self,
                      messageId = nextId,
                      payload
                    })
                  close so
              return s {nextId = succ nextId}
            UpdatePeer peer addy ->
              return s {cmPeers = insert peer addy cmPeers}
            Broadcast payload -> do
              let mkSend peer = Send peer payload (const (return ()))
              mapM_ (writeChan chan . mkSend) (Map.keys cmPeers)
              return s
              
      )

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
    cmPeers :: Map Peer BSockAddr,
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


{- |
  The type of a clusterId
-}
type ClusterId = Text


{- |
  The different types of messages handled by this process.
-}
data Message request response
  = P PeerMessage
  | R (RequestMsg request response)


