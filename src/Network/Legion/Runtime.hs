{-# LANGUAGE DeriveGeneric #-}
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
  StartupMode(..),
  Runtime,
  makeRequest,
  search,
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (writeChan, newChan, Chan)
import Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import Control.Monad (void, forever, join, (>=>))
import Control.Monad.Catch (catchAll, try, SomeException, throwM)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger (logWarn, logError, logInfo, LoggingT,
  MonadLoggerIO, runLoggingT, askLoggerIO, logDebug)
import Control.Monad.Trans.Class (lift)
import Data.Binary (encode, Binary)
import Data.Conduit (Source, ($$), (=$=), yield, await, awaitForever,
  transPipe, ConduitM, runConduit, Sink)
import Data.Conduit.Network (sourceSocket)
import Data.Conduit.Serialization.Binary (conduitDecode)
import Data.Map (Map)
import Data.Set (Set)
import Data.Text (pack)
import GHC.Generics (Generic)
import Network.Legion.Admin (runAdmin, AdminMessage(GetState, GetPart,
  Eject))
import Network.Legion.Application (LegionConstraints, getState, Persistence)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.ClusterState (ClusterPowerState)
import Network.Legion.Conduit (merge, chanToSink, chanToSource)
import Network.Legion.Distribution (Peer, newPeer)
import Network.Legion.Fork (forkC)
import Network.Legion.Index (IndexRecord(IndexRecord), irTag, irKey,
  SearchTag(SearchTag))
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.Runtime.ConnectionManager (newConnectionManager,
  send, ConnectionManager, newPeers)
import Network.Legion.Runtime.PeerMessage (PeerMessage(PeerMessage),
  PeerMessagePayload(ForwardRequest, ForwardResponse, ClusterMerge,
  PartitionMerge, Search, SearchResponse), MessageId, newSequence,
  nextMessageId)
import Network.Legion.Settings (RuntimeSettings(RuntimeSettings,
  adminHost, adminPort, peerBindAddr, joinBindAddr))
import Network.Legion.StateMachine (partitionMerge, clusterMerge,
  NodeState, newNodeState, runSM, UserResponse(Forward, Respond),
  userRequest, heartbeat, rebalance, migrate, propagate, ClusterAction,
  eject, minimumCompleteServiceSet)
import Network.Legion.UUID (getUUID)
import Network.Socket (Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN),
  SocketOption(ReuseAddr), SocketType(Stream), accept, bind,
  defaultProtocol, listen, setSocketOption, socket, SockAddr(SockAddrInet,
  SockAddrInet6, SockAddrUnix, SockAddrCan), connect, getPeerName, Socket)
import Network.Socket.ByteString.Lazy (sendAll)
import qualified Data.Conduit.List as CL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.ClusterState as C
import qualified Network.Legion.StateMachine as SM


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
  => Persistence i o s
    {- ^ The persistence layer used to back the legion framework. -}
  -> RuntimeSettings
    {- ^ Settings and configuration of the legionframework.  -}
  -> StartupMode
  -> Source IO (RequestMsg i o)
    {- ^ A source of requests, together with a way to respond to the requets. -}
  -> LoggingT IO ()
    {-
      Don't expose 'LIO' here because 'LIO' is a strictly internal
      symbol. 'LoggingT IO' is what we expose to the world.
    -}

runLegionary
    persistence
    settings@RuntimeSettings {adminHost, adminPort}
    startupMode
    requestSource
  = do
    {- Start the various messages sources.  -}
    peerS <- loggingC =<< startPeerListener settings
    adminS <- loggingC =<< runAdmin adminPort adminHost
    joinS <- loggingC (joinMsgSource settings)

    (self, nodeState, peers) <- makeNodeState settings startupMode
    cm <- newConnectionManager peers

    firstMessageId <- newSequence
    let
      rts = RuntimeState {
          forwarded = Map.empty,
          nextId = firstMessageId,
          cm,
          self,
          searches = Map.empty
        }
    runConduit $
      (joinS `merge` (peerS `merge` (requestSource `merge` adminS)))
        =$= CL.map toMessage
        =$= messageSink persistence (rts, nodeState)
  where
    toMessage
      :: Either
          (JoinRequest, JoinResponse -> LIO ())
          (Either
            (PeerMessage i o s)
            (Either
              (RequestMsg i o)
              (AdminMessage i o s)))
      -> RuntimeMessage i o s
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


{- |
  This is how requests are packaged when they are sent to the legion framework
  for handling. It includes the request information itself, a partition key to
  which the request is directed, and a way for the framework to deliver the
  response to some interested party.
-}
data RequestMsg i o
  = Request PartitionKey i (o -> IO ())
  | SearchDispatch SearchTag (Maybe IndexRecord -> IO ())
instance (Show i) => Show (RequestMsg i o) where
  show (Request k i _) = "(Request " ++ show k ++ " " ++ show i ++ " _)"
  show (SearchDispatch s _) = "(SearchDispatch " ++ show s ++ " _)"


messageSink :: (LegionConstraints i o s)
  => Persistence i o s
  -> (RuntimeState i o s, NodeState i o s)
  -> Sink (RuntimeMessage i o s) LIO ()
messageSink persistence states =
    await >>= \case
      Nothing -> return ()
      Just msg -> do
        $(logDebug) . pack
          $ "Receieved: " ++ show msg
        lift . handleMessage persistence msg
          >=> lift . updatePeers persistence
          >=> lift . clusterHousekeeping persistence
          >=> messageSink persistence
          $ states


{- |
  Make sure the connection manager knows about any new peers that have
  joined the cluster.
-}
updatePeers
  :: Persistence i o s
  -> (RuntimeState i o s, NodeState i o s)
  -> LIO (RuntimeState i o s, NodeState i o s)
updatePeers persistence (rts, ns) = do
  (peers, ns2) <- runSM persistence ns SM.getPeers
  newPeers (cm rts) peers
  return (rts, ns2)


{- |
  Perform any cluster management actions, and update the state
  appropriately.
-}
clusterHousekeeping :: (LegionConstraints i o s)
  => Persistence i o s
  -> (RuntimeState i o s, NodeState i o s)
  -> LIO (RuntimeState i o s, NodeState i o s)
clusterHousekeeping persistence (rts, ns) = do
    (actions, ns2) <- runSM persistence ns (
        heartbeat
        >> rebalance
        >> migrate
        >> propagate
      )
    rts2 <- foldr (>=>) return (clusterAction <$> actions) rts
    return (rts2, ns2)


{- |
  Actually perform a cluster action as directed by the state
  machine.
-}
clusterAction
  :: ClusterAction i o s
  -> RuntimeState i o s
  -> LIO (RuntimeState i o s)

clusterAction
    (SM.ClusterMerge peer ps)
    rts@RuntimeState {self, nextId, cm}
  = do
    send cm peer (PeerMessage self nextId (ClusterMerge ps))
    return rts {nextId = nextMessageId nextId}

clusterAction
    (SM.PartitionMerge peer key ps)
    rts@RuntimeState {self, nextId, cm}
  = do
    send cm peer (PeerMessage self nextId (PartitionMerge key ps))
    return rts {nextId = nextMessageId nextId}


{- |
  Handle an individual runtime message, accepting an initial runtime
  state and an initial node state, and producing an updated runtime
  state and node state.
-}
handleMessage :: (LegionConstraints i o s)
  => Persistence i o s
  -> RuntimeMessage i o s
  -> (RuntimeState i o s, NodeState i o s)
  -> LIO (RuntimeState i o s, NodeState i o s)

handleMessage {- Partition Merge -}
    persistence
    (P (PeerMessage source _ (PartitionMerge key ps)))
    (rts, ns)
  = do
    ((), ns2) <- runSM persistence ns (partitionMerge source key ps)
    return (rts, ns2)

handleMessage {- Cluster Merge -}
    persistence
    (P (PeerMessage source _ (ClusterMerge cs)))
    (rts, ns)
  = do
    ((), ns2) <- runSM persistence ns (clusterMerge source cs)
    return (rts, ns2)

handleMessage {- Forward Request -}
    persistence
    (P (msg@(PeerMessage source mid (ForwardRequest key request))))
    (rts@RuntimeState {nextId, cm, self}, ns)
  = do
    (output, ns2) <- runSM persistence ns (userRequest key request)
    case output of
      Respond response -> do
        send cm source (
            PeerMessage self nextId (ForwardResponse mid response)
          )
        return (rts {nextId = nextMessageId nextId}, ns2)
      Forward peer -> do
        send cm peer msg
        return (rts {nextId = nextMessageId nextId}, ns2)

handleMessage {- Forward Response -}
    _legionary
    (msg@(P (PeerMessage _ _ (ForwardResponse mid response))))
    (rts, ns)
  =
    case lookupDelete mid (forwarded rts) of
      (Nothing, fwd) -> do
        $(logWarn) . pack $ "Unsolicited ForwardResponse: " ++ show msg
        return (rts {forwarded = fwd}, ns)
      (Just respond, fwd) -> do
        respond response
        return (rts {forwarded = fwd}, ns)

handleMessage {- User Request -}
    persistence
    (R (Request key request respond))
    (rts@RuntimeState {self, cm, nextId, forwarded}, ns)
  = do
    (output, ns2) <- runSM persistence ns (userRequest key request)
    case output of
      Respond response -> do
        lift (respond response)
        return (rts, ns2)
      Forward peer -> do
        send cm peer (
            PeerMessage self nextId (ForwardRequest key request)
          )
        return (
            rts {
              forwarded = Map.insert nextId (lift . respond) forwarded,
              nextId = nextMessageId nextId
            },
            ns2
          )

handleMessage {- Search Dispatch -}
    {-
      This is where we send out search request to all the appropriate
      nodes in the cluster.
    -}
    persistence
    (R (SearchDispatch searchTag respond))
    (rts@RuntimeState {cm, self, searches}, ns)
  =
    case Map.lookup searchTag searches of
      Nothing -> do
        {-
          No identical search is currently being executed, kick off a
          new one.
        -}
        (mcss, ns2) <- runSM persistence ns minimumCompleteServiceSet 
        rts2 <- foldr (>=>) return (sendOne <$> Set.toList mcss) rts
        return (
            rts2 {
              searches = Map.insert
                searchTag
                (mcss, Nothing, [lift . respond])
                searches
            },
            ns2
          )
      Just (peers, best, responders) ->
        {-
          A search for this tag is already in progress, just add the
          responder to the responder list.
        -}
        return (
            rts {
              searches = Map.insert
                searchTag
                (peers, best, (lift . respond):responders)
                searches
            },
            ns
          )
  where
    sendOne :: Peer -> RuntimeState i o s -> LIO (RuntimeState i o s)
    sendOne peer r@RuntimeState {nextId} = do
      send cm peer (PeerMessage self nextId (Search searchTag))
      return r {nextId = nextMessageId nextId}

handleMessage {- Search Execution -}
    {- This is where we handle local search execution. -}
    persistence
    (P (PeerMessage source _ (Search searchTag)))
    (rts@RuntimeState {nextId, cm, self}, ns)
  = do
    (output, ns2) <- runSM persistence ns (SM.search searchTag) 
    send cm source (PeerMessage self nextId (SearchResponse searchTag output))
    return (rts {nextId = nextMessageId nextId}, ns2)

handleMessage {- Search Response -}
    {-
      This is where we gather all the responses from the various peers
      to which we dispatched search requests.
    -}
    _legionary
    (msg@(P (PeerMessage source _ (SearchResponse searchTag response))))
    (rts@RuntimeState {searches}, ns)
  =
    {- TODO: see if this function can't be made more elegant. -}
    case Map.lookup searchTag searches of
      Nothing -> do
        {- There is no search happening. -}
        $(logWarn) . pack $ "Unsolicited SearchResponse: " ++ show msg
        return (rts, ns)
      Just (peers, best, responders) ->
        if source `Set.member` peers
          then
            let peers2 = Set.delete source peers
            in if null peers2
              then do
                {-
                  All peers have responded, go ahead and respond to
                  the client.
                -}
                mapM_ ($ bestOf best response) responders
                return (
                    rts {searches = Map.delete searchTag searches},
                    ns
                  )
              else
                {- We are still waiting on some outstanding requests. -}
                return (
                    rts {
                      searches = Map.insert
                        searchTag
                        (peers2, bestOf best response, responders)
                        searches
                    },
                    ns
                  )
          else do
            {-
              There is a search happening, but the peer that responded
              is not part of it.
            -}
            $(logWarn) . pack $ "Unsolicited SearchResponse: " ++ show msg
            return (rts, ns)
  where
    {- |
      Figure out which index record returned to us by the various peers
      is the most appropriate to return to the user. This is mostly like
      'min' but we can't use 'min' (or fancy applicative formulations)
      because we want to favor 'Just' instead of 'Nothing'.
    -}
    bestOf :: Maybe IndexRecord -> Maybe IndexRecord -> Maybe IndexRecord
    bestOf (Just a) (Just b) = Just (min a b)
    bestOf Nothing b = b
    bestOf a Nothing = a

handleMessage {- Join Request -}
    persistence
    (J (JoinRequest addy, respond))
    (rts, ns)
  = do
    ((peer, cluster), ns2) <- runSM persistence ns (SM.join addy)
    respond (JoinOk peer cluster)
    return (rts, ns2)

handleMessage {- Admin Get State -}
    _legionary
    (A (GetState respond))
    (rts, ns)
  =
    respond ns >> return (rts, ns)

handleMessage {- Admin Get Partition -}
    persistence
    (A (GetPart key respond))
    (rts, ns)
  = do
    respond =<< lift (getState persistence key)
    return (rts, ns)

handleMessage {- Admin Eject Peer -}
    persistence
    (A (Eject peer respond))
    (rts, ns)
  = do
    {-
      TODO: we should attempt to notify the ejected peer that it has
      been ejected instead of just cutting it off and washing our hands
      of it. I have a vague notion that maybe ejected peers should be
      permanently recorded in the cluster state so that if they ever
      reconnect then we can notify them that they are no longer welcome
      to participate.

      On a related note, we need to think very hard about the split brain
      problem. A random thought about that is that we should consider the
      extreme case where the network just fails completely and every node
      believes that every other node should be or has been ejected. This
      would obviously be catastrophic in terms of data durability unless
      we have some way to reintegrate an ejected node. So, either we
      have to guarantee that such a situation can never happen, or else
      implement a reintegration strategy.  It might be acceptable for
      the reintegration strategy to be very costly if it is characterized
      as an extreme recovery scenario.

      Question: would a reintegration strategy become less costly if the
      "next state id" for a peer were global across all power states
      instead of local to each power state?
    -}
    ((), ns2) <- runSM persistence ns (eject peer)
    respond ()
    return (rts, ns2)


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
  => RuntimeSettings
  -> LIO (Source LIO (PeerMessage i o s))

startPeerListener RuntimeSettings {peerBindAddr} =
    catchAll (do
        (inputChan, so) <- lift $ do
          inputChan <- newChan
          so <- socket (fam peerBindAddr) Stream defaultProtocol
          setSocketOption so ReuseAddr 1
          bind so peerBindAddr
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
makeNodeState
  :: RuntimeSettings
  -> StartupMode
  -> LIO (Peer, NodeState i o s, Map Peer BSockAddr)

makeNodeState RuntimeSettings {peerBindAddr} NewCluster = do
  {- Build a brand new node state, for the first node in a cluster. -}
  self <- newPeer
  clusterId <- getUUID
  let
    cluster = C.new clusterId self peerBindAddr
    nodeState = newNodeState self cluster
  return (self, nodeState, C.getPeers cluster)

makeNodeState RuntimeSettings {peerBindAddr} (JoinCluster addr) = do
    {-
      Join a cluster by either starting fresh, or recovering from a
      shutdown or crash.
    -}
    $(logInfo) "Trying to join an existing cluster."
    (self, clusterPS) <- joinCluster (JoinRequest (BSockAddr peerBindAddr))
    let
      cluster = C.initProp self clusterPS
      nodeState = newNodeState self cluster
    return (self, nodeState, C.getPeers cluster)
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
  :: RuntimeSettings
  -> Source LIO (JoinRequest, JoinResponse -> LIO ())

joinMsgSource RuntimeSettings {joinBindAddr} = join . lift $
    catchAll (do
        (chan, so) <- lift $ do
          chan <- newChan
          so <- socket (fam joinBindAddr) Stream defaultProtocol
          setSocketOption so ReuseAddr 1
          bind so joinBindAddr
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

  - @__i__@ is the type of request your application will handle. @__i__@ stands
    for __"input"__.
  - @__o__@ is the type of response produced by your application. @__o__@ stands
    for __"output"__
  - @__s__@ is the type of state maintained by your application. More
    precisely, it is the type of the individual partitions that make up
    your global application state. @__s__@ stands for __"state"__.
-}
forkLegionary :: (LegionConstraints i o s, MonadLoggerIO io)
  => Persistence i o s
    {- ^ The persistence layer used to back the legion framework. -}
  -> RuntimeSettings
    {- ^ Settings and configuration of the legion framework. -}
  -> StartupMode
  -> io (Runtime i o)

forkLegionary persistence settings startupMode = do
  logging <- askLoggerIO
  liftIO . (`runLoggingT` logging) $ do
    chan <- liftIO newChan
    forkC "main legion thread" $
      runLegionary persistence settings startupMode (chanToSource chan)
    return Runtime {
        rtMakeRequest = \key request -> liftIO $ do
          responseVar <- newEmptyMVar
          writeChan chan (Request key request (putMVar responseVar))
          takeMVar responseVar,
        rtSearch =
          let
            findNext :: SearchTag -> IO (Maybe IndexRecord)
            findNext searchTag = do
              responseVar <- newEmptyMVar
              writeChan chan (SearchDispatch searchTag (putMVar responseVar))
              takeMVar responseVar
          in findNext

      }


{- |
  This type represents a handle to the runtime environment of your
  Legion application. This allows you to make requests and access the
  partition index.

  'Runtime' is an opaque structure. Use 'makeRequest' to access it.
-}
data Runtime i o = Runtime {
    {- |
      Send an application request to the legion runtime, and get back
      a response.
    -}
    rtMakeRequest :: PartitionKey -> i -> IO o,

    {- | Query the index to find a set of partition keys.  -}
    rtSearch :: SearchTag -> IO (Maybe IndexRecord)
  }


{- | Send a user request to the legion runtime. -}
makeRequest :: (MonadIO io) => Runtime i o -> PartitionKey -> i -> io o
makeRequest rt key = liftIO . rtMakeRequest rt key


{- |
  Send a search request to the legion runtime. Returns results that are
  __strictly greater than__ the provided 'SearchTag'.
-}
search :: (MonadIO io) => Runtime i o -> SearchTag -> Source io IndexRecord
search rt tag =
  liftIO (rtSearch rt tag) >>= \case
    Nothing -> return ()
    Just record@IndexRecord {irTag, irKey} -> do
      yield record
      search rt (SearchTag irTag (Just irKey))


{- | This is the type of message passed around in the runtime. -}
data RuntimeMessage i o s
  = P (PeerMessage i o s)
  | R (RequestMsg i o)
  | J (JoinRequest, JoinResponse -> LIO ())
  | A (AdminMessage i o s)
instance (Show i, Show o, Show s) => Show (RuntimeMessage i o s) where
  show (P m) = "(P " ++ show m ++ ")"
  show (R m) = "(R " ++ show m ++ ")"
  show (J (jr, _)) = "(J (" ++ show jr ++ ", _))"
  show (A a) = "(A (" ++ show a ++ "))"


{- |
  The runtime state.

  The 'searches' field is a little weird.

  It turns out that searches are deterministic over the parameters of
  'SearchTag' and cluster state. This should make sense, because everything in
  Haskell is deterministic given __all__ the parameters. Since the cluster
  state only changes over time, searches that happen "at the same time" and
  for the same 'SearchTag' can be considered identical. I don't think it is too
  much of a stretch to say that searches that have overlapping execution times
  can be considered to be happening "at the same time", therefore the
  search tag becomes determining factor in the result of the search.

  This is a long-winded way of justifying the fact that, if we are currently
  executing a search and an identical search requests arrives, then the second
  identical search is just piggy-backed on the results of the currently
  executing search. Whether this counts as a premature optimization hack or a
  beautifully elegant expression of platonic reality is left as an exercise for
  the reader. It does help simplify the code a little bit because we don't have
  to specify some kind of UUID to differentiate otherwise identical searches.
-}
data RuntimeState i o s = RuntimeState {
         self :: Peer,
    forwarded :: Map MessageId (o -> LIO ()),
       nextId :: MessageId,
           cm :: ConnectionManager i o s,
     searches :: Map
                  SearchTag
                  (Set Peer, Maybe IndexRecord, [Maybe IndexRecord -> LIO ()])
  }


{- | This is the type of a join request message. -}
data JoinRequest = JoinRequest BSockAddr
  deriving (Generic, Show)
instance Binary JoinRequest


{- | The response to a JoinRequst message -}
data JoinResponse
  = JoinOk Peer ClusterPowerState
  | JoinRejected String
  deriving (Generic)
instance Binary JoinResponse


{- | Lookup a key from a map, and also delete the key if it exists. -}
lookupDelete :: (Ord k) => k -> Map k v -> (Maybe v, Map k v)
lookupDelete = Map.updateLookupWithKey (const (const Nothing))


