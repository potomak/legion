{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{- |
  This module contains the shared cluster state, which is basically a CRDT that
  is distributed throughout the cluster. The idea is that updates will be
  distributed via some kind of efficient gossip protocol.
-}
module Network.Legion.ClusterManagement (
  Cluster,
  ClusterState,
  PeerMessage(..),
  PeerMessagePayload(..),
  MessageId,
  newCluster,
  initManager,
  distribution,
  self,
  peers,
  recover,
  current,
  send,
  forward,
  requestJoin,
  requestRejoin,
  claim,
  merge
) where

import Prelude hiding (null, readFile, lookup)

import Control.Applicative ((<$>))
import Control.Concurrent (Chan, writeChan, newChan, readChan,
  newEmptyMVar, putMVar, takeMVar, threadDelay)
import Control.Concurrent.STM (TVar, atomically, readTVar, newTVar,
  writeTVar)
import Control.Exception (try, SomeException)
import Control.Monad (void, join, forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Binary (Binary(get), encode)
import Data.Binary.Get (runGetOrFail, Get)
import Data.Bool (bool)
import Data.ByteString.Lazy (ByteString, null, hPut, hGetContents)
import Data.Conduit (($$), awaitForever)
import Data.Map (Map, lookup, toAscList, toDescList, differenceWith,
  unionWith, keys)
import Data.Set (Set, union, (\\), unions)
import Data.UUID (UUID)
import Data.UUID.V1 (nextUUID)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr, getAddr))
import Network.Legion.Conduit (chanToSource)
import Network.Legion.Distribution (KeySet, PartitionDistribution,
  PartitionKey, Peer, Replica, update, PartitionState, fromRange,
  Replica(R1))
import Network.Legion.Fork (forkC)
import Network.Socket (SockAddr, Socket, socket, SocketType(Stream),
  defaultProtocol, connect, close, SockAddr(SockAddrInet, SockAddrInet6,
  SockAddrUnix, SockAddrCan), Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN))
import Network.Socket.ByteString.Lazy (sendAll)
import System.Directory (doesFileExist)
import System.IO (withBinaryFile, IOMode(WriteMode, ReadMode),
  BufferMode(NoBuffering), hSetBuffering)
import qualified Data.Map as Map (empty, singleton, fromList, insert,
  toList)
import qualified Data.Set as Set (singleton, fromList, insert, difference,
  null, toList)
import qualified Network.Legion.Distribution as D (empty)
import qualified System.Log.Logger as L (infoM, warningM, errorM, debugM)


{- |
  The managed cluster state. This takes care of maintaining connections
  to other nodes, and automatic journaling of all mutations to the
  cluster state.
-}
data Cluster = J {
    csT :: TVar ClusterState,
    journal :: (UpdateId, (Update, Set Peer)) -> IO (),
    self :: Peer,
      -- ^ The `self` value never changes, so we are free to cache it
      --   here for convenience. Maintainers should note, however, that
      --   "self" is really an attribute of `ClusterState`, and that
      --   maintaining it here is only an optimization.
    cm :: ConnectionManager
  }


{- |
  Figure out the partition distribution based on the known cluster state.
-}
distribution :: Cluster -> IO PartitionDistribution
distribution J {csT} = do
    cs <- atomically (readTVar csT)
    (return . foldr apply D.empty . fmap (fst . snd) . toDescList . updates) cs
  where
    apply :: Update -> PartitionDistribution -> PartitionDistribution
    apply (Claim peer ranges) = update peer ranges
    apply (PeerJoined peer _) = update peer Map.empty


{- |
  Get all of the known peers.
-}
peers :: Cluster -> IO (Map Peer SockAddr)
peers J {csT} = do
  cs <- atomically (readTVar csT)
  return (csPeers cs)


{- |
  Build the peer map from a raw `ClusterState`
-}
csPeers :: ClusterState -> Map Peer SockAddr
csPeers = foldr apply Map.empty . fmap (fst . snd) . toAscList . updates
  where
    apply :: Update -> Map Peer SockAddr -> Map Peer SockAddr
    apply (PeerJoined peer addr) = Map.insert peer (getAddr addr)
    apply _ = id


{- |
  Create a new cluster state, with a new cluster id, and a new self.
-}
newCluster :: FilePath -> SockAddr-> IO Cluster
newCluster journalFile selfAddr = do
  clusterId <- getUUID
  csSelf <- getUUID
  let cs = ClusterState {
          csSelf,
          clusterId,
          updates = Map.fromList [
            (
              UpdateId 0 csSelf,
              (
                Claim csSelf (Map.singleton R1 (fromRange minBound maxBound)),
                Set.singleton csSelf
              )
            ),
            (
              UpdateId 1 csSelf,
              (
                PeerJoined csSelf (BSockAddr selfAddr),
                Set.singleton csSelf
              )
            )
          ]
        }
  initManager journalFile cs


{- |
  Initialize a cluster manager based off of the `ClusterState` that was
  returned to us as a result of a cluster join request.
-}
initManager :: FilePath -> ClusterState -> IO Cluster
initManager journalFile cs = do
  journal <- initJournal journalFile cs
  csT <- atomically (newTVar cs)
  cm <- newConnectionManager (csSelf cs) (csPeers cs)
  let cluster = J {csT, journal, self = csSelf cs, cm}
  startGossip cluster
  return cluster


{- |
  Start gossiping the cluster state to other nodes.
  
  This is the most degenerate algorithm for now just to get things
  working until I can dedicate brain-power to this.
-}
startGossip :: Cluster -> IO ()
startGossip cluster =
    forkC "gossip thread" (forever $ do
        threadDelay oneSecond
        cs@ClusterState {updates} <- current cluster
        let peerSet = Set.fromList (keys (csPeers cs))
            notInTheKnow = (Set.toList . unions)
              $ (peerSet \\) <$> (snd . snd <$> Map.toList updates)
        mapM_ (\p -> send cluster p (UpdateCluster cs)) notInTheKnow
      )
  where
    oneSecond = 1000000


{- |
  Tries to recover the cluster-state journal.
-}
recover
  :: FilePath
  -> IO (Maybe ClusterState)
recover journalFile =
    doesFileExist journalFile >>= bool (return Nothing) (
        withBinaryFile journalFile ReadMode $ \h -> do
          fileData <- hGetContents h
          case runGetOrFail getInit fileData of
            Left (_, _, err) -> error
              $ "Malformed or corrupt journal file: " ++ show journalFile
              ++ ". We can't go on like this. The best thing to do "
              ++ "is to deep six this node entirely, maybe add a virgin "
              ++ "node to the cluster, and let the cluster heal itself. "
              ++ "Unless you know EXACTLY what you are doing, just deleting "
              ++ "the journal file and restarting is probably going to be "
              ++ "something you will regret. The specific error was: " ++ err
            Right (remaining, _, val) -> do
              let (cs, warning) = readAndApply val remaining
              maybe (return ()) warningM warning
              return (Just cs)
      )
  where
    getInit :: Get ClusterState
    getInit = get

    getEntry :: Get (UpdateId, (Update, Set Peer))
    getEntry = get

    apply
      :: (UpdateId, (Update, Set Peer))
      -> ClusterState
      -> ClusterState
    apply (uid, u) cs@ClusterState {updates} =
      cs {
        updates = Map.insert uid u updates
      }

    readAndApply val bytes
      | null bytes = (val, Nothing)
      | otherwise =
          case runGetOrFail getEntry bytes of
            Left (_, _, err) ->
              (
                val,
                Just
                  $ "It looks like the last entry in the journal file is"
                  ++ "corrupt, which might mean that the node crashed while"
                  ++ "trying to write an entry. We are going to continue"
                  ++ "with what we have so far and ignore the malformed"
                  ++ "portion of the file.  The specific error was: "
                  ++ err
              )
            Right (remaining, _, entry) ->
              readAndApply (apply entry val) remaining


{- |
  Start the background journaling thread. Returns a way to record entries in
  journal.
-}
initJournal
  :: FilePath
    -- ^ The location in which to store the journal.
  -> ClusterState
    -- ^ The initial cluster state.
  -> IO ((UpdateId, (Update, Set Peer)) -> IO ())
initJournal file cs = do
  chan <- newChan
  {-
    We don't want to return until the initial write is complete, because
    returning from this function signals a guarantee that the journal
    has been written. We decided to use `MVar`s instead of opening and
    closing the file in this thread before opening it a second time in
    the forked thread.
  -}
  initialWriteComplete <- newEmptyMVar
  void . forkC "journaling thread" $ 
    withBinaryFile file WriteMode (\h -> do
        hSetBuffering h NoBuffering
        hPut h (encode cs)
        putMVar initialWriteComplete ()
        chanToSource chan $$ awaitForever (\(entry, finished) -> liftIO $ do
            hPut h (encode entry)
            -- TODO maybe we still need to fsync the file?
            putMVar finished ()
          )
      )
  takeMVar initialWriteComplete
  return (\e -> do
      finished <- newEmptyMVar
      writeChan chan (e, finished)
      takeMVar finished
    )


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
  Changing the cluster state by assigning the specified partitions to the
  target peer.
-}
claim :: Cluster -> Peer -> Map Replica KeySet -> IO ()
claim J {csT, journal, self} peer ranges = join . atomically $ do
  cs@ClusterState {updates} <- readTVar csT
  let updateId = nextUpdateId cs
      thisUpdate = (Claim peer ranges, Set.singleton self)
  writeTVar csT cs {
      updates = Map.insert updateId thisUpdate updates
    }
  {-
    FIXME - This is a concurrency risk. Everything is fine while we only
    access or mutate the cluster state in one thread, but if we want
    to make it thread safe, i.e. eliminate race conditions, we have to
    write the journal prior to updating the in-memory representation.
    This can't be done using STM because we have to use exclusive locks,
    which means MVar or something.
  -}
  return (journal (updateId, thisUpdate))


{- |
  Merge a foreign cluster state with the local cluster state.
-}
merge :: Cluster -> ClusterState -> IO ()
merge
    J {csT, self, journal}
    ClusterState {updates = u2}
  = join . atomically $ do
    cs@ClusterState {updates = u1} <- readTVar csT
    let newUpdates = markAsKnown <$> differenceWith diffPeers u2 u1
    writeTVar csT cs {
        updates = unionWith mergePeers u1 newUpdates
      }
    return $ mapM_ journal (Map.toList newUpdates)
  where
    markAsKnown (u, p) = (u, Set.insert self p)
    mergePeers (u, p1) (_, p2) =
      (u, p1 `union` p2)

    diffPeers (u, p1) (_, p2) =
      let p3 = Set.difference p1 p2 in
      if Set.null p3
        then Nothing
        else Just (u, p3)


{- |
  Return the current `ClusterState` value.
-}
current :: Cluster -> IO ClusterState
current J {csT} = atomically (readTVar csT)


{- |
  A handle on the connection manager
-}
data ConnectionManager = C (Chan Message)


{- |
  Create a new connection manager.
-}
newConnectionManager :: Peer -> Map Peer SockAddr -> IO ConnectionManager
newConnectionManager self initPeers = do
    nextId <- newSequence
    chan <- newChan
    forkC "connection manager thread" $
      manager chan S {nextId, connections = Map.empty}
    let cm = C chan
    mapM_ ((uncurry . newPeer) cm) (Map.toList initPeers)
    return cm
  where
    manager chan state = readChan chan >>= handle state >>= manager chan
    handle :: State -> Message -> IO State
    handle s@S {nextId, connections} (Send peer payload respond) =
      case lookup peer connections of
        Nothing ->
          error ("unknown peer: " ++ show peer)
        Just conn -> do
          respond nextId
          writeChan conn PeerMessage {
              source = self,
              messageId = nextId,
              payload
            }
          return s {nextId = next nextId}
    handle s@S {connections} (NewPeer peer addr) = do
      conn <- connection addr
      return s {
          connections = Map.insert peer conn connections
        }
    handle s@S {connections} (Forward peer msg) = do
      case lookup peer connections of
        Nothing -> errorM $ "unknown peer: " ++ show peer
        Just conn -> writeChan conn msg
      return s


{- |
  Build a new connection.
-}
connection :: SockAddr -> IO (Chan PeerMessage)
connection addr = do
    chan <- newChan
    forkC ("connection to: " ++ show addr) $
      handle chan =<< openSocket
    return chan
  where
    handle :: Chan PeerMessage -> Socket -> IO ()
    handle chan so =
      readChan chan >>= sendWithRetry so . encode >>= handle chan

    {- |
      Open a socket.
    -}
    openSocket :: IO Socket
    openSocket = do
      so <- socket (fam addr) Stream defaultProtocol
      connect so addr
      return so

    {- |
      Try to send the payload over the socket, and if that fails, then try to
      create a new socket and retry sending the payload. Return whatever the
      "working" socket is.
    -}
    sendWithRetry :: Socket -> ByteString -> IO Socket
    sendWithRetry so payload = do
      result <- try (sendAll so payload)
      case result of
        Left err -> do
          infoM
            $ "Socket to " ++ show addr ++ " died. Retrying on a new "
            ++ "socket. The error was: " ++ show (err :: SomeException)
          void (try (close so) :: IO (Either SomeException ()))
          so2 <- openSocket
          sendAll so2 payload
          return so2
        Right _ ->
          return so


{- |
  Send a message to a peer. Returns the id of the message that was sent.
-}
send :: Cluster -> Peer -> PeerMessagePayload -> IO MessageId
send J {cm = C chan} peer msg = do
  debugM ("Sending " ++ show msg ++ " to peer: " ++ show peer)
  mvar <- newEmptyMVar
  writeChan chan (Send peer msg (putMVar mvar))
  takeMVar mvar


{- |
  Forward a pre-built message to a peer.
-}
forward :: Cluster -> Peer -> PeerMessage -> IO ()
forward J {cm = C chan} peer msg = writeChan chan (Forward peer msg)


{- |
  Tell the connection manager about a new peer.
-}
newPeer :: ConnectionManager -> Peer -> SockAddr -> IO ()
newPeer (C chan) peer addr = writeChan chan (NewPeer peer addr)


{- |
  The internal state of the connection manager.
-}
data State = S {
    nextId :: MessageId,
    connections :: Map Peer (Chan PeerMessage)
  }


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
  | ForwardRequest PartitionKey ByteString
    -- ^ Forward a binary encoded user request to the receiving node.
  | ForwardResponse MessageId ByteString
    -- ^ Respond to the forwarded request, identified by MessageId,
    --   with the binary encoded user response.
  | UpdateCluster ClusterState
    -- ^ This message allows the sending node to provide a possibly updated
    --   cluster state to the receiving node.
  deriving (Generic, Show)

instance Binary PeerMessagePayload


{- |
  The types of messages that the ConnectionManager understands.
-}
data Message
  = Send Peer PeerMessagePayload (MessageId -> IO ())
  | NewPeer Peer SockAddr
  | Forward Peer PeerMessage


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
infoM :: String -> IO ()
infoM = L.infoM "legion"


{- |
  An opaque data type, representing the cluster state that is shared
  between all peers.
-}
data ClusterState = ClusterState {
    csSelf :: Peer,
      -- ^ The peer that is keeping track of this cluster state
      --   instance. It is important that this be defined here for subtle
      --   reasons that I should probably write down when I have time
      --   to formulate the words.
      --
      --   TODO: EXPLAIN MYSELF!
    clusterId :: UUID,
    updates :: Map UpdateId (Update, Set Peer)
  }
  deriving (Show, Generic)

instance Binary ClusterState


{- |
  Identifies a particular update to the cluster state.
-}
data UpdateId = UpdateId Word64 Peer deriving (Eq, Ord, Show, Generic)
instance Binary UpdateId


{- |
  The kinds of updates that can be applied to the cluster state.
-}
data Update
  = Claim Peer (Map Replica KeySet)
  | PeerJoined Peer BSockAddr
  deriving (Show, Generic)
instance Binary Update


data MessageId = M UUID Word64 deriving (Generic, Show, Eq, Ord)
instance Binary MessageId


{- |
  Initialize a new sequence of messageIds
-}
newSequence :: IO MessageId
newSequence = do
  sid <- getUUID
  return (M sid 0)


{- |
  Generate the next message id in the sequence. We would use `succ`, but making
  `MessageId` an  instance of `Enum` really isn't appropriate.
-}
next :: MessageId -> MessageId
next (M sequenceId ord) = M sequenceId (ord + 1)


{- |
  Make a UUID, no matter what.
-}
getUUID :: IO UUID
getUUID = nextUUID >>= maybe (wait >> getUUID) return
  where
    wait = threadDelay oneMillisecond
    oneMillisecond = 1000


{- |
  Allow a new potential peer to join the cluster, returning the initial cluster
  state they.
-}
requestJoin :: Cluster -> SockAddr -> IO ClusterState
requestJoin J {csT, cm, self, journal} addr = do
  peer <- getUUID
  newPeer cm peer addr
  join . atomically $ do
    cs@ClusterState {updates} <- readTVar csT
    let updateId = nextUpdateId cs
        thisUpdate = (
            PeerJoined peer (BSockAddr addr),
            Set.fromList [peer, self]
          )
        cs2 = cs {
            updates = Map.insert updateId thisUpdate updates
          }
    writeTVar csT cs2
    return $ do
      journal (updateId, thisUpdate)
      return cs2 {csSelf = peer}


{- |
  Maybe allow an old peer to re-join the cluster after it crashed or
  was shut down, and tell it what its new `ClusterState` should be.
-}
requestRejoin :: Cluster -> ClusterState -> IO (Either String ClusterState)
requestRejoin J {csT} other = atomically $ do
  cs <- readTVar csT
  if csSelf other `elem` keys (csPeers cs)
    then return (Right other)
    else return . Left
      $ "Rejoin request denied. Peer " ++ show (csSelf other)
      ++ " was never a member of this cluster."


{- |
  Find the next update id.
-}
nextUpdateId :: ClusterState -> UpdateId
nextUpdateId ClusterState {csSelf, updates} =
  case fst <$> toDescList updates of
    [] -> UpdateId 0 csSelf
    UpdateId o _:_ -> UpdateId (o + 1) csSelf


