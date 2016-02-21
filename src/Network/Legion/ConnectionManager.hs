{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  This module manages connections to other nodes in the cluster.
-}
module Network.Legion.ConnectionManager (
  ConnectionManager,
  PeerMessage(..),
  PeerMessagePayload(..),
  MessageId,
  newConnectionManager,
  send,
  forward,
  newPeer,
  newPeers
) where

import Prelude hiding (lookup)

import Control.Concurrent (Chan, writeChan, newChan, readChan,
  newEmptyMVar, putMVar, takeMVar, threadDelay)
import Control.Exception (try, SomeException)
import Control.Monad (void)
import Control.Monad.Logger (logInfo, logWarn, logDebug)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary, encode)
import Data.ByteString.Lazy (ByteString)
import Data.Map (toList, insert, empty, Map, lookup)
import Data.Text (pack)
import Data.UUID (UUID)
import Data.UUID.V1 (nextUUID)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.ClusterState (ClusterPowerState)
import Network.Legion.Distribution (Peer)
import Network.Legion.Fork (forkC)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState)
import Network.Socket (SockAddr, Socket, socket, SocketType(Stream),
  defaultProtocol, connect, close, SockAddr(SockAddrInet, SockAddrInet6,
  SockAddrUnix, SockAddrCan), Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN))
import Network.Socket.ByteString.Lazy (sendAll)

{- |
  A handle on the connection manager
-}
data ConnectionManager i o s = C (Chan (Message i o s))
instance Show (ConnectionManager i o s) where
  show _ = "ConnectionManager"


{- |
  Create a new connection manager.
-}
newConnectionManager :: (Binary i, Binary o, Binary s)
  => Peer
  -> Map Peer SockAddr
  -> LIO (ConnectionManager i o s)
newConnectionManager self initPeers = do
    nextId <- newSequence
    chan <- lift newChan
    forkC "connection manager thread" $
      manager chan S {nextId, connections = empty}
    let cm = C chan
    mapM_ ((uncurry . newPeer) cm) (toList initPeers)
    return cm
  where
    manager :: (Binary s, Binary o, Binary i)
      => Chan (Message i o s)
      -> State i o s
      -> LIO ()
    manager chan state = lift (readChan chan) >>= handle state >>= manager chan

    handle :: (Binary i, Binary o, Binary s)
      => State i o s
      -> Message i o s
      -> LIO (State i o s)
    handle s@S {nextId, connections} (Send peer payload respond) =
      case lookup peer connections of
        Nothing ->
          error ("unknown peer: " ++ show peer)
        Just conn -> do
          respond nextId
          lift $ writeChan conn PeerMessage {
              source = self,
              messageId = nextId,
              payload
            }
          return s {nextId = next nextId}

    handle s@S {connections} (NewPeer peer addr) =
      case lookup peer connections of
        Nothing -> do
          conn <- connection addr
          return s {
              connections = insert peer conn connections
            }
        Just _ ->
          return s

    handle s@S {connections} (Forward peer msg) = do
      case lookup peer connections of
        Nothing -> $(logWarn) . pack $ "unknown peer: " ++ show peer
        Just conn -> lift $ writeChan conn msg
      return s


{- |
  Build a new connection.
-}
connection :: (Binary i, Binary o, Binary s)
  => SockAddr
  -> LIO (Chan (PeerMessage i o s))

connection addr = do
    chan <- lift newChan
    forkC ("connection to: " ++ show addr) $
      handle chan Nothing
    return chan
  where
    handle :: (Binary i, Binary o, Binary s)
      => Chan (PeerMessage i o s)
      -> Maybe Socket
      -> LIO ()
    handle chan so =
      lift (readChan chan) >>= sendWithRetry so . encode >>= handle chan

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
    sendWithRetry :: Maybe Socket -> ByteString -> LIO (Maybe Socket)
    sendWithRetry Nothing payload = do
      result <- (lift . try) openSocket
      case result of
        Left err -> do
          $(logWarn) . pack
            $ "Can't connect to: " ++ show addr ++ ". Dropping message on "
            ++ "the floor: " ++ show payload ++ ". The error was: "
            ++ show (err :: SomeException)
          return Nothing
        Right so -> do
          result2 <- (lift . try) (sendAll so payload)
          case result2 of
            Left err -> $(logWarn) . pack
              $ "An error happend when trying to send a payload over a socket "
              ++ "to the address: " ++ show addr ++ ". The error was: "
              ++ show (err :: SomeException) ++ ". This is the last straw, we "
              ++ "are not retrying. The message is being dropped on the floor. "
              ++ "The message was: " ++ show payload
            Right _ -> return ()
          return (Just so)
    sendWithRetry (Just so) payload = do
      result <- (lift . try) (sendAll so payload)
      case result of
        Left err -> do
          $(logInfo) . pack
            $ "Socket to " ++ show addr ++ " died. Retrying on a new "
            ++ "socket. The error was: " ++ show (err :: SomeException)
          (lift . void) (try (close so) :: IO (Either SomeException ()))
          sendWithRetry Nothing payload
        Right _ ->
          return (Just so)


{- |
  Send a message to a peer. Returns the id of the message that was sent.
-}
send :: (Show i, Show o, Show s)
  => ConnectionManager i o s
  -> Peer
  -> PeerMessagePayload i o s
  -> LIO MessageId
send (C chan) peer msg = do
  mid <- lift $ do
    mvar <- newEmptyMVar
    writeChan chan (Send peer msg (lift . putMVar mvar))
    takeMVar mvar
  $(logDebug) . pack
    $ "Sent " ++ show msg ++ " to peer: " ++ show peer
    ++ "with messageId: " ++ show mid
  return mid


{- |
  Forward a pre-built message to a peer.
-}
forward
  :: ConnectionManager i o s
  -> Peer
  -> PeerMessage i o s
  -> LIO ()
forward (C chan) peer = lift . writeChan chan . Forward peer


{- |
  Tell the connection manager about a new peer.
-}
newPeer
  :: ConnectionManager i o s
  -> Peer
  -> SockAddr
  -> LIO ()
newPeer (C chan) peer addr = lift $ writeChan chan (NewPeer peer addr)


{- |
  Tell the connection manager about all the peers known to the cluster state.
-}
newPeers :: ConnectionManager i o s -> Map Peer BSockAddr -> LIO ()
newPeers cm peers =
    mapM_ oneNewPeer (toList peers)
  where
    oneNewPeer (peer, BSockAddr addy) = newPeer cm peer addy


{- |
  The internal state of the connection manager.
-}
data State i o s = S {
    nextId :: MessageId,
    connections :: Map Peer (Chan (PeerMessage i o s))
  }


{- |
  The type of messages sent to us from other peers.
-}
data PeerMessage i o s = PeerMessage {
    source :: Peer,
    messageId :: MessageId,
    payload :: PeerMessagePayload i o s
  }
  deriving (Generic, Show)
instance (Binary i, Binary o, Binary s) => Binary (PeerMessage i o s)


{- |
  The data contained within a peer message.

  When we get around to implementing durability and data replication,
  the sustained inability to confirm that a node has received one of
  these messages should result in the ejection of that node from the
  cluster and the blacklisting of that node so that it can never re-join.
-}
data PeerMessagePayload i o s
  = PartitionMerge PartitionKey (PartitionPowerState i s)
  | ForwardRequest PartitionKey i
  | ForwardResponse MessageId o
  | ClusterMerge ClusterPowerState
  deriving (Generic, Show)
instance (Binary i, Binary o, Binary s) => Binary (PeerMessagePayload i o s)


{- |
  The types of messages that the ConnectionManager understands.
-}
data Message i o s
  = Send Peer (PeerMessagePayload i o s) (MessageId -> LIO ())
  | NewPeer Peer SockAddr
  | Forward Peer (PeerMessage i o s)


{- |
  Guess the family of a `SockAddr`.
-}
fam :: SockAddr -> Family
fam SockAddrInet {} = AF_INET
fam SockAddrInet6 {} = AF_INET6
fam SockAddrUnix {} = AF_UNIX
fam SockAddrCan {} = AF_CAN


{- |
  Initialize a new sequence of messageIds
-}
newSequence ::  LIO MessageId
newSequence = lift $ do
  sid <- getUUID
  return (M sid 0)


{- |
  Make a UUID, no matter what.
-}
getUUID :: IO UUID
getUUID = nextUUID >>= maybe (wait >> getUUID) return
  where
    wait = threadDelay oneMillisecond
    oneMillisecond = 1000


{- |
  Generate the next message id in the sequence. We would use `succ`, but making
  `MessageId` an  instance of `Enum` really isn't appropriate.
-}
next :: MessageId -> MessageId
next (M sequenceId ord) = M sequenceId (ord + 1)


data MessageId = M UUID Word64 deriving (Generic, Show, Eq, Ord)
instance Binary MessageId


