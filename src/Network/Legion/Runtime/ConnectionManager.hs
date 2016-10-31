{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  This module manages connections to other nodes in the cluster.
-}
module Network.Legion.Runtime.ConnectionManager (
  ConnectionManager,
  newConnectionManager,
  send,
  newPeers,
) where

import Prelude hiding (lookup)

import Control.Concurrent (Chan, writeChan, newChan, readChan)
import Control.Exception (try, SomeException)
import Control.Monad (void)
import Control.Monad.Logger (logInfo, logWarn)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary, encode)
import Data.ByteString.Lazy (ByteString)
import Data.Map (toList, insert, empty, Map, lookup)
import Data.Text (pack)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.Distribution (Peer)
import Network.Legion.Fork (forkC)
import Network.Legion.LIO (LIO)
import Network.Legion.Runtime.PeerMessage (PeerMessage)
import Network.Socket (SockAddr, Socket, socket, SocketType(Stream),
  defaultProtocol, connect, close, SockAddr(SockAddrInet, SockAddrInet6,
  SockAddrUnix, SockAddrCan), Family(AF_INET, AF_INET6, AF_UNIX, AF_CAN))
import Network.Socket.ByteString.Lazy (sendAll)

{- |
  A handle on the connection manager
-}
data ConnectionManager e o s = C (Chan (Message e o s))
instance Show (ConnectionManager e o s) where
  show _ = "ConnectionManager"


{- |
  Create a new connection manager.
-}
newConnectionManager :: (Binary e, Binary o, Binary s)
  => Map Peer BSockAddr
  -> LIO (ConnectionManager e o s)
newConnectionManager initPeers = do
    chan <- lift newChan
    forkC "connection manager thread" $
      manager chan S {connections = empty}
    let cm = C chan
    newPeers cm initPeers
    return cm
  where
    manager :: (Binary s, Binary o, Binary e)
      => Chan (Message e o s)
      -> State e o s
      -> LIO ()
    manager chan state = lift (readChan chan) >>= handle state >>= manager chan

    handle :: (Binary e, Binary o, Binary s)
      => State e o s
      -> Message e o s
      -> LIO (State e o s)
    handle s@S {connections} (NewPeer peer addr) =
      case lookup peer connections of
        Nothing -> do
          conn <- connection addr
          return s {
              connections = insert peer conn connections
            }
        Just _ ->
          return s

    handle s@S {connections} (Send peer msg) = do
      case lookup peer connections of
        Nothing -> $(logWarn) . pack $ "unknown peer: " ++ show peer
        Just conn -> lift $ writeChan conn msg
      return s


{- |
  Build a new connection.
-}
connection :: (Binary e, Binary o, Binary s)
  => SockAddr
  -> LIO (Chan (PeerMessage e o s))

connection addr = do
    chan <- lift newChan
    forkC ("connection to: " ++ show addr) $
      handle chan Nothing
    return chan
  where
    handle :: (Binary e, Binary o, Binary s)
      => Chan (PeerMessage e o s)
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
    sendWithRetry Nothing payload =
      (lift . try) openSocket >>= \case
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
    sendWithRetry (Just so) payload =
      (lift . try) (sendAll so payload) >>= \case
        Left err -> do
          $(logInfo) . pack
            $ "Socket to " ++ show addr ++ " died. Retrying on a new "
            ++ "socket. The error was: " ++ show (err :: SomeException)
          (lift . void) (try (close so) :: IO (Either SomeException ()))
          sendWithRetry Nothing payload
        Right _ ->
          return (Just so)


{- |
  Send a message to a peer.
-}
send
  :: ConnectionManager e o s
  -> Peer
  -> PeerMessage e o s
  -> LIO ()
send (C chan) peer = lift . writeChan chan . Send peer


{- |
  Tell the connection manager about a new peer.
-}
newPeer
  :: ConnectionManager e o s
  -> Peer
  -> SockAddr
  -> LIO ()
newPeer (C chan) peer addr = lift $ writeChan chan (NewPeer peer addr)


{- |
  Tell the connection manager about all the peers known to the cluster state.
-}
newPeers :: ConnectionManager e o s -> Map Peer BSockAddr -> LIO ()
newPeers cm peers =
    mapM_ oneNewPeer (toList peers)
  where
    oneNewPeer (peer, BSockAddr addy) = newPeer cm peer addy


{- |
  The internal state of the connection manager.
-}
data State e o s = S {
    connections :: Map Peer (Chan (PeerMessage e o s))
  }


{- |
  The types of messages that the ConnectionManager understands.
-}
data Message e o s
  = NewPeer Peer SockAddr
  | Send Peer (PeerMessage e o s)


{- |
  Guess the family of a `SockAddr`.
-}
fam :: SockAddr -> Family
fam SockAddrInet {} = AF_INET
fam SockAddrInet6 {} = AF_INET6
fam SockAddrUnix {} = AF_UNIX
fam SockAddrCan {} = AF_CAN


