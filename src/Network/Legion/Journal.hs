{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NamedFieldPuns #-}
{- |
  This module contains the node-state journaling functionality.
-}
module Network.Legion.Journal (
  initJournal,
  readJournal,
  Entry(..)
) where

import Prelude hiding (null, readFile)

import Control.Concurrent (forkIO, newChan, writeChan)
import Control.Monad (void)
import Data.Binary (Binary(get), encode)
import Data.Binary.Get (runGetOrFail, Get)
import Data.Bool (bool)
import Data.ByteString.Lazy (null, readFile, hPut)
import Data.Conduit (($$), ($=))
import Data.Conduit.Binary (sinkHandle)
import Data.Conduit.Serialization.Binary (conduitEncode)
import Data.Map (Map, insert)
import Data.Text (Text)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr, getAddr))
import Network.Legion.Conduit (chanToSource)
import Network.Legion.Distribution (Peer, PartitionDistribution)
import Network.Legion.MessageId (MessageId)
import Network.Socket (SockAddr)
import System.Directory (doesFileExist)
import System.IO (withBinaryFile, IOMode(WriteMode))
import qualified System.Log.Logger as L (warningM)


{- |
  Start the background journaling thread. Returns a way to record entries in
  journal.
-}
initJournal
  :: FilePath
    -- ^ The location in which to store the journal
  -> Peer
    -- ^ The local peer
  -> Map Peer SockAddr
    -- ^ the known peers and their addresses.
  -> PartitionDistribution
    -- ^ the initial key distribution.
  -> MessageId
    -- ^ the initial message id.
  -> IO (Entry -> IO ())
initJournal file self peers keyspace nextId = do
  chan <- newChan
  void . forkIO $ 
    withBinaryFile file WriteMode (\h -> do
        hPut h (encode R {self, peers = fmap BSockAddr peers, keyspace, nextId})
        chanToSource chan $= conduitEncode $$ sinkHandle h
      )
  return (writeChan chan)


{- |
  Read the contents of the journal file and construct a final value for
  the critical node state information.
-}
readJournal
  :: FilePath
  -> IO (Maybe (Text, Map Peer SockAddr, PartitionDistribution, MessageId))
readJournal file =
    doesFileExist file >>= bool (return Nothing) (do
        fileData <- readFile file
        case runGetOrFail getInit fileData of
          Left (_, _, err) -> error
            $ "Malformed or corrupt journal file: " ++ show file
            ++ ". We can't go on like this. The specific error was: " ++ err

          Right (remaining, _, val) -> do
            let (R a b c d, warning) = readAndApply val remaining
            maybe (return ()) warningM warning
            return (Just (a, fmap getAddr b, c, d))
      )
  where
    getInit :: Get RecoveryData
    getInit = get

    getEntry :: Get Entry
    getEntry = get

    apply
      :: Entry
      -> RecoveryData
      -> RecoveryData
    apply (UpdatingPeer peer addy) r@R {peers} =
      r {peers = insert peer addy peers}
    apply (UpdatingNextId nextId) r = r {nextId}
    apply (UpdatingKeyspace keyspace) r = r {keyspace}

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
  This is the type of valid jorunal entries.
-}
data Entry
  = UpdatingPeer Peer BSockAddr
  | UpdatingNextId MessageId
  | UpdatingKeyspace PartitionDistribution
  deriving (Generic)
instance Binary Entry


{- |
  Shorthand logging.
-}
warningM :: String -> IO ()
warningM = L.warningM "legion"


{- |
  The data needed for a proper recovery.
-}
data RecoveryData = R {
    self :: Text,
    peers :: Map Peer BSockAddr,
    keyspace :: PartitionDistribution,
    nextId :: MessageId
  }
  deriving (Generic)
instance Binary RecoveryData


