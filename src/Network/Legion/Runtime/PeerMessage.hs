{-# LANGUAGE DeriveGeneric #-}
{- |
  This module contains the type of runtime messages that can be exchanged
  between peers.
-}
module Network.Legion.Runtime.PeerMessage (
  PeerMessage(..),
  PeerMessagePayload(..),
  MessageId,
  newSequence,
  nextMessageId,
) where

import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary)
import Data.UUID (UUID)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Legion.ClusterState (ClusterPowerState)
import Network.Legion.Distribution (Peer)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState)
import Network.Legion.UUID (getUUID)


{- |
  The type of messages sent between peers.
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


data MessageId = M UUID Word64 deriving (Generic, Show, Eq, Ord)
instance Binary MessageId


{- |
  Initialize a new sequence of messageIds. It would be perfectly fine to ensure
  unique message ids by generating a unique UUID for each one, but generating
  UUIDs is not free, and we are probably going to be generating a lot of these.
-}
newSequence ::  LIO MessageId
newSequence = lift $ do
  sid <- getUUID
  return (M sid 0)


{- |
  Generate the next message id in the sequence. We would normally use
  `succ` for this kind of thing, but making `MessageId` an instance of
  `Enum` really isn't appropriate.
-}
nextMessageId :: MessageId -> MessageId
nextMessageId (M sequenceId ord) = M sequenceId (ord + 1)


