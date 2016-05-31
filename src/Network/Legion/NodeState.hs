{- |
  Node State
-}
module Network.Legion.NodeState (
  NodeState(..),
  Forwarded(..),
) where

import Data.Map (Map)
import Network.Legion.ClusterState (ClusterPropState)
import Network.Legion.ConnectionManager (ConnectionManager, MessageId)
import Network.Legion.Distribution (Peer)
import Network.Legion.KeySet (KeySet)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState(PartitionPropState)
import qualified Data.Map as Map


{- | Defines the local state of a node in the cluster.  -}
data NodeState i o s = NodeState {
             self :: Peer,
               cm :: ConnectionManager i o s,
          cluster :: ClusterPropState,
        forwarded :: Forwarded o,
       propStates :: Map PartitionKey (PartitionPropState i s),
        migration :: KeySet
  }
  deriving (Show)


{- | A set of forwardmed messages.  -}
newtype Forwarded o = F {unF :: Map MessageId (o -> LIO ())}
instance Show (Forwarded o) where
  show = show . Map.keys . unF



