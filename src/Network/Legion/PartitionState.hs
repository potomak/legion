{- |
  This module contains types related to the partition state.
-}
module Network.Legion.PartitionState (
  PartitionPowerState,
  PartitionPowerStateT,
) where

import Network.Legion.Distribution (Peer)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PowerState (PowerState)
import Network.Legion.PowerState.Monad (PowerStateT)

{- | A representation of all possible partition states. -}
type PartitionPowerState e o s = PowerState PartitionKey s Peer e o


{- |
  A convenient spelling for the partition-flavored power state monad
  transformer.
-}
type PartitionPowerStateT e o s = PowerStateT PartitionKey s Peer e o


