{-# LANGUAGE ConstraintKinds #-}
{- |
  This module contains the data types necessary for implementing the
  user application.
-}
module Network.Legion.Application (
  LegionConstraints,
  Persistence(..),
) where

import Data.Binary (Binary)
import Data.Conduit (Source)
import Data.Default.Class (Default)
import Network.Legion.Index (Indexable)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState)
import Network.Legion.PowerState (Event)

{- |
  This is a more convenient way to write the somewhat unwieldy set of
  constraints

  > (
  >   Event e o s, Default s, Binary e, Binary o, Binary s, Show e,
  >   Show o, Show s, Eq e
  > )
-}
type LegionConstraints e o s = (
    Event e o s, Indexable s, Default s, Binary e, Binary o, Binary s,
    Show e, Show o, Show s, Eq e
  )


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See 'Network.Legion.newMemoryPersistence' or
  'Network.Legion.diskPersistence' if you need to get started quickly.
-}
data Persistence e o s = Persistence {
     getState :: PartitionKey -> IO (Maybe (PartitionPowerState e o s)),
    saveState :: PartitionKey -> Maybe (PartitionPowerState e o s) -> IO (),
         list :: Source IO (PartitionKey, PartitionPowerState e o s)
      {- ^
        List all the keys known to the persistence layer. It is important
        that the implementation do the right thing with regard to
        `Data.Conduit.addCleanup`, because there are cases where the
        conduit is terminated without reading the entire list.
      -}
  }


