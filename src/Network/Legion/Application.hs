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
import Network.Legion.PowerState (ApplyDelta)

{- |
  This is a more convenient way to write the somewhat unwieldy set of
  constraints

  > (
  >   ApplyDelta i o s, Default s, Binary i, Binary o, Binary s, Show i,
  >   Show o, Show s, Eq i
  > )
-}
type LegionConstraints i o s = (
    ApplyDelta i o s, Indexable s, Default s, Binary i, Binary o, Binary s,
    Show i, Show o, Show s, Eq i
  )


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See 'Network.Legion.newMemoryPersistence' or
  'Network.Legion.diskPersistence' if you need to get started quickly.
-}
data Persistence i o s = Persistence {
     getState :: PartitionKey -> IO (Maybe (PartitionPowerState i o s)),
    saveState :: PartitionKey -> Maybe (PartitionPowerState i o s) -> IO (),
         list :: Source IO (PartitionKey, PartitionPowerState i o s)
      {- ^
        List all the keys known to the persistence layer. It is important
        that the implementation do the right thing with regard to
        `Data.Conduit.addCleanup`, because there are cases where the
        conduit is terminated without reading the entire list.
      -}
  }


