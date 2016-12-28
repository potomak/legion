{-# LANGUAGE ConstraintKinds #-}
{- |
  This module contains the data types necessary for implementing the
  user application.
-}
module Network.Legion.Application (
  LegionConstraints,
  Persistence(..),
) where

import Data.Aeson (ToJSON)
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
  >   Binary e, Binary o, Binary s, Default s, Eq e, Event e o s, Indexable s,
  >   Show e, Show o, Show s, ToJSON s
  > )

  The @ToJSON s@ requirement is strictly for servicing the admin web
  endpoints.
-}
type LegionConstraints e o s = (
    Binary e, Binary o, Binary s, Default s, Eq e, Event e o s, Indexable s,
    Show e, Show o, Show s, ToJSON s
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
                   List all the keys known to the persistence layer. It is
                   important that the implementation do the right thing
                   with regard to `Data.Conduit.addCleanup`, because
                   there are cases where the conduit is terminated
                   without reading the entire list.
                 -}
  }


