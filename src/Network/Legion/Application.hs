{-# LANGUAGE ConstraintKinds #-}
{- |
  This module contains the data types necessary for implementing the
  user application.
-}
module Network.Legion.Application (
  LegionConstraints,
  Legionary(..),
  Persistence(..),
) where

import Data.Binary (Binary)
import Data.Conduit (Source)
import Data.Default.Class (Default)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState)
import Network.Legion.PowerState (ApplyDelta)

{- |
  This is a more convenient way to write the somewhat unwieldy set of
  constraints

  > (
  >   ApplyDelta i s, Default s, Binary i, Binary o, Binary s, Show i,
  >   Show o, Show s, Eq i
  > )
-}
type LegionConstraints i o s = (
    ApplyDelta i s, Default s, Binary i, Binary o, Binary s, Show i,
    Show o, Show s, Eq i
  )


{- |
  This is the type of a user-defined Legion application. Implement this and
  allow the Legion framework to manage your cluster.

  - @__i__@ is the type of request your application will handle. @__i__@ stands
    for __"input"__.
  - @__o__@ is the type of response produced by your application. @__o__@ stands
    for __"output"__
  - @__s__@ is the type of state maintained by your application. More
    precisely, it is the type of the individual partitions that make up
    your global application state. @__s__@ stands for __"state"__.
-}
data Legionary i o s = Legionary {
    {- |
      The request handler, implemented by the user to service requests.

      Given a request and a state, returns a response to the request.
    -}
    handleRequest :: PartitionKey -> i -> s -> o,

    {- | The user-defined persistence layer implementation. -}
    persistence :: Persistence i s
  }


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See 'Network.Legion.newMemoryPersistence' or
  'Network.Legion.diskPersistence' if you need to get started quickly.
-}
data Persistence i s = Persistence {
     getState :: PartitionKey -> IO (Maybe (PartitionPowerState i s)),
    saveState :: PartitionKey -> Maybe (PartitionPowerState i s) -> IO (),
         list :: Source IO (PartitionKey, PartitionPowerState i s)
      {- ^
        List all the keys known to the persistence layer. It is important
        that the implementation do the right thing with regard to
        `Data.Conduit.addCleanup`, because there are cases where the
        conduit is terminated without reading the entire list.
      -}
  }


