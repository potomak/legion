{-# LANGUAGE MultiParamTypeClasses #-}
{- |
  This module contains the data types necessary for implementing the
  user application.
-}
module Network.Legion.Application (
  LegionConstraints,
  Legionary(..),
  Persistence(..),
  RequestMsg,
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
class (
    ApplyDelta i s, Default s, Binary i, Binary o, Binary s, Show i,
    Show o, Show s, Eq i
  ) => LegionConstraints i o s where


{- |
  This is the type of a user-defined Legion application. Implement this and
  allow the Legion framework to manage your cluster.
-}
data Legionary i o s = Legionary {
    {- |
      The request handler, implemented by the user to service requests.

      Returns a response to the request, together with the new partitoin
      state.
    -}
    handleRequest :: PartitionKey -> i -> s -> o,
    {- |
      The user-defined persistence layer implementation.
    -}
    persistence :: Persistence i s
  }


{- |
  The type of a user-defined persistence strategy used to persist
  partition states. See 'Network.Legion.newMemoryPersistence' or
  'Network.Legion.diskPersistence' if you need to get started quicky.
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


{- |
  This is how requests are packaged when they are sent to the legion framework
  for handling. It includes the request information itself, a partition key to
  which the request is directed, and a way for the framework to deliver the
  response to some interested party.

  Unless you know exactly what you are doing, you will have used
  'Network.Legion.forkLegionary' instead of 'Network.Legion.runLegionary'
  to run the framework, in which case you can safely ignore the existence
  of this type.
-}
type RequestMsg i o = ((PartitionKey, i), o -> IO ())

