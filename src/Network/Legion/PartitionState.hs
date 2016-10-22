{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{- |
  This module contains types related to the partition state.
-}
module Network.Legion.PartitionState (
  PartitionPropState,
  PartitionPowerState,
  ask,
  mergeEither,
  actions,
  new,
  initProp,
  participating,
  getPowerState,
  delta,
  heartbeat,
  participate,
  projParticipants,
  projected,
  infimum,
  idle,
) where

import Data.Aeson (ToJSON)
import Data.Binary (Binary)
import Data.Default.Class (Default)
import Data.Set (Set)
import Data.Time.Clock (UTCTime)
import Network.Legion.Distribution (Peer)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PowerState (ApplyDelta)
import Network.Legion.Propagation (PropState, PropPowerState)
import qualified Network.Legion.Propagation as P

{- |
  This is an opaque representation of your application's partition state.
  Internally, this represents the complete, nondeterministic set of states the
  partition can be in as a result of concurrency, eventual consistency, and all
  the other distributed systems reasons your partition state might have more
  than one value.

  You can save these guys to disk in your `Network.Legion.Persistence`
  layer by using its `Binary` instance.
-}
newtype PartitionPowerState i o s = PartitionPowerState {
    unPowerState :: PropPowerState PartitionKey s Peer i o
  } deriving (Show, Binary)


{- |
  A reification of `PropState`, representing the propagation state of the
  partition state.
-}
newtype PartitionPropState i o s = PartitionPropState {
    unPropState :: PropState PartitionKey s Peer i o
  } deriving (Eq, Show, ToJSON)


-- {- |
--   A convenient alias for the partition state infimum.
-- -}
-- type PartitionInfimum s = Infimum s Peer


{- |
  Get the projected partition state value.
-}
ask :: (ApplyDelta i o s) => PartitionPropState i o s -> s
ask = P.ask . unPropState


{- |
  Try to merge two partition states.
-}
mergeEither :: (Show i, Show s, ApplyDelta i o s)
  => Peer
  -> PartitionPowerState i o s
  -> PartitionPropState i o s
  -> Either String (PartitionPropState i o s)
mergeEither peer ps prop =
  PartitionPropState <$>
    P.mergeEither peer (unPowerState ps) (unPropState prop)


{- |
  Get the peers which require action (i.e. Send), if any, and the
  powerstate version to send to those peers, and the new propagation
  state that is applicable after those actions have been taken.
-}
actions
  :: PartitionPropState i o s
  -> (Set Peer, PartitionPowerState i o s, PartitionPropState i o s)
actions prop =
  let (peers, ps, newProp) = P.actions (unPropState prop)
  in (peers, PartitionPowerState ps, PartitionPropState newProp)


{- |
  Create a new, default, PartitionPropState.
-}
new :: (Default s)
  => PartitionKey
    {- ^ The power state origin, which is the partition key. -}
  -> Peer
    {- ^ self -}
  -> Set Peer
    {- ^ The default participation. -}
  -> PartitionPropState i o s
new key self = PartitionPropState . P.new key self


{- |
  Initialize a `PartitionPropState` based on the initial underlying
  partition power state.
-}
initProp :: (ApplyDelta i o s)
  => Peer
  -> PartitionPowerState i o s
  -> PartitionPropState i o s
initProp self = PartitionPropState . P.initProp self . unPowerState


{- |
  Return `True` if the local peer is participating in the partition
  power state.
-}
participating :: PartitionPropState i o s -> Bool
participating = P.participating . unPropState


{- |
  Get an opaque encapsulation of the partition power state, for
  transferring accros the network or whatever.
-}
getPowerState :: PartitionPropState i o s -> PartitionPowerState i o s
getPowerState = PartitionPowerState . P.getPowerState . unPropState


{- | Apply a delta to the partition state.  -}
delta :: (ApplyDelta i o s)
  => i
  -> PartitionPropState i o s
  -> PartitionPropState i o s
delta d = PartitionPropState . P.delta d . unPropState


{- | Move time forward for the propagation state.  -}
heartbeat :: UTCTime -> PartitionPropState i o s -> PartitionPropState i o s
heartbeat now = PartitionPropState . P.heartbeat now . unPropState


{- |
  Allow a participant to join in the distributed nature of the power state.
-}
participate :: (ApplyDelta i o s)
  => Peer
  -> PartitionPropState i o s
  -> PartitionPropState i o s
participate peer = PartitionPropState . P.participate peer . unPropState


{- |
  Return the projected peers which are participating in the partition
  state.
-}
projParticipants :: PartitionPropState i o s -> Set Peer
projParticipants = P.projParticipants . unPropState


{- |
  Get the projected value of a `PartitionPowerState`.
-}
projected :: (ApplyDelta i o s) => PartitionPowerState i o s -> s
projected = P.projected . unPowerState


{- |
  Get the infimum value of a `PartitionPowerState`
-}
infimum :: PartitionPowerState i o s -> s
infimum = P.infimum . unPowerState


{- |
  Figure out if this propagation state has any work to do. Return 'True' if all
  known propagation work has been completed. The implication here is that the
  only way more work can happen is if new deltas are applied, either directly
  or via a merge.
-}
idle :: PartitionPropState i o s -> Bool
idle = P.idle . unPropState


