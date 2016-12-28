{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{- |
  This module defines how to propagate a PowerState amoung its participants.
-}
module Network.Legion.Propagation (
  PropState,
  PropPowerState,
  merge,
  mergeMaybe,
  mergeEither,
  heartbeat,
  event,
  actions,
  new,
  initProp,
  getPowerState,
  ask,
  participate,
  disassociate,
  getSelf,
  divergences,
  participating,
  allParticipants,
  projParticipants,
  projected,
  infimum,
  idle,
) where

import Prelude hiding (lookup)

import Data.Aeson (ToJSON, object, (.=), toJSON)
import Data.Binary (Binary)
import Data.Default.Class (Default)
import Data.Map (Map, lookup)
import Data.Maybe (fromMaybe)
import Data.Set (member, Set)
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime)
import Data.Time.Format () -- For `instance Show UTCTime`
import Network.Legion.PowerState (PowerState, divergent, Event,
  acknowledge, projectedValue, StateId)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.PowerState as PS


{- |
  Internally, we use `Maybe UTCTime` to represent the current time, so that we
  have a convenient way to represent "now" (i.e. `Nothing`) without using `IO`.
  This type aliases gives us a convenient way to spell `Maybe UTCTime`.
-}
type Time = Maybe UTCTime


{- |
  Opaque Propagation State. Values of this type encapsulate the
  current value of a power state along with state having to do with
  the distribution of that powerstate among its participants. The
  power state is not directly accessible, but rather must be accessed
  through functions provided by this module. In addition to providing
  a more coherent hierarchy of abstraction, this also helps ensure that
  the power state remains consistent with the state of its propagation
  throughout the network.
-}
data PropState o s p e r = PropState {
    powerState :: PowerState o s p e r,
    peerStates :: Map p PeerStatus,
          self :: p,
           now :: Time
  } deriving (Eq, Show)
instance (Show o, Show s, Show p, Show e) => ToJSON (PropState o s p e r) where
  toJSON PropState {powerState, peerStates, self, now} = object [
      "powerState" .= powerState,
      "peerStates" .= Map.fromList [
          (show p, show s)
          | (p, s) <- Map.toList peerStates
        ],
      "self" .= show self,
      "now" .= show now
    ]


{- |
  This type is an opaque representation of the underlying power state. It
  exists because we sometimes want to pack up the power state and ship
  it over the network, but we don't want any code outside of this module
  to operate on it.
-}
newtype PropPowerState o s p e r = PropPowerState {
    unPowerState :: PowerState o s p e r
  } deriving (Show, Binary)


{- |
  Retriev the current projected value of the underlying state.
-}
ask :: (Event e r s) => PropState o s p e r -> s
ask = projectedValue . powerState


{- |
  Create a new propagation state based on an existing power state.
-}
initProp :: (Event e r s, Ord p)
  => p
  -> PropPowerState o s p e r
  -> PropState o s p e r
initProp self ps =
  let powerState = acknowledge self (unPowerState ps)
  in PropState {
      powerState = powerState,
      peerStates = Map.fromAscList [
          (p, NeedsSendAt Nothing)
          | p <- Set.toAscList (divergent powerState)
        ],
      self,
      now = Nothing
    }


{- |
  Return an opaque representation of the power state, for transfer across
  the network, or whatever.
-}
getPowerState :: PropState o s p e r -> PropPowerState o s p e r
getPowerState = PropPowerState . powerState


{- |
  The propagation state of a single remote participant.
-}
data PeerStatus
  = NeedsSendAt Time
  | NeedsAck
  deriving (Show, Eq)


{- |
  Create a new propagation state.
-}
new :: (Default s) => o -> p -> Set p -> PropState o s p e r
new origin self participants =
  PropState {
      powerState = PS.new origin participants,
      peerStates = Map.empty,
      self,
      now = Nothing
    }


{- |
  Like `merge`, but total. `mergeEither` returns a human readable reason why
  the foreign powerstate can't be merged in the event of an error.
-}
mergeEither :: (Eq o, Ord p, Show o, Show s, Show p, Show e, Event e r s)
  => p
  -> PropPowerState o s p e r
  -> PropState o s p e r
  -> Either String (PropState o s p e r)
mergeEither source kernel (prop@PropState {powerState, peerStates, self, now}) =
  let ps = unPowerState kernel
  in case acknowledge self <$> PS.mergeEither ps powerState of
    Left err -> Left err
    Right merged -> Right prop {
        powerState = merged,

        {-
          This algorithm is weaksauce. We need to find someone who knows
          a lot about gossip protocols to fix this.
        -}
        peerStates =
          Map.fromList $ [
              (p, ns)
              | p <- Set.toList (divergent merged)
              , let ns = fromMaybe (NeedsSendAt now) (lookup p peerStates)
            ]
          ++
            {-
              If the source of the foreign powerstate believes we
              are divergent, then it is going to keep sending updates
              until someone clues it in. That someone is us for now.
            -}
            [(source, NeedsAck) | self `member` divergent ps]
      }


{- |
  Like `merge`, but total. `mergeMaybe` returns `Nothing` if the foreign power
  state can't be merged.
-}
mergeMaybe :: (Eq o, Ord p, Show o, Show s, Show p, Show e, Event e r s)
  => p
  -> PropPowerState o s p e r
  -> PropState o s p e r
  -> Maybe (PropState o s p e r)
mergeMaybe source ps prop =
  case mergeEither source ps prop of
    Left _ -> Nothing
    Right v -> Just v


{- |
  Try to merge a foreign powerstate. The precondition is that the foreign
  powerstate shares the same origin as the local powerstate. If this
  precondition is not met, `error` will be called (making this function
  non-total). Using `mergeMaybe` or `mergeEither` is recommended.
-}
merge :: (Eq o, Ord p, Show o, Show s, Show p, Show e, Event e r s)
  => p
  -> PropPowerState o s p e r
  -> PropState o s p e r
  -> PropState o s p e r
merge source ps prop =
  case mergeEither source ps prop of
    Left err -> error err
    Right v -> v


{- |
  Time moves forward.
-}
heartbeat :: UTCTime -> PropState o s p e r -> PropState o s p e r
heartbeat newNow prop = prop {now = max (now prop) (Just newNow)}


{- |
  Apply an event.
-}
event :: (Ord p, Event e r s)
  => e
  -> PropState o s p e r
  -> PropState o s p e r
event e prop@PropState {self, powerState, now} =
  let newPowerState = PS.event self e powerState
  in prop {
      powerState = newPowerState,
      peerStates = Map.fromAscList [
          (p, NeedsSendAt now)
          | p <- Set.toAscList (divergent newPowerState)
        ]
    }


{- |
  Get the peers which require action (i.e. Send), if any, and the
  powerstate version to send to those peers, and the new propagation
  state that is applicable after those actions have been taken.
-}
actions :: (Eq p)
  => PropState o s p e r
  -> (Set p, PropPowerState o s p e r, PropState o s p e r)
actions prop@PropState {powerState, peerStates, now} =
    (outOfDatePeers, PropPowerState powerState, newPropState)
  where
    outOfDatePeers = Set.fromAscList [
        p
        | (p, status) <- Map.toAscList peerStates
        , shouldSendNow status
      ]

    shouldSendNow NeedsAck = True
    shouldSendNow (NeedsSendAt time) = now > time

    newPropState = prop {
        peerStates = Map.fromAscList [
            (p, ns)
            {- Careful, this pattern omits `NeedsAck`. This is intentional. -}
            | (p, NeedsSendAt time) <- Map.toAscList peerStates
            , let ns = NeedsSendAt (nextTime time)
          ]
      }

    nextTime :: Time -> Time
    nextTime time =
      if now > time
        then addUTCTime gracePeriod <$> now
        else time


{- |
  The grace period for receiving some response to an action.
-}
gracePeriod :: NominalDiffTime
gracePeriod = oneMinute
  where
    oneMinute = 60


{- |
  Allow a participant to join in the distributed nature of the power state.
-}
participate :: (Ord p, Event e r s)
  => p
  -> PropState o s p e r
  -> PropState o s p e r
participate peer prop@PropState {powerState, now} =
  let newPowerState = PS.participate peer powerState
  in prop {
      powerState = newPowerState,
      peerStates = Map.fromAscList [
          (p, NeedsSendAt now)
          | p <- Set.toAscList (divergent newPowerState)
        ]
    }


{- |
  Eject a participant from the power state.
-}
disassociate :: (Ord p, Event e r s)
  => p
  -> PropState o s p e r
  -> PropState o s p e r
disassociate peer prop@PropState {powerState, now} =
  let newPowerState = PS.disassociate peer powerState
  in prop {
      powerState = newPowerState,
      peerStates = Map.fromAscList [
          (p, NeedsSendAt now)
          | p <- Set.toAscList (divergent newPowerState)
        ]
    }


{- |
  Return the events that are unknown to the specified peer.
-}
divergences :: (Ord p) => p -> PropState o s p e r -> Map (StateId p) e
divergences peer = PS.divergences peer . powerState


{- |
  Return self.
-}
getSelf :: PropState o s p e r -> p
getSelf = self


{- |
  Return `True` if the local peer is participating in the underlying
  power state. This will return `True` even if the peer is projected
  for removal, because until the infimum catches up to that projection,
  this peer still has an obligation to participate.
-}
participating :: (Ord p) => PropState o s p e r -> Bool
participating PropState{self, powerState} =
  self `member` PS.allParticipants powerState


{- |
  Get all known participants. This includes participants that are
  projected for removal.
-}
allParticipants :: (Ord p) => PropState o s p e r -> Set p
allParticipants = PS.allParticipants . powerState


{- |
  Get all of the projected participants.
-}
projParticipants :: (Ord p) => PropState o s p e r -> Set p
projParticipants = PS.projParticipants . powerState


{- |
  Get the projected value of a PropPowerState.
-}
projected :: (Event e r s) => PropPowerState o s p e r -> s
projected = PS.projectedValue . unPowerState


{- |
  Get the infimum value of the PropPowerState.
-}
infimum :: PropPowerState o s p e r -> s
infimum = PS.infimumValue . unPowerState


{- |
  Figure out if this propagation state has any work to do. Return 'True' if all
  known propagation work has been completed. The implication here is that the
  only way more work can happen is if new events are applied, either directly
  or via a merge.
-}
idle :: (Ord p) => PropState o s p e r -> Bool
idle PropState {powerState, peerStates} =
  Map.null peerStates && Set.null (divergent powerState)


