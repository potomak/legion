{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{- |
  This module contains the fundamental distributed data object.
-}
module Network.Legion.PowerState (
  PowerState,
  Infimum(..),
  ApplyDelta(..),
  StateId,
  new,
  merge,
  mergeMaybe,
  mergeEither,
  acknowledge,
  participate,
  disassociate,
  projectedValue,
  infimumValue,
  infimumParticipants,
  allParticipants,
  projParticipants,
  divergent,
  divergences,
  delta,
) where

import Prelude hiding (null)

import Data.Binary (Binary(put, get))
import Data.Default.Class (Default(def))
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.Map (Map, filterWithKey, unionWith, minViewWithKey, keys,
  toDescList, toAscList, fromAscList)
import Data.Set (Set, union, (\\), null, member)
import Data.Word (Word64)
import GHC.Generics (Generic)
import qualified Data.Map as Map
import qualified Data.Set as Set


{- |
  This represents the set of all possible future values of @s@, in a
  distributed, monotonically increasing environment.
-}
data PowerState o s p d = PowerState {
     origin :: o,
    infimum :: Infimum s p,
     deltas :: Map (StateId p) (Delta p d, Set p)
  } deriving (Generic, Show, Eq)
instance (Binary o, Binary s, Binary p, Binary d) => Binary (PowerState o s p d)


{- |
  `Infimum` is the infimum, or greatest lower bound, of the possible
  values of @s@.
-}
data Infimum s p = Infimum {
         stateId :: StateId p,
    participants :: Set p,
      stateValue :: s
  } deriving (Generic, Show)
instance (Binary s, Binary p) => Binary (Infimum s p)
instance (Eq p) => Eq (Infimum s p) where
  Infimum s1 _ _ == Infimum s2 _ _ = s1 == s2
instance (Ord p) => Ord (Infimum s p) where
  compare (Infimum s1 _ _) (Infimum s2 _ _) = compare s1 s2


{- |
  `StateId` is a monotonically increasing, totally ordered identification
  value which allows us to lend the attribute of monotonicity to state
  operations which would not naturally be monotonic.
-}
data StateId p
  = BottomSid
  | Sid Word256 p
  deriving (Generic, Eq, Ord, Show)
instance (Binary p) => Binary (StateId p) where
  put = put . toMaybe
    where
      toMaybe :: StateId p -> Maybe (Word64, Word64, Word64, Word64, p)
      toMaybe BottomSid =
        Nothing
      toMaybe (Sid (Word256 (Word128 a b) (Word128 c d)) p) =
        Just (a, b, c, d, p)
  get = do
    theThing <- get
    return $ case theThing of
      Nothing -> BottomSid
      Just (a, b, c, d, p) -> Sid (Word256 (Word128 a b) (Word128 c d)) p
instance Default (StateId p) where
  def = BottomSid


{- |
  `Delta` is how we represent mutations to the power state.
-}
data Delta p d
  = Join p
  | UnJoin p
  | Delta d
  deriving (Generic, Show, Eq)
instance (Binary p, Binary d) => Binary (Delta p d)


{- |
  The class which allows for delta application.
-}
class ApplyDelta i s where
  {- |
    Apply a delta to a state value. *This function MUST be total!!!*
  -}
  apply :: i -> s -> s


{- |
  Construct a new PowerState with the given origin and initial participants
-}
new :: (Default s) => o -> Set p -> PowerState o s p d
new origin participants =
  PowerState {
      origin,
      infimum = Infimum {
          stateId = def,
          participants,
          stateValue = def
        },
      deltas = Map.empty
    }


{- |
  Monotonically merge the information in two power states.  The resulting
  power state may have a higher infimum value, but it will never have
  a lower one. This function is not total. Only `PowerState`s that originated
  from the same `new` call can be merged.
-}
merge :: (Eq o, ApplyDelta d s, Ord p, Show o, Show s, Show p, Show d)
  => PowerState o s p d
  -> PowerState o s p d
  -> PowerState o s p d
merge a b = either error id (mergeEither a b)


{- |
  Like `merge`, but safe. Returns `Nothing` if the two power states do
  not share the same origin.
-}
mergeMaybe :: (Eq o, ApplyDelta d s, Ord p, Show o, Show s, Show p, Show d)
  => PowerState o s p d
  -> PowerState o s p d
  -> Maybe (PowerState o s p d)
mergeMaybe a b = either (const Nothing) Just (mergeEither a b)


{- |
  Like `mergeMaybe`, but returns a human-decipherable error message of
  exactly what went wrong.
-}
mergeEither :: (Eq o, ApplyDelta d s, Ord p, Show o, Show s, Show p, Show d)
  => PowerState o s p d
  -> PowerState o s p d
  -> Either String (PowerState o s p d)
mergeEither (PowerState o1 i1 d1) (PowerState o2 i2 d2) | o1 == o2 =
    Right . reduce $ PowerState {
        origin = o1,
        infimum,
        deltas = removeObsolete (unionWith mergeAcks d1 d2)
      }
  where
    infimum = max i1 i2

    {- |
      Obsolete deltas are deltas that are already included in the latest
      infimum.
    -}
    removeObsolete = filterWithKey (\k _ -> k > stateId infimum)

    mergeAcks (d, s1) (_, s2) = (d, s1 `union` s2)

mergeEither a b = Left
  $ "PowerStates " ++ show a ++ " and " ++ show b ++ " do not share the "
  ++ "same origin, and cannot be merged."


{- |
  Record the fact that the participant acknowledges the information
  contained in the powerset. The implication is that the participant
  **must** base all future operations on the result of this function.
-}
acknowledge :: (ApplyDelta d s, Ord p)
  => p
  -> PowerState o s p d
  -> PowerState o s p d
acknowledge p ps@PowerState {deltas} =
    reduce ps {deltas = fmap ackOne deltas}
  where
    ackOne (d, acks) = (d, Set.insert p acks)


{- |
  Allow a participant to join in the distributed nature of the power state.
-}
participate :: (ApplyDelta d s, Ord p)
  => p
  -> PowerState o s p d
  -> PowerState o s p d
participate p ps@PowerState {deltas} = acknowledge p $ ps {
    deltas = Map.insert (nextId p ps) (Join p, Set.empty) deltas
  }


{- |
  Indicate that a participant is removing itself from participating in
  the distributed power state.
-}
disassociate :: (ApplyDelta d s, Ord p)
  => p
  -> PowerState o s p d
  -> PowerState o s p d
disassociate p ps@PowerState {deltas} = acknowledge p $ ps {
    deltas = Map.insert (nextId p ps) (UnJoin p, Set.empty) deltas
  }


{- |
  Introduce a change to the PowerState on behalf of the participant.
-}
delta :: (ApplyDelta d s, Ord p)
  => p
  -> d
  -> PowerState o s p d
  -> PowerState o s p d
delta p d ps@PowerState {deltas} = acknowledge p $ ps {
    deltas = Map.insert (nextId p ps) (Delta d, Set.empty) deltas
  }


{- |
  Return the current projected value of the power state.
-}
projectedValue :: (ApplyDelta d s) => PowerState o s p d -> s
projectedValue PowerState {infimum = Infimum {stateValue}, deltas} =
    foldr apply stateValue changes
  where
    changes = foldr getDeltas [] (toDescList deltas)
    getDeltas (_, (Delta d, _)) acc = d:acc
    getDeltas _ acc = acc


{- |
  Return the current infimum value of the power state.
-}
infimumValue :: PowerState o s p d -> s
infimumValue PowerState {infimum = Infimum {stateValue}} = stateValue


{- |
  Gets the known participants at the infimum.
-}
infimumParticipants :: PowerState o s p d -> Set p
infimumParticipants PowerState {infimum = Infimum {participants}} = participants


{- |
  Get all known participants. This includes participants that are
  projected for removal.
-}
allParticipants :: (Ord p) => PowerState o s p d -> Set p
allParticipants PowerState {
    infimum = Infimum {participants},
    deltas
  } =
    foldr updateParticipants participants (toDescList deltas)
  where
    updateParticipants (_, (Join p, _)) = Set.insert p
    updateParticipants _ = id


{- |
  Get all the projected participants. This does not include participants that
  are projected for removal.
-}
projParticipants :: (Ord p) => PowerState o s p d -> Set p
projParticipants PowerState {
    infimum = Infimum {participants},
    deltas
  } =
    foldr updateParticipants participants (toDescList deltas)
  where
    updateParticipants (_, (Join p, _)) = Set.insert p
    updateParticipants (_, (UnJoin p, _)) = Set.delete p
    updateParticipants _ = id


{- |
  Returns the participants that we think might be diverging. In this
  context, a peer is "diverging" if there is a delta that the peer has
  not acknowledged.
-}
divergent :: (Ord p) => PowerState o s p d -> Set p
divergent PowerState {
    infimum = Infimum {participants},
    deltas
  } =
    accum participants Set.empty (toAscList deltas)
  where
    {- |
      `accum` mnemonics:
        j = pro(J)ected participants
        d = (D)ivergent participants
        a = peers that have (A)cknowledged an update.
        p = (P)eer that is joining or unjoining
    -}
    accum _ d [] = d

    accum j d ((_, (Join p, a)):moreDeltas) =
      let
        j2 = Set.insert p j
        d2 = (j2 \\ a) `union` d
      in
        accum j2 d2 moreDeltas

    accum j d ((_, (UnJoin p, a)):moreDeltas) =
      let
        j2 = Set.delete p j
        d2 = (j2 \\ a) `union` d
      in
        accum j2 d2 moreDeltas

    accum j d ((_, (Delta _, a)):moreDeltas) =
      let
        d2 = (j \\ a) `union` d
      in
        accum j d2 moreDeltas


{- |
  Return the deltas that are unknown to the specified peer.
-}
divergences :: (Ord p) => p -> PowerState o s p d -> Map (StateId p) d
divergences peer PowerState {deltas} =
  fromAscList [
    (sid, d)
    | (sid, (Delta d, p)) <- toAscList deltas
    , not (peer `member` p)
  ]


{- |
  This helper function is responsible for figuring out if the power state
  has enough information to derive a new infimum value. In other words,
  this is where garbage collection happens.
-}
reduce :: (ApplyDelta d s, Ord p) => PowerState o s p d -> PowerState o s p d
reduce ps@PowerState {
    infimum = infimum@Infimum {participants, stateValue},
    deltas
  } =
    case minViewWithKey deltas of
      Nothing -> ps
      Just ((i, (update, acks)), newDeltas) ->
        if not . null $ participants \\ acks
          then ps
          else case update of
            Join p -> reduce ps {
                infimum = infimum {
                    stateId = i,
                    participants = Set.insert p participants
                  },
                deltas = newDeltas
              }
            UnJoin p -> reduce ps {
                infimum = infimum {
                    stateId = i,
                    participants = Set.delete p participants
                  },
                deltas = newDeltas
              }
            Delta d -> reduce ps {
                infimum = infimum {
                    stateId = i,
                    stateValue = apply d stateValue
                  },
                deltas = newDeltas
              }


{- |
  A utility function that constructs the next `StateId` on behalf of
  a participant.
-}
nextId :: (Ord p) => p -> PowerState o s p d -> StateId p
nextId p PowerState {infimum = Infimum {stateId}, deltas} =
  case maximum (stateId:keys deltas) of
    BottomSid -> Sid 0 p
    Sid ord _ -> Sid (succ ord) p


