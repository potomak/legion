{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{- |
  This module defines the data structures and functions used for handling the
  key space distribution.
-}
module Network.Legion.Distribution (
  PartitionKey(..),
  PartitionDistribution,
  KeySet,
  Replica(..),
  member,
  Peer,
  empty,
  findPartition,
  peerOwns,
  update,
  delete,
  fromRange,
  size,
  rebalanceAction,
  RebalanceAction(..)
) where

import Prelude hiding (lookup, take)

import Control.Applicative ((<$>))
import Data.Binary (Binary(put, get))
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.Function (on)
import Data.List (sortBy)
import Data.Map (Map, toList, lookup, adjust, singleton, unionWith)
import Data.Maybe (fromMaybe)
import Data.Ranged (Range(Range), RSet, rSetEmpty, Boundary(BoundaryBelow,
  BoundaryAbove, BoundaryAboveAll, BoundaryBelowAll), makeRangedSet,
  rSetHas, rSetUnion, (-!-), unsafeRangedSet, rSetRanges,
  DiscreteOrdered(adjacent, adjacentBelow))
import Data.Text (Text)
import GHC.Generics (Generic)
import qualified Data.Map as Map (empty, union, foldr)


{- |
  The distribution of partitions and partition replicas among the cluster.
-}
newtype PartitionDistribution = D {
    unD :: Map Peer (Map Replica KeySet)
  } deriving (Show, Binary)


{- |
  Enumerate the individual replicas.
-}
data Replica = R1 | R2 | R3 deriving (Eq, Ord, Show, Enum, Bounded, Generic)
instance Binary Replica


{- |
  This is how partitions are identified and referenced.
-}
newtype PartitionKey = K {unkey :: Word256} deriving (Eq, Ord, Show, Bounded)

instance Binary PartitionKey where
  put (K (Word256 (Word128 a b) (Word128 c d))) = put (a, b, c, d)
  get = do
    (a, b, c, d) <- get
    return (K (Word256 (Word128 a b) (Word128 c d)))

instance DiscreteOrdered PartitionKey where
  adjacent (K a) (K b) = a < b && succ a == b
  adjacentBelow (K k) = if k == minBound then Nothing else Just (K (pred k))


{- |
  Represents a set of partition keys. This type is intended to have set
  semantics, but unlike `Data.Set.Set`, it performs well with dense sets
  because it only stores the set of continuous ranges in memory.
-}
newtype KeySet = S {unS :: RSet PartitionKey} deriving (Show)

instance Binary KeySet where
  put =
      put . fmap encodeRange . rSetRanges . unS
    where
      encodeRange (Range a b) = (boundaryToBin a, boundaryToBin b)
      boundaryToBin :: Boundary a -> BinBoundary a
      boundaryToBin (BoundaryBelow a) = BinBelow a
      boundaryToBin (BoundaryAbove a) = BinAbove a
      boundaryToBin BoundaryBelowAll = BinBelowAll
      boundaryToBin BoundaryAboveAll = BinAboveAll
  get =
      (S . unsafeRangedSet . fmap decodeRange) <$> get
    where
      decodeRange (a, b) = Range (binToBoundary a) (binToBoundary b)
      binToBoundary :: BinBoundary a -> Boundary a
      binToBoundary (BinBelow a) = BoundaryBelow a
      binToBoundary (BinAbove a) = BoundaryAbove a
      binToBoundary BinBelowAll = BoundaryBelowAll
      binToBoundary BinAboveAll = BoundaryAboveAll


{- |
  Constuct a distribution that contains no partitions.
-}
empty :: PartitionDistribution
empty = D Map.empty


{- |
  Find the peer that owns the specified partition.
-}
findPartition :: PartitionKey -> PartitionDistribution -> Maybe Peer
findPartition k (D d) =
  let list = [(p, ks) | (p, kss) <- toList d, (_r, ks) <- toList kss] in
  case dropWhile (not . member k . snd) list of
    [] -> Nothing
    (p, _):_ -> Just p


{- |
  Find all of they keys that the specified peer owns.
-}
peerOwns
  :: Peer
  -> PartitionDistribution
  -> Map Replica KeySet
peerOwns p (D d) = fromMaybe Map.empty (lookup p d)


{- |
  Update the distribution so that the specified peer owns all of the
  partitions within the specified key set.
-}
update
  :: Peer
  -> Map Replica KeySet
  -> PartitionDistribution
  -> PartitionDistribution
update p r ks =
  (D . unionWith Map.union (singleton p r) . unD)
    (foldr (uncurry delete) ks (toList r))


{- |
  Remove all partitions identified by the key set from the distribution.
-}
delete
  :: Replica
  -> KeySet
  -> PartitionDistribution
  -> PartitionDistribution
delete r ks = D . fmap (adjust (\\ ks) r) . unD


{- |
  Construct the set of all partition keys within the specified range. Both the
  start element and the end element are inclusive.
-}
fromRange :: PartitionKey -> PartitionKey -> KeySet
fromRange a b
  | a > b = fromRange b a
  | otherwise = S (makeRangedSet [Range (BoundaryBelow a) (BoundaryAbove b)])


{- |
  Construct an empty `KeySet`.
-}
emptyKS :: KeySet
emptyKS = S rSetEmpty


{- |
  Take the difference of the two sets.
-}
(\\) :: KeySet -> KeySet -> KeySet
S a \\ S b = S (a -!- b)


{- |
  Test for set membership.
-}
member :: PartitionKey -> KeySet -> Bool
member k = flip rSetHas k . unS


{- |
  Take the union of the two sets.
-}
union :: KeySet -> KeySet -> KeySet
union (S a) (S b) = S (a `rSetUnion` b)


{- |
  Used to help with the Binary instance of KeySet.
-}
data BinBoundary a
  = BinAbove a
  | BinBelow a
  | BinAboveAll
  | BinBelowAll
  deriving (Generic)
instance (Binary a) => Binary (BinBoundary a)


{- |
  Figure out how large a `KeySet` is.
-}
size :: KeySet -> Integer
size = sum . fmap rangeSize . rSetRanges . unS


{- |
  Figure out how large a particular range is.
-}
rangeSize :: Range PartitionKey -> Integer
rangeSize (Range BoundaryBelowAll b) = rangeSize (Range (BoundaryBelow minBound) b)
rangeSize (Range BoundaryAboveAll b) = rangeSize (Range (BoundaryAbove maxBound) b)
rangeSize (Range a BoundaryBelowAll) = rangeSize (Range a (BoundaryBelow minBound))
rangeSize (Range a BoundaryAboveAll) = rangeSize (Range a (BoundaryAbove maxBound))
rangeSize (Range (BoundaryAbove a) (BoundaryAbove b)) = toI b - toI a
rangeSize (Range (BoundaryBelow a) (BoundaryBelow b)) = toI b - toI a
rangeSize (Range (BoundaryAbove a) (BoundaryBelow b)) = (toI b - toI a) - 1
rangeSize (Range (BoundaryBelow a) (BoundaryAbove b)) = (toI b - toI a) + 1


{- |
  To help with `rangeSize`.
-}
toI :: PartitionKey -> Integer
toI = toInteger . unkey


{- |
  Opposite of `toI`
-}
fromI :: Integer -> PartitionKey
fromI = K . fromInteger


{- |
  The way to identify a peer.
-}
type Peer = Text


{- |
  Return the best action, if any, that the indicated peer should take to
  rebalance an unbalanced distribution.
-}
rebalanceAction :: Peer -> PartitionDistribution -> Maybe RebalanceAction
rebalanceAction peer (D dist) = 
    -- TODO: first figure out if any replicas need re-building.
    case sortBy (weight `on` snd) (toList dist) of
      (p, keyspace):remaining@(_:_) | p == peer ->
        let (target, targetSpace) = last remaining 
            {- |
              Keys that already exist at the target, no matter what
              replica, are not eligible for being moved.
            -}
            targetKeys = Map.foldr union emptyKS targetSpace
            eligibleSpace = fmap (\\ targetKeys) keyspace
            migrationSize = (weightOf keyspace - weightOf targetSpace) `div` 2
            migrants = pickMigrants migrationSize eligibleSpace
        in
        case migrants of
          Just (replica, keys) -> Just (Move target replica keys)
          Nothing -> Nothing
      _ -> Nothing
  where
    weight
      :: Map Replica KeySet
      -> Map Replica KeySet
      -> Ordering
    weight = flip compare `on` weightOf

    weightOf :: Map Replica KeySet -> Integer
    weightOf = Map.foldr (+) 0 . fmap size

    pickMigrants :: Integer -> Map Replica KeySet -> Maybe (Replica, KeySet)
    pickMigrants n keyspace =
      case sortBy (flip compare `on` size . snd) (toList keyspace) of
        (r, ks):_ ->
          let migrants = take n ks in
          if size migrants > 0
            then Just (r, migrants)
            else Nothing 
        _ -> Nothing

  -- case sortBy (flip compare `on` ((size . snd) &&& fst)) (toList (unD dist)) of
  --   (p, keyspace):remaining@(_:_) | p == peer -> 
  --     let (target, targetSpace) = last remaining in
  --     -- Add 100 to give some wiggle room for remainders, etc.
  --     if size keyspace > (size targetSpace + 100)
  --       then Just $
  --         Move
  --           target
  --           (take ((size keyspace - size targetSpace) `div` 2) keyspace)
  --       else Nothing
  --   _ -> Nothing


{- |
  The actions that are taken in order to build a balanced cluster.
-}
data RebalanceAction
  = Move Peer Replica KeySet
  deriving (Show)


{- |
  Take the first n values from a KeySet.
-}
take :: Integer -> KeySet -> KeySet
take num set =
    S $ doTake num [] (rSetRanges (unS set))
  where
    doTake 0 acc _ = makeRangedSet acc
    doTake _ acc [] = makeRangedSet acc
    doTake n acc (first:remaining)
        | firstSize <= n =
            doTake (n - firstSize) (acc ++ [first]) remaining
        | otherwise =
            makeRangedSet (acc ++ [takeRange n first])
      where
        firstSize = rangeSize first

    takeRange
      :: Integer
      -> Range PartitionKey
      -> Range PartitionKey
    takeRange n (Range BoundaryBelowAll b) =
      takeRange n (Range (BoundaryBelow minBound) b)
    takeRange n (Range BoundaryAboveAll b) =
      takeRange n (Range (BoundaryAbove minBound) b)
    takeRange n (Range (BoundaryAbove a) _) =
      Range (BoundaryAbove a) (BoundaryAbove (fromI (toI a + n)))
    takeRange n (Range (BoundaryBelow a) _) =
      Range (BoundaryBelow a) (BoundaryBelow (fromI (toI a + n)))


