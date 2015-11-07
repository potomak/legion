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

import Prelude hiding (lookup, map, null, take)

import Control.Applicative ((<$>))
import Control.Arrow ((&&&))
import Data.Binary (Binary(put, get))
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.Function (on)
import Data.List (sortBy)
import Data.Map (Map, toList, lookup, alter, map, null)
import Data.Maybe (fromMaybe)
import Data.Ranged (Range(Range), RSet, rSetEmpty, Boundary(BoundaryBelow,
  BoundaryAbove, BoundaryAboveAll, BoundaryBelowAll), makeRangedSet,
  rSetHas, rSetUnion, (-!-), unsafeRangedSet, rSetRanges,
  DiscreteOrdered(adjacent, adjacentBelow))
import Data.Text (Text)
import GHC.Generics (Generic)
import qualified Data.Map as Map (empty)


{- |
  The distribution of partitions and partition replicas among the cluster.
-}
newtype PartitionDistribution = D {
    unD :: Map Peer KeySet
  } deriving (Show, Binary)


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
  case dropWhile (not . member k . snd) (toList d) of
    [] -> Nothing
    (p, _):_ -> Just p


{- |
  Find all of they keys that the specified peer owns.
-}
peerOwns
  :: Peer
  -> PartitionDistribution
  -> KeySet
peerOwns p (D d)= fromMaybe (S rSetEmpty) (lookup p d)


{- |
  Update the distribution so that the specified peer owns all of the
  partitions within the specified key set.
-}
update
  :: Peer
  -> KeySet
  -> PartitionDistribution
  -> PartitionDistribution
update p r =
    D . alter addRange p . unD . delete r
  where
    addRange Nothing = Just r
    addRange (Just rs) =
      Just (rs `union` r)


{- |
  Remove all partitions identified by the key set from the distribution.
-}
delete
  :: KeySet
  -> PartitionDistribution
  -> PartitionDistribution
delete ks = D . map (\\ ks) . unD


{- |
  Construct the set of all partition keys within the specified range. Both the
  start element and the end element are inclusive.
-}
fromRange :: PartitionKey -> PartitionKey -> KeySet
fromRange a b
  | a > b = fromRange b a
  | otherwise = S (makeRangedSet [Range (BoundaryBelow a) (BoundaryAbove b)])


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
  Return the best action, if any that the indicated peer should take to
  rebalance an unbalanced keyspace.
-}
rebalanceAction :: Peer -> PartitionDistribution -> Maybe RebalanceAction
rebalanceAction _ dist | null (unD dist) = Nothing
rebalanceAction peer dist =
  case sortBy (flip compare `on` ((size . snd) &&& fst)) (toList (unD dist)) of
    (p, keyspace):remaining@(_:_) | p == peer -> 
      let (target, targetSpace) = last remaining in
      -- Add 100 to give some wiggle room for remainders, etc.
      if size keyspace > (size targetSpace + 100)
        then Just $
          Move
            target
            (take ((size keyspace - size targetSpace) `div` 2) keyspace)
        else Nothing
    _ -> Nothing


{- |
  The actions that are taken in order to build a balanced cluster.
-}
data RebalanceAction = Move Peer KeySet deriving (Show)


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
      | rangeSize first < n =
          doTake (n - rangeSize first) (acc ++ [first]) remaining
      | otherwise =
          makeRangedSet (acc ++ [takeRange n first])

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

