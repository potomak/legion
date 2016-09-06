{-# LANGUAGE DeriveGeneric #-}
{- |
  This module contains the `KeySet` data type and operations. `KeySet`,
  conceptually, has the same meaning as @Set PartitionKey@, but it is optimized
  for non-sparse continuous ranges of included values that could never fit into
  memory in an actual @Set PartitionKey@.
-}
module Network.Legion.KeySet (
  KeySet,
  take,
  size,
  union,
  unions,
  member,
  (\\),
  empty,
  null,
  fromRange,
  full
) where

import Prelude hiding (take, null)

import Data.Binary (Binary(put, get))
import Data.Ranged (Range(Range), RSet, rSetEmpty, Boundary(BoundaryBelow,
  BoundaryAbove, BoundaryAboveAll, BoundaryBelowAll), makeRangedSet,
  rSetHas, rSetUnion, (-!-), unsafeRangedSet, rSetRanges)
import GHC.Generics (Generic)
import Network.Legion.PartitionKey (PartitionKey(K, unKey))


{- |
  Represents a set of partition keys. This type is intended to have set
  semantics, but unlike `Data.Set.Set`, it performs well with dense sets
  because it only stores the set of continuous ranges in memory.
-}
newtype KeySet = S {unS :: RSet PartitionKey} deriving (Show, Eq)

instance Binary KeySet where
  put =
      put . fmap encodeRange . rSetRanges . unS
    where
      encodeRange
        :: Range PartitionKey
        -> (BinBoundary PartitionKey, BinBoundary PartitionKey)
      encodeRange (Range a b) = (boundaryToBin a, boundaryToBin b)

      boundaryToBin :: Boundary a -> BinBoundary a
      boundaryToBin (BoundaryBelow a) = BinBelow a
      boundaryToBin (BoundaryAbove a) = BinAbove a
      boundaryToBin BoundaryBelowAll = BinBelowAll
      boundaryToBin BoundaryAboveAll = BinAboveAll
  get =
      (S . unsafeRangedSet . fmap decodeRange) <$> get
    where
      decodeRange
        :: (BinBoundary PartitionKey, BinBoundary PartitionKey)
        -> Range PartitionKey
      decodeRange (a, b) = Range (binToBoundary a) (binToBoundary b)

      binToBoundary :: BinBoundary a -> Boundary a
      binToBoundary (BinBelow a) = BoundaryBelow a
      binToBoundary (BinAbove a) = BoundaryAbove a
      binToBoundary BinBelowAll = BoundaryBelowAll
      binToBoundary BinAboveAll = BoundaryAboveAll



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
empty :: KeySet
empty = S rSetEmpty


{- |
  Construct a KeySet containing all keys.
-}
full :: KeySet
full = fromRange minBound maxBound


{- |
  Check if a key range is empty or not.
-}
null :: KeySet -> Bool
null = (0 >=) . size


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
  Take the union of a list of sets.
-}
unions :: [KeySet] -> KeySet
unions = foldr union empty


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
toI = toInteger . unKey


{- |
  Opposite of `toI`
-}
fromI :: Integer -> PartitionKey
fromI = K . fromInteger


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


