{- |
  This module contains the PartitionKey type.
-}
module Network.Legion.PartitionKey (
  PartitionKey(..),
  toHex,
  fromHex
) where


import Data.Attoparsec.ByteString (parseOnly, atEnd)
import Data.Attoparsec.ByteString.Char8 (hexadecimal)
import Data.Binary (Binary(put, get))
import Data.Bits (testBit)
import Data.Bool (bool)
import Data.ByteString.Char8 (pack)
import Data.DoubleWord (Word256(Word256), Word128(Word128))
import Data.Ranged (DiscreteOrdered(adjacent, adjacentBelow))
import Data.Word (Word64)


{- | This is how partitions are identified and referenced. -}
newtype PartitionKey = K {unkey :: Word256} deriving (Eq, Ord, Show, Bounded)

instance Binary PartitionKey where
  put (K (Word256 (Word128 a b) (Word128 c d))) = put (a, b, c, d)
  get = do
    (a, b, c, d) <- get
    return (K (Word256 (Word128 a b) (Word128 c d)))

instance DiscreteOrdered PartitionKey where
  adjacent (K a) (K b) = a < b && succ a == b
  adjacentBelow (K k) = if k == minBound then Nothing else Just (K (pred k))


{- | Convert a `PartitionKey` into a hex string. -}
toHex :: PartitionKey -> String
toHex (K (Word256 (Word128 a b) (Word128 c d))) =
  concatMap toHex64 [a, b, c, d]


{- |
  Convert a `Word64` into a hex string.

  I know I'm going to hell for this, but I just can't abide the
  @hexstring@ package pulling @aeson@ into our dependency tree.
-}
toHex64 :: Word64 -> String
toHex64 w = fmap (digit . quad) [15, 14..0]
  where
    quad :: Int -> (Int, Int, Int, Int)
    quad n = let base = n * 4 in (base + 3, base + 2, base + 1, base)

    digit :: (Int, Int, Int, Int) -> Char
    digit (a, b, c, d) =
      case (testBit w a, testBit w b, testBit w c, testBit w d) of
        (False, False, False, False) -> '0'
        (False, False, False, True)  -> '1'
        (False, False, True,  False) -> '2'
        (False, False, True,  True)  -> '3'
        (False, True,  False, False) -> '4'
        (False, True,  False, True)  -> '5'
        (False, True,  True,  False) -> '6'
        (False, True,  True,  True)  -> '7'
        (True,  False, False, False) -> '8'
        (True,  False, False, True)  -> '9'
        (True,  False, True,  False) -> 'a'
        (True,  False, True,  True)  -> 'b'
        (True,  True,  False, False) -> 'c'
        (True,  True,  False, True)  -> 'd'
        (True,  True,  True,  False) -> 'e'
        (True,  True,  True,  True)  -> 'f'


{- | Maybe convert a hex string into a partition key -}
fromHex :: String -> Either String PartitionKey
fromHex str
    | length str > 64 =
        Left "trailing characters while parsing hex PartitionKey"
    | otherwise =
        K <$> parseOnly parser (pack str)
  where
    parser = do
      w <- hexadecimal
      atEnd >>= bool
        (fail "not a valid hex string")
        (return w)


