{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{- | This module contains types related to partition indexing. -}
module Network.Legion.Index (
  Tag(..),
  IndexRecord(..),
  Indexable(..),
  SearchTag(..),
) where

import Data.Aeson (ToJSON, toJSON, object, (.=))
import Data.Binary (Binary)
import Data.ByteString (ByteString)
import Data.Set (Set)
import Data.String (IsString)
import Data.Text.Encoding (decodeUtf8)
import GHC.Generics (Generic)
import Network.Legion.PartitionKey (PartitionKey, unKey)


{- | This typeclass provides the ability to index partition states. -}
class Indexable s where
  {- |
    A way of indexing partitions so that they can be found without knowing
    the partition key. An index entry for the partition will be created
    under each of the tags returned by this function.
  -}
  indexEntries :: s -> Set Tag


{- |
  A tag is a value associated with a partition state that can be used
  to look up a partition key.
-}
newtype Tag = Tag {unTag :: ByteString}
  deriving (Eq, Ord, Show, Binary, IsString)


{- | This data structure describes a record in the index.  -}
data IndexRecord = IndexRecord {
    irTag :: Tag,
    irKey :: PartitionKey
  }
  deriving (Eq, Ord, Show, Generic)
instance Binary IndexRecord
instance ToJSON IndexRecord where
  toJSON (IndexRecord tag key) = object [
      (decodeUtf8 . unTag) tag .= toInteger (unKey key)
    ]


{- | This data structure describes where in the index to start scrolling. -}
data SearchTag = SearchTag {
    stTag :: Tag,
    stKey :: Maybe PartitionKey
  }
  deriving (Show, Eq, Ord, Generic)
instance Binary SearchTag


