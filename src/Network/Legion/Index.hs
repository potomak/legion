{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{- | This module contains types related to partition indexing. -}
module Network.Legion.Index (
  Tag(..),
  IndexRecord(..),
  SearchTag(..),
) where

import Data.Binary (Binary)
import Data.ByteString (ByteString)
import Data.String (IsString)
import GHC.Generics (Generic)
import Network.Legion.PartitionKey (PartitionKey)


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


{- | This data structure describes where in the index to start scrolling. -}
data SearchTag = SearchTag {
    stTag :: Tag,
    stKey :: Maybe PartitionKey
  }
  deriving (Show, Eq, Ord, Generic)
instance Binary SearchTag


