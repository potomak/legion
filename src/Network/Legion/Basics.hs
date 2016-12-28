{-# LANGUAGE NamedFieldPuns #-}
{- |
  This module contains some basis persistence strategies useful for
  testing, or getting started.
-}
module Network.Legion.Basics (
  newMemoryPersistence,
  diskPersistence,
) where

import Prelude hiding (lookup, readFile, writeFile)

import Control.Concurrent.STM (atomically, newTVar, modifyTVar', readTVar,
  TVar)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary, encode, decode)
import Data.Bool (bool)
import Data.ByteString (readFile, writeFile)
import Data.ByteString.Lazy (toStrict, fromStrict)
import Data.Conduit (Source, (=$=), awaitForever, yield)
import Data.Conduit.List (sourceList)
import Data.Either (rights)
import Data.Map (Map, insert, lookup)
import Network.Legion.Application (Persistence(Persistence, getState,
  saveState, list))
import Network.Legion.PartitionKey (PartitionKey, toHex, fromHex)
import Network.Legion.PartitionState(PartitionPowerState)
import System.Directory (removeFile, doesFileExist, getDirectoryContents)
import qualified Data.Map as Map


{- |
  A convenient memory-based persistence layer. Good for testing or for
  applications (like caches) that don't have durability requirements.
-}
newMemoryPersistence :: IO (Persistence e o s)

newMemoryPersistence = do
    cacheT <- atomically (newTVar Map.empty)
    return Persistence {
        getState = fetchState cacheT,
        saveState = saveState_ cacheT,
        list = list_ cacheT
      }
  where
    saveState_
      :: TVar (Map PartitionKey (PartitionPowerState e o s))
      -> PartitionKey
      -> Maybe (PartitionPowerState e o s)
      -> IO ()
    saveState_ cacheT key (Just state) =
      (atomically . modifyTVar' cacheT . insert key) state

    saveState_ cacheT key Nothing =
      (atomically . modifyTVar' cacheT . Map.delete) key

    fetchState
      :: TVar (Map PartitionKey (PartitionPowerState e o s))
      -> PartitionKey
      -> IO (Maybe (PartitionPowerState e o s))
    fetchState cacheT key = atomically $
      lookup key <$> readTVar cacheT

    list_
      :: TVar (Map PartitionKey (PartitionPowerState e o s))
      -> Source IO (PartitionKey, PartitionPowerState e o s)
    list_ cacheT =
      sourceList . Map.toList =<< lift (atomically (readTVar cacheT))


{- | A convenient way to persist partition states to disk.  -}
diskPersistence :: (Binary e, Binary s)
  => FilePath
    -- ^ The directory under which partition states will be stored.
  -> Persistence e o s

diskPersistence directory = Persistence {
      getState,
      saveState,
      list
    }
  where
    getState :: (Binary e, Binary s)
      => PartitionKey
      -> IO (Maybe (PartitionPowerState e o s))
    getState key =
      let path = toPath key in
      doesFileExist path >>= bool
        (return Nothing)
        ((Just . decode . fromStrict) <$> readFile path)

    saveState :: (Binary e, Binary s)
      => PartitionKey
      -> Maybe (PartitionPowerState e o s)
      -> IO ()
    saveState key (Just state) =
      writeFile (toPath key) (toStrict (encode state))
    saveState key Nothing =
      let path = toPath key in
      doesFileExist path >>= bool
        (return ())
        (removeFile path)

    list :: (Binary e, Binary s)
      => Source IO (PartitionKey, PartitionPowerState e o s)
    list = do
        keys <- lift $ readHexList <$> getDirectoryContents directory
        sourceList keys =$= fillData
      where
        fillData = awaitForever (\key -> do
            let path = toPath key
            state <- lift ((decode . fromStrict) <$> readFile path)
            yield (key, state)
          )
        readHexList = rights . fmap fromHex . filter notSys
        notSys = not . (`elem` [".", ".."])

    {- |
      Convert a key to a path
    -}
    toPath :: PartitionKey -> FilePath
    toPath key = directory ++ "/" ++ toHex key


