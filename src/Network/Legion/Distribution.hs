{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{- |
  This module defines the data structures and functions used for handling the
  key space distribution.
-}
module Network.Legion.Distribution (
  ParticipationDefaults,
  Peer,
  empty,
  modify,
  findPartition,
  rebalanceAction,
  RebalanceAction(..),
  newPeer,
) where

import Prelude hiding (null)

import Data.Binary (Binary)
import Data.Function (on)
import Data.List (sort, sortBy)
import Data.Set (Set, toList)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import Network.Legion.KeySet (KeySet, member, (\\), null)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.UUID (getUUID)
import qualified Data.Set as Set
import qualified Network.Legion.KeySet as KS


{- |
  The way to identify a peer.
-}
newtype Peer = Peer UUID deriving (Show, Binary, Eq, Ord)


{- |
  The distribution of partitions and partition replicas among the cluster.
-}
newtype ParticipationDefaults = D {
    unD :: [(KeySet, Set Peer)]
  } deriving (Show, Binary)


{- |
  Constuct a distribution that contains no partitions.
-}
empty :: ParticipationDefaults

empty = D []


{- |
  Find the peers that own the specified partition.
-}
findPartition :: PartitionKey -> ParticipationDefaults -> Set Peer

findPartition k d =
  case [ps | (ks, ps) <- unD d, k `member` ks] of
    [ps] -> ps
    _ -> error
      $ "No exact mach for key in distribution. This means there is a bug in "
      ++ "the module `Network.Legion.Distribution`. Please report this bug "
      ++ "via github: " ++ show (k, d)


{- |
  Modify the default participation for the key set.
-}
modify
  :: (Set Peer -> Set Peer)
  -> KeySet
  -> ParticipationDefaults
  -> ParticipationDefaults

modify fun keyset =
    {-
      doModify can produce key ranges that contain zero keys, which is
      why the `filter`.
    -}
    D . filter (not . null . fst) . doModify keyset . unD
  where
    doModify ks [] = [(ks, fun Set.empty)]
    doModify ks ((r, ps):dist) =
      let {
        unaffected = r \\ ks;
          affected = r \\ unaffected;
         remaining = ks \\ affected;
      } in
      (unaffected, ps):(affected, fun ps):doModify remaining dist


{- |
  Return the best action, if any, that the indicated peer should take to
  rebalance an unbalanced distribution.
-}
rebalanceAction
  :: Peer
  -> Set Peer
  -> ParticipationDefaults
  -> Maybe RebalanceAction
rebalanceAction self allPeers (D dist) =
    rebuild
    {- TODO rebalance -}
  where
    _rebalance :: a
    _rebalance = error "rebalance undefined"
    rebuild =
      let
        underserved = [
            (ks, ps)
            | (ks, ps) <- dist
            , Set.size ps < 3
            , not (self `Set.member` ps)
          ]
        mostUnderserved = sortBy (compare `on` Set.size . snd) underserved
      in case mostUnderserved of
        [] -> Nothing
        (ks, ps):_ -> 
          let
            candidateHosts = toList (allPeers Set.\\ ps)
            bestHosts = sort [(weightOf p, p) | p <- candidateHosts]
          in case bestHosts of
            {- we are the best host -}
            (_, candidate):_ | candidate == self -> Just (Invite ks)
            _ -> Nothing

    weightOf p = sum [KS.size ks | (ks, ps) <- dist, p `Set.member` ps]

--     -- TODO: first figure out if any replicas need re-building.
--     case sortBy (weight `on` snd) (toList dist) of
--       (p, keyspace):remaining | p == peer ->
--         case reverse remaining of
--           [] -> Nothing
--           (target, targetSpace):_ ->
--             let
--                 {- |
--                   Keys that already exist at the target are not eligible
--                   for being moved.
--                 -}
--                 eligibleSpace = keyspace \\ targetSpace
--                 migrationSize = (size keyspace - size targetSpace) `div` 2
--                 migrants = pickMigrants migrationSize eligibleSpace
--             in
--             case migrants of
--               Just keys -> Just (Move target keys)
--               Nothing -> Nothing
--       _ -> Nothing
--   where
--     weight
--       :: KeySet
--       -> KeySet
--       -> Ordering
--     weight = flip compare `on` size
-- 
--     pickMigrants :: Integer -> KeySet -> Maybe KeySet
--     pickMigrants n keyspace =
--       let migrants = take n keyspace in
--       if size migrants > 0
--         then Just migrants
--         else Nothing 


{- |
  The actions that are taken in order to build a balanced cluster.
-}
data RebalanceAction
  = Invite KeySet
  deriving (Show, Generic)
instance Binary RebalanceAction


{- |
  Create a new peer.
-}
newPeer :: LIO Peer
newPeer = Peer <$> getUUID


-- {- |
--   Trace helper
-- -}
-- t :: (Show a) => String -> a -> a
-- t msg a = trace (msg ++ ": " ++ show a) a


