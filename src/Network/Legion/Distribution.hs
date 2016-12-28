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
  minimumCompleteServiceSet,
) where

import Prelude hiding (null)

import Control.Monad.IO.Class (MonadIO)
import Data.Aeson (ToJSON, toJSON, object, (.=))
import Data.Binary (Binary)
import Data.Function (on)
import Data.List (sort, sortBy)
import Data.Map (Map)
import Data.Monoid ((<>))
import Data.Set (Set)
import Data.Text (pack)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import Network.Legion.KeySet (KeySet, member, (\\), null)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.UUID (getUUID)
import Text.Read (readPrec)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.KeySet as KS


{- |
  The way to identify a peer.
-}
newtype Peer = Peer UUID deriving (Show, Binary, Eq, Ord)
instance Read Peer where
  readPrec = Peer <$> readPrec


{- |
  The distribution of partitions and partition replicas among the cluster.
-}
newtype ParticipationDefaults = D {
    unD :: [(KeySet, Set Peer)]
  } deriving (Show, Binary)
instance ToJSON ParticipationDefaults where
  toJSON (D dist) = object [
      pack (show ks) .= Set.map show peers
      | (ks, peers) <- dist
    ]


{- | Constuct a distribution that contains no partitions. -}
empty :: ParticipationDefaults

empty = D []


{- | Find the peers that own the specified partition. -}
findPartition :: PartitionKey -> ParticipationDefaults -> Set Peer

findPartition k d =
  case [ps | (ks, ps) <- unD d, k `member` ks] of
    [ps] -> ps
    _ -> error
      $ "No exact mach for key in distribution. This means there is a bug in "
      ++ "the module `Network.Legion.Distribution`. Please report this bug "
      ++ "via github: " ++ show (k, d)


{- | Find a solution to the minimum complete service set. -}
minimumCompleteServiceSet :: ParticipationDefaults -> Set Peer
minimumCompleteServiceSet defs = Set.fromList [
    p
    | (_, peers) <- unD defs
    , Just (p, _) <- [Set.minView peers]
  ]


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
  Return the best action, if any, that should be taken to rebalance an
  unbalanced distribution, along with the resulting distribution.
-}
rebalanceAction
  :: Set Peer {- ^ The set of all peers in the cluster. -}
  -> ParticipationDefaults
  -> (RebalanceAction, ParticipationDefaults)
rebalanceAction allPeers distribution =
    let
      action = underServed <> overServed <> underUtilized
      newDist = case action of
        NoAction -> D dist
        Invite peer ks -> modify (Set.insert peer) ks (D dist)
        Drop peer ks -> modify (Set.delete peer) ks (D dist)
    in (action, newDist)
  where

    {- | Remove any defunct peers from the distribution. -}
    dist = 
      let
        distPeers = Set.unions (snd <$> unD distribution)
        defunct = distPeers Set.\\ allPeers
      in
        unD (modify (Set.\\ defunct) KS.full distribution)


    {- |
      Figure out if there are any under-served partitions and also figure
      out if this peer is the best candidate to service them . "Under
      served" means that the partition isn't replicated enough times,
      where "enough" is the magic number 3.
    -}
    underServed :: RebalanceAction
    underServed =
      let
        underserved = [
            (ks, ps)
            | (ks, ps) <- dist
            , Set.size ps < 3
          ]
        mostUnderserved = sortBy (compare `on` Set.size . snd) underserved
      in case mostUnderserved of
        [] -> NoAction
        (ks, ps):_ ->
          let
            {- |
              Any peer that is not currently servicing the keyspace
              segment is a candidate.
            -}
            candidateHosts = Set.toAscList (allPeers Set.\\ ps)

            {- |
              The best candidate is the one that currently has the
              least load.
            -}
            bestHosts = sort [(load p, p) | p <- candidateHosts]
          in case bestHosts of
            (currentLoad, candidate):_ ->
              {-
                Don't be too eager to take on too much additional
                load, because if we take more than our fair share, then
                the extra is just going to get rebalanced away almost
                immediately, leading to inefficiency.
              -}
              let
                additionalLoad :: KeySet
                additionalLoad = KS.take (idealLoad - currentLoad) ks
              in Invite candidate additionalLoad
            _ -> NoAction

    {- |
      Figure out if there are any partitions being over served and also
      figure out if we are the best candidate to drop them. "Over served"
      means that the partition it replicated too many times. "Too many times"
      is anything over the magic number, 3.
    -}
    overServed :: RebalanceAction
    overServed =
      let
        {- | 'over' maps peers to the set of keys that peer should drop. -}
        over :: Map Peer KeySet
        over = Map.filter (not . KS.null) . Map.fromList $ [
            (candidate, foldr KS.union KS.empty [
                ks
                | (ks, ps) <- dist
                , Set.size ps > 3
                , best:_ <- [sortBy (flip compare `on` load) (Set.toList ps)]
                , best == candidate
              ])
            | candidate <- Set.toList allPeers
          ]
      in case Map.toAscList over of
          [] -> NoAction
          (peer, ks):_ -> Drop peer ks

    {- |
      Figure out which peer is most underutilized with respect to the
      rest of the cluster and also what keys that peer should begin to
      serve to correct the underutilization.
    -}
    underUtilized :: RebalanceAction
    underUtilized =
      let
        under = sortBy (compare `on` load) [
            p
            | p <- Set.toList allPeers
            , load p + 1 < idealLoad
          ]
        over = sortBy (flip compare `on` load) [
            p
            | p <- Set.toList allPeers
            , load p > idealLoad
          ]
      in case (under, over) of
        (u:_, o:_) | u /= o ->
          {-
            Figure out which keys to take, which is a selection of
            the difference between them large enough to move the under
            utilized peer up to the ideal load.
          -}
          let
            difference = (servicedBy o \\ servicedBy u)
            keys = KS.take (idealLoad - load u) difference
          in Invite u keys 
        _ -> NoAction

    {- | Figure out how much load a peer is servicing.  -}
    load :: Peer -> Integer
    load = KS.size . servicedBy

    {- | The ideal load for each peer.  -}
    idealLoad :: Integer
    idealLoad =
      let
        total = KS.size KS.full * 3
        numPeers = toInteger (Set.size allPeers)
      in (total `div` numPeers) + 1

    {- | Figure out the keyspace serviced by a peer.  -}
    servicedBy :: Peer -> KeySet
    servicedBy p = foldr KS.union KS.empty [
        ks
        | (ks, ps) <- dist
        , p `Set.member` ps
      ]


{- | The actions that are taken in order to build a balanced cluster. -}
data RebalanceAction
  = Invite Peer KeySet
  | Drop Peer KeySet
  | NoAction
  deriving (Show, Generic)
instance Binary RebalanceAction
instance Monoid RebalanceAction where
  mempty = NoAction
  mappend NoAction a = a
  mappend a _ = a


{- |
  Create a new peer.
-}
newPeer :: (MonadIO m) => m Peer
newPeer = Peer <$> getUUID


