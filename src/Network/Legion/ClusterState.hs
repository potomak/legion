{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{- |
  This module contains the data types related to the distributed cluster state.
-}
module Network.Legion.ClusterState (
  ClusterState,
  ClusterPowerState,
  ClusterPowerStateT,
  RebalanceOrd,
  new,
  getPeers,
  findRoute,
  findOwners,
  getDistribution,
  joinCluster,
  finishRebalance,
  eject,
  nextAction,
) where

import Control.Exception (throw)
import Data.Aeson (ToJSON, toJSON, object, (.=), encode)
import Data.Binary (Binary)
import Data.Default.Class (Default(def))
import Data.Functor.Identity (runIdentity)
import Data.Map (Map)
import Data.Set (Set)
import Data.Text.Encoding (decodeUtf8)
import Data.UUID (UUID)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.Distribution (ParticipationDefaults,
  Peer, rebalanceAction, RebalanceAction(NoAction))
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PowerState (Event, apply, PowerState)
import Network.Legion.PowerState.Monad (PowerStateT, runPowerStateT)
import Network.Socket (SockAddr)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Network.Legion.Distribution as D
import qualified Network.Legion.PowerState as PS
import qualified Network.Legion.PowerState.Monad as PM


{- |
  An opaque data type, representing the cluster state that is shared
  between all peers.
-}
data ClusterState = ClusterState {
    distribution :: ParticipationDefaults,
           peers :: Map Peer BSockAddr,
         updates :: [ClusterChange],
    rebalanceOrd :: RebalanceOrd
  }
  deriving (Generic)
instance Binary ClusterState
instance Default ClusterState where
  def = ClusterState {
      distribution = D.empty,
             peers = Map.empty,
           updates = [],
      rebalanceOrd = minBound
    }
instance ToJSON ClusterState where
  toJSON (ClusterState distribution_ peers_ updates_ rebalanceOrd_) = object [
      "distribution" .= distribution_,
      "peers" .= Map.fromList [
          (show p, show a)
          | (p, a) <- Map.toList peers_
        ],
      "updates" .= (show <$> updates_),
      "rebalanceOrd" .= show rebalanceOrd_
    ]
instance Show ClusterState where
  show = T.unpack . decodeUtf8 . LBS.toStrict . encode


{- | A representation of all possible cluster states. -}
type ClusterPowerState =
  PowerState UUID ClusterState Peer Update ()


{- | A convenient alias for the cluster power state monad transformer. -}
type ClusterPowerStateT =
  PowerStateT UUID ClusterState Peer Update ()


{- | The type of rebalancing action ordinal. -}
newtype RebalanceOrd = RebalanceOrd Word64 
  deriving (Generic, Show, Enum, Bounded, Eq, Ord)
instance Binary RebalanceOrd


{- | The kinds of updates that can be applied to the cluster state. -}
data Update
  = Change ClusterChange
  | Complete
  deriving (Show, Generic, Eq)
instance Binary Update
instance Event Update () ClusterState where
  apply update cs@ClusterState {peers, updates, distribution, rebalanceOrd} =
    ((),) . popUpdate $ case update of
      Change change -> cs {updates = updates ++ [change]}
      Complete -> cs {
          distribution =
            snd (rebalanceAction (Map.keysSet peers) distribution),
          rebalanceOrd =
            succ rebalanceOrd
        }


{- |
  Helper for 'instance Event Update () ClusterState'. Applies updates
  from the update queue until an uncompleted rebalance action prevents
  further progress, and returns the resulting cluster state.
-}
popUpdate :: ClusterState -> ClusterState
popUpdate cs@ClusterState {updates, distribution, peers} =
  case (updates, rebalanceAction (Map.keysSet peers) distribution) of
    (u:moreUpdates, (NoAction, _)) -> popUpdate cs {
        peers = case u of
          PeerJoined peer addr -> Map.insert peer addr peers
          PeerEjected peer -> Map.delete peer peers,
        updates = moreUpdates
      }
    _ -> cs


{- | This type describes how a cluster topology can change. -}
data ClusterChange
  = PeerJoined Peer BSockAddr
  | PeerEjected Peer
  deriving (Show, Generic, Eq)
instance Binary ClusterChange


{- |
  Create the cluster state appropriate for a brand-new cluster.
-}
new :: UUID -> Peer -> SockAddr -> ClusterPowerState
new clusterId self addy =
  runIdentity $ runPowerStateT self (PS.new clusterId (Set.singleton self)) (do
      PM.event (Change (PeerJoined self (BSockAddr addy)))
      PM.event Complete
      PM.acknowledge
    ) >>= \case
      Left err -> throw err
      Right ((), _, cluster, _) -> return cluster


{- | Get the cluster peers. -}
getPeers :: ClusterPowerState -> Map Peer BSockAddr
getPeers = peers . PS.projectedValue


{- |
  get the cluster distribution.
-}
getDistribution :: ClusterPowerState -> ParticipationDefaults
getDistribution = distribution . PS.projectedValue


{- |
  Find the nodes to which a given request might be routed. This might be
  different from `findOwners` when a cluster rebalancing is taking place.
-}
findRoute :: PartitionKey -> ClusterPowerState -> Set Peer
findRoute key =
  D.findPartition key . distribution . PS.projectedValue


{- |
  Find the nodes which own a particular partition. This is used for
  primarily for initializing a new partition, and may be different than
  `findRoute` when a cluster rebalancing is happening.
-}
findOwners :: PartitionKey -> ClusterPowerState -> Set Peer
findOwners key cluster =
  let ClusterState {distribution, peers} = PS.projectedValue cluster
  in
    D.findPartition
      key
      (snd (rebalanceAction (Map.keysSet peers) distribution))


{- | Allow a new peer to join the cluster. -}
joinCluster :: (Monad m)
  => Peer
    {- ^ The peer that is joining. -}
  -> BSockAddr
    {- ^ The cluster address of the new peer. -}
  -> ClusterPowerStateT m ()
joinCluster peer addy = do
  PM.participate peer
  PM.event (Change (PeerJoined peer addy))


{- | Mark the current rebalance action as complete. -}
finishRebalance :: (Monad m) => ClusterPowerStateT m ()
finishRebalance = PM.event Complete


{- |
  Eject a peer from the cluster.
-}
eject :: (Monad m) => Peer -> ClusterPowerStateT m ()
eject peer = do
  PM.event (Change (PeerEjected peer))
  PM.disassociate peer


{- |
  Get the current rebalance action, along with its ordinal. This is
  taken from the infimum, so it may not reflect projected changes.
-}
nextAction :: ClusterPowerState -> (RebalanceOrd, RebalanceAction)
nextAction cluster =
  let ClusterState {peers, distribution, rebalanceOrd} = PS.infimumValue cluster
  in (rebalanceOrd, fst (rebalanceAction (Map.keysSet peers) distribution))

