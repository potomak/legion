{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{- |
  This module contains the data types related to the distributed cluster state.
-}
module Network.Legion.ClusterState (
  ClusterState,
  ClusterPowerState,
  ClusterPropState,
  claimParticipation,
  new,
  initProp,
  getPowerState,
  getPeers,
  findPartition,
  getDistribution,
  joinCluster,
  eject,
  mergeEither,
  actions,
  allParticipants,
  heartbeat,
) where

import Data.Aeson (ToJSON, toJSON, object, (.=))
import Data.Binary (Binary)
import Data.Default.Class (Default(def))
import Data.Map (Map)
import Data.Set (Set)
import Data.Time.Clock (UTCTime)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import Network.Legion.BSockAddr (BSockAddr(BSockAddr))
import Network.Legion.Distribution (ParticipationDefaults, modify, Peer)
import Network.Legion.KeySet (KeySet, full, unions)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PowerState (Event, apply, StateId, DifferentOrigins)
import Network.Legion.Propagation (PropState, PropPowerState)
import Network.Socket (SockAddr)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.Distribution as D
import qualified Network.Legion.Propagation as P


{- |
  An opaque data type, representing the cluster state that is shared
  between all peers.
-}
data ClusterState = ClusterState {
    distribution :: ParticipationDefaults,
           peers :: Map Peer BSockAddr
  }
  deriving (Show, Generic)
instance Binary ClusterState
instance Default ClusterState where
  def = ClusterState {
      distribution = D.empty,
             peers = Map.empty
    }
instance ToJSON ClusterState where
  toJSON ClusterState {distribution, peers} = object [
      "distribution" .= distribution,
      "peers" .= Map.fromList [
          (show p, show a)
          | (p, a) <- Map.toList peers
        ]
    ]


{- | A representation of all possible cluster states. -}
newtype ClusterPowerState = ClusterPowerState {
    unPowerState :: PropPowerState UUID ClusterState Peer Update ()
  } deriving (Show, Binary)


{- |
  A reification of `PropState`, representing the propagation state of the
  cluster state.
-}
newtype ClusterPropState = ClusterPropState {
    unPropState :: PropState UUID ClusterState Peer Update ()
  } deriving (Show, ToJSON)


{- | The kinds of updates that can be applied to the cluster state. -}
data Update
  = PeerJoined Peer BSockAddr
  | Participating Peer KeySet
  | PeerEjected Peer
  deriving (Show, Generic)
instance Binary Update
instance Event Update () ClusterState where
  apply (PeerJoined peer addr) cs@ClusterState {peers} =
    ((), cs {peers = Map.insert peer addr peers})
  apply (Participating peer ks) cs@ClusterState {distribution} =
    ((), cs {distribution = modify (Set.insert peer) ks distribution})
  apply (PeerEjected peer) cs@ClusterState {distribution, peers} =
    ((), cs {
        distribution = modify (Set.delete peer) full distribution,
        peers = Map.delete peer peers
      })


{- |
  Helper function, for easily claiming participation in a key set.
-}
claimParticipation
  :: Peer
  -> KeySet
  -> ClusterPropState
  -> ClusterPropState
claimParticipation peer ks =
  ClusterPropState
  . P.event (Participating peer ks)
  . unPropState


{- |
  Create the cluster state appropriate for a brand-new cluster.
-}
new :: UUID -> Peer -> SockAddr -> ClusterPropState
new clusterId self addy =
  claimParticipation self full
  . ClusterPropState
  . P.event (PeerJoined self (BSockAddr addy))
  $ P.new clusterId self (Set.singleton self)


{- |
  Initialize a `ClusterPropState` based on the initial underlying cluster power
  state.
-}
initProp :: Peer -> ClusterPowerState -> ClusterPropState
initProp self = ClusterPropState . P.initProp self . unPowerState


{- |
  Return an opaque representation of the underling power state, for transfer
  across the network, or whatever.
-}
getPowerState :: ClusterPropState -> ClusterPowerState
getPowerState = ClusterPowerState . P.getPowerState . unPropState


{- |
  Get the cluster peers.
-}
getPeers :: ClusterPropState -> Map Peer BSockAddr
getPeers = peers . P.ask . unPropState


{- |
  get the cluster distribution.
-}
getDistribution :: ClusterPropState -> ParticipationDefaults
getDistribution = distribution . P.ask . unPropState


{- |
  Find the nodes that own a given partition.
-}
findPartition :: PartitionKey -> ClusterPropState -> Set Peer
findPartition key =
  D.findPartition key . distribution . P.ask . unPropState


{- |
  Allow a new peer to join the cluster.
-}
joinCluster
  :: Peer
    {- ^ The peer that is joining. -}
  -> BSockAddr
    {- ^ The cluster address of the new peer. -}
  -> ClusterPropState
    {- ^ The current cluster propagation state. -}
  -> ClusterPropState
joinCluster peer addy =
  ClusterPropState
  . P.event (PeerJoined peer addy)
  . P.participate peer
  . unPropState


{- |
  Eject a peer from the cluster.
-}
eject :: Peer -> ClusterPropState -> ClusterPropState
eject peer =
  ClusterPropState
  . P.event (PeerEjected peer)
  . P.disassociate peer
  . unPropState


{- |
  Merge a foreign cluster state with our own cluster state. This function
  returns the new cluster propagation state, along with a set of partition keys
  for which the default participation has changed (aka, a rebalance happened),
  indicating that some action should be taken to migrate the indicated
  partitions.
-}
mergeEither
  :: Peer
  -> ClusterPowerState
  -> ClusterPropState
  -> Either
      (DifferentOrigins UUID)
      ((ClusterPropState, Map (StateId Peer) ()), KeySet)
mergeEither otherPeer (ClusterPowerState otherPS) (ClusterPropState prop) =
  let
    self = P.getSelf prop
    divergences = P.divergences self (P.initProp otherPeer otherPS)
    migrating = unions [
        ks
        | (_, Participating _ ks) <- Map.toList divergences
      ]
  in case P.mergeEither otherPeer otherPS prop of
    Left err -> Left err
    Right (merged, outputs) ->
      Right ((ClusterPropState merged, outputs), migrating)


{- |
  Get the peers which require action (i.e. Send), if any, and the
  powerstate version to send to those peers, and the new propagation
  state that is applicable after those actions have been taken.
-}
actions :: ClusterPropState -> (Set Peer, ClusterPowerState, ClusterPropState)
actions prop =
  let (peers, ps, newProp) = P.actions (unPropState prop)
  in (peers, ClusterPowerState ps, ClusterPropState newProp)


{- |
  Return all cluster participants.
-}
allParticipants :: ClusterPropState -> Set Peer
allParticipants = P.allParticipants . unPropState


{- |
  Move time forward for the propagation state.
-}
heartbeat :: UTCTime -> ClusterPropState -> ClusterPropState
heartbeat now = ClusterPropState . P.heartbeat now . unPropState


