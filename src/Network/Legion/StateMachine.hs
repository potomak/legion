{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  This module contains the "pure-ish" state machine that defines what
  it means to be a legion node. As described on 'SM', the state machine
  is modeled in monadic fashion, where the state machine sate is modeled
  as monadic context, state machine input is modeled as various monadic
  functions, and state machine output is modeled as the result of those
  monadic functions.

  The reason the state lives behind a monad is because part of the
  node state (i.e. the persistence layer) really does live behind IO,
  and cannot be accessed purely. Therefore, the state is divided into a
  pure part, modeled by 'NodeState'; and an impure part, modeled by the
  persistence layer interface. We wrap these two components inside
  of a new, opaque, monad called 'SM' by using a monad transformation
  stack, where 'StateT' wraps the pure part of the state, and IO wraps
  the impure part of the state. (This is a simplified description. The
  actual monad transformation stack is more complicated, because it
  incorporates logging and access to the user-defined request handler.)

  The overall purpose of all of this is to separate as much as
  possible the abstract idea of what a legion node is with its runtime
  considerations. The state machine contained in this module defines how a
  legion node should behave when faced with various inputs, and it would
  be completely pure but for the persistence layer interface. The runtime
  system 'Network.Legion.Runtime' implements the mechanisms by which
  such input is collected and any behavior associated with the output
  (e.g. managing network connections, sending data across the wire,
  reading data from the wire, transforming those data into inputs to
  the state machine, etc.).
-}
module Network.Legion.StateMachine(
  -- * Running the state machine.
  NodeState,
  newNodeState,
  SM,
  runSM,

  -- * State machine inputs.
  userRequest,
  partitionMerge,
  clusterMerge,
  migrate,
  propagate,
  rebalance,
  heartbeat,
  eject,
  join,
  minimumCompleteServiceSet,
  search,

  -- * State machine outputs.
  ClusterAction(..),
  UserResponse(..),

  -- * State inspection
  getPeers,
) where

import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Logger (MonadLogger, logWarn, logDebug, logError)
import Control.Monad.Trans.Class (lift, MonadTrans)
import Control.Monad.Trans.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans.State (StateT, runStateT, get, put, modify)
import Data.Aeson (ToJSON, toJSON, object, (.=), encode)
import Data.ByteString.Lazy (toStrict)
import Data.Conduit (($=), ($$), Sink, transPipe, awaitForever)
import Data.Default.Class (Default)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Set (Set, (\\))
import Data.Text (pack, unpack)
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock (getCurrentTime)
import Network.Legion.Application (Legionary(Legionary), getState,
  saveState, list, persistence, index)
import Network.Legion.BSockAddr (BSockAddr)
import Network.Legion.ClusterState (ClusterPropState, ClusterPowerState)
import Network.Legion.Distribution (Peer, rebalanceAction, newPeer,
  RebalanceAction(Invite))
import Network.Legion.Index (IndexRecord(IndexRecord), stTag, stKey,
  irTag, irKey, SearchTag(SearchTag))
import Network.Legion.KeySet (KeySet, union)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState, PartitionPropState)
import Network.Legion.PowerState (ApplyDelta, apply)
import qualified Data.Conduit.List as CL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.ClusterState as C
import qualified Network.Legion.Distribution as D
import qualified Network.Legion.KeySet as KS
import qualified Network.Legion.PartitionState as P


{- |
  This is the portion of the local node state that is not persistence
  related.
-}
data NodeState i o s = NodeState {
             self :: Peer,
          cluster :: ClusterPropState,
       partitions :: Map PartitionKey (PartitionPropState i o s),
        migration :: KeySet,
          nsIndex :: Set IndexRecord
  }
instance (Show i, Show s) => Show (NodeState i o s) where
  show = unpack . decodeUtf8 . toStrict . encode
{-
  The ToJSON instance is mainly for debugging. The Haskell-generated 'Show'
  instance is very hard to read.
-}
instance (Show i, Show s) => ToJSON (NodeState i o s) where
  toJSON (NodeState self cluster partitions migration nsIndex) =
    object [
              "self" .= show self,
           "cluster" .= cluster,
        "partitions" .= Map.mapKeys show partitions,
         "migration" .= show migration,
           "nsIndex" .= show nsIndex
      ]


{- |
  Make a new node state.
-}
newNodeState :: Peer -> ClusterPropState -> NodeState i o s
newNodeState self cluster =
  NodeState {
      self,
      cluster,
      partitions = Map.empty,
      migration = KS.empty,
      nsIndex = Set.empty
    }


{- |
  This monad encapsulates the global state of the legion node (not
  counting the runtime stuff, like open connections and what have
  you).

  The main reason that the state is hidden behind a monad is because part
  of the sate (i.e. the partition data) lives behind 'IO'.  Therefore,
  if we want to model the global state of the node as a single unit,
  we have to do so using a monad.
-}
newtype SM i o s a = SM {
    unSM :: ReaderT (Legionary i o s) (StateT (NodeState i o s) LIO) a
  }
  deriving (Functor, Applicative, Monad, MonadLogger, MonadIO)


{- |
  Run an SM action.
-}
runSM
  :: Legionary i o s
  -> NodeState i o s
  -> SM i o s a
  -> LIO (a, NodeState i o s)
runSM l ns action = runStateT (runReaderT (unSM action) l) ns


{- | Handle a user request. -}
userRequest :: (ApplyDelta i o s, Default s)
  => PartitionKey
  -> i
  -> SM i o s (UserResponse o)
userRequest key request = SM $ do
  NodeState {self, cluster} <- lift get
  let owners = C.findPartition key cluster
  if self `Set.member` owners
    then do
      partition <- unSM $ getPartition key
      let
        response = fst (apply request (P.ask partition))
        partition2 = P.delta request partition
      unSM $ savePartition key partition2
      return (Respond response)

    else case Set.toList owners of
      [] -> do
        let msg = "No owners for key: " ++ show key
        $(logError) . pack $ msg
        error msg
      peer:_ -> return (Forward peer)


{- |
  Handle the state transition for a partition merge event. Returns 'Left'
  if there is an error, and 'Right' if everything went fine.
-}
partitionMerge :: (Show i, Show s, ApplyDelta i o s, Default s)
  => Peer
  -> PartitionKey
  -> PartitionPowerState i o s
  -> SM i o s ()
partitionMerge source key foreignPartition = do
  partition <- getPartition key
  case P.mergeEither source foreignPartition partition of
    Left err -> $(logWarn) . pack
      $ "Can't apply incomming partition merge from "
      ++ show source ++ ": " ++ show foreignPartition
      ++ ". because of: " ++ show err
    Right newPartition -> savePartition key newPartition


{- | Handle the state transition for a cluster merge event. -}
clusterMerge
  :: Peer
  -> ClusterPowerState
  -> SM i o s ()
clusterMerge source foreignCluster = SM . lift $ do
  nodeState@NodeState {migration, cluster} <- get
  case C.mergeEither source foreignCluster cluster of
    Left err -> $(logWarn) . pack
      $ "Can't apply incomming cluster merge from "
      ++ show source ++ ": " ++ show foreignCluster
      ++ ". because of: " ++ show err
    Right (newCluster, newMigration) ->
      put nodeState {
          migration = migration `union` newMigration,
          cluster = newCluster
        }


{- |
  Migrate partitions based on new cluster state information.

  TODO: this migration algorithm is super naive. It just goes ahead
  and migrates everything in one pass, which is going to be terrible
  for performance.

  Also, it is important to remember that "migrate" in this context does
  not mean "transfer data". Rather, "migrate" means to add a participating
  peer to a partition. This will cause the data to be transfered in the
  normal course of propagation.
-}
migrate :: (Default s, ApplyDelta i o s) => SM i o s ()
migrate = do
    NodeState {migration} <- (SM . lift) get
    Legionary {persistence} <- SM ask
    unless (KS.null migration) $
      transPipe (SM . lift3) (list persistence)
      $= CL.filter ((`KS.member` migration) . fst)
      $$ accum
    (SM . lift) $ modify (\ns -> ns {migration = KS.empty})
  where
    accum :: (Default s, ApplyDelta i o s)
      => Sink (PartitionKey, PartitionPowerState i o s) (SM i o s) ()
    accum = awaitForever $ \ (key, ps) -> do
      NodeState {self, cluster, partitions} <- (lift . SM . lift) get
      let
        partition = fromMaybe (P.initProp self ps) (Map.lookup key partitions)
        newPeers = C.findPartition key cluster \\ P.projParticipants partition
        newPartition = foldr P.participate partition (Set.toList newPeers)
      $(logDebug) . pack $ "Migrating: " ++ show key
      lift (savePartition key newPartition)


{- |
  Handle all cluster and partition state propagation actions, and return
  an updated node state.
-}
propagate :: SM i o s [ClusterAction i o s]
propagate = SM $ do
    partitionActions <- getPartitionActions
    clusterActions <- unSM getClusterActions
    return (clusterActions ++ partitionActions)
  where
    getPartitionActions = do
      ns@NodeState {partitions} <- lift get
      let
        updates = [
            (key, newPartition, [
                PartitionMerge peer key ps
                | peer <- Set.toList peers_
              ])
            | (key, partition) <- Map.toAscList partitions
            , let (peers_, ps, newPartition) = P.actions partition
          ]
        actions = [a | (_, _, as) <- updates, a <- as]
        newPartitions = Map.fromAscList [
            (key, newPartition)
            | (key, newPartition, _) <- updates
            , not (P.idle newPartition)
          ]
      (lift . put) ns {
          partitions = newPartitions
        }
      return actions

    getClusterActions :: SM i o s [ClusterAction i o s]
    getClusterActions = SM $ do
      ns@NodeState {cluster} <- lift get
      let
        (peers, cs, newCluster) = C.actions cluster
        actions = [ClusterMerge peer cs | peer <- Set.toList peers]
      (lift . put) ns {
          cluster = newCluster
        }
      return actions


{- |
  Figure out if any rebalancing actions must be taken by this node, and kick
  them off if so.
-}
rebalance :: SM i o s ()
rebalance = SM $ do
  ns@NodeState {self, cluster} <- lift get
  let
    allPeers = (Set.fromList . Map.keys . C.getPeers) cluster
    dist = C.getDistribution cluster
    action = rebalanceAction self allPeers dist
  $(logDebug) . pack $ "The rebalance action is: " ++ show action
  (lift . put) ns {
      cluster = case action of
        Nothing -> cluster
        Just (Invite ks) ->
          {-
            This 'claimParticipation' will be enforced by the remote
            peers, because those peers will see the change in distribution
            and then perform a 'migrate'.
          -}
          C.claimParticipation self ks cluster
    }


{- | Update all of the propagation states with the current time.  -}
heartbeat :: SM i o s ()
heartbeat = SM $ do
  now <- lift3 getCurrentTime
  ns@NodeState {cluster, partitions} <- lift get
  (lift . put) ns {
      cluster = C.heartbeat now cluster,
      partitions = Map.fromAscList [
          (k, P.heartbeat now p)
          | (k, p) <- Map.toAscList partitions
        ]
    }


{- | Eject a peer from the cluster.  -}
eject :: Peer -> SM i o s ()
eject peer = SM . lift $ do
  ns@NodeState {cluster} <- get
  put ns {cluster = C.eject peer cluster}


{- | Handle a peer join request.  -}
join :: BSockAddr -> SM i o s (Peer, ClusterPowerState)
join peerAddr = SM $ do
  peer <- lift2 newPeer
  ns@NodeState {cluster} <- lift get
  let newCluster = C.joinCluster peer peerAddr cluster
  (lift . put) ns {cluster = newCluster}
  return (peer, C.getPowerState newCluster)


{- |
  Figure out the set of nodes to which search requests should be
  dispatched. "Minimum complete service set" means the minimum set
  of peers that, together, service the whole partition key space;
  thereby guaranteeing that if any particular partition is indexed,
  the corresponding index record will exist on one of these peers.

  Implementation considerations:

  There will usually be more than one solution for the MCSS. For now,
  we just compute a deterministic solution, but we should implement
  a random (or pseudo-random) solution in order to maximally balance
  cluster resources.

  Also, it is not clear that the minimum complete service set is even
  what we really want. MCSS will reduce overall network utilization,
  but it may actually increase latency. If we were to dispatch redundant
  requests to multiple nodes, we could continue with whichever request
  returns first, and ignore the slow responses. This is probably the
  best solution. We will call this "fastest competitive search".

  TODO: implement fastest competitive search.
-}
minimumCompleteServiceSet :: SM i o s (Set Peer)
minimumCompleteServiceSet = SM $ do
  NodeState {cluster} <- lift get
  return (D.minimumCompleteServiceSet (C.getDistribution cluster))


{- |
  Search the index, and return the first record that is __strictly
  greater than__ the provided search tag, if such a record exists.
-}
search :: SearchTag -> SM i o s (Maybe IndexRecord)
search SearchTag {stTag, stKey = Nothing} = SM $ do
  NodeState {nsIndex} <- lift get
  return (Set.lookupGE IndexRecord {irTag = stTag, irKey = minBound} nsIndex)
search SearchTag {stTag, stKey = Just key} = SM $ do
  NodeState {nsIndex} <- lift get
  return (Set.lookupGT IndexRecord {irTag = stTag, irKey = key} nsIndex)


{- |
  These are the actions that a node can take which allow it to coordinate
  with other nodes. It is up to the runtime system to implement the
  actions.
-}
data ClusterAction i o s
  = ClusterMerge Peer ClusterPowerState
  | PartitionMerge Peer PartitionKey (PartitionPowerState i o s)


{- |
  The type of response to a user request, either forward to another node,
  or respond directly.
-}
data UserResponse o
  = Forward Peer
  | Respond o


{- | Get the known peer data from the cluster. -}
getPeers :: SM i o s (Map Peer BSockAddr)
getPeers = SM $ C.getPeers . cluster <$> lift get


{- | Gets a partition state. -}
getPartition :: (Default s, ApplyDelta i o s)
  => PartitionKey
  -> SM i o s (PartitionPropState i o s)
getPartition key = SM $ do
  Legionary {persistence} <- ask
  NodeState {self, partitions, cluster} <- lift get
  case Map.lookup key partitions of
    Nothing ->
      lift3 (getState persistence key) <&> \case
        Nothing -> P.new key self (C.findPartition key cluster)
        Just partition -> P.initProp self partition
    Just partition -> return partition


{- |
  Saves a partition state. This function automatically handles the cache
  for active propagations, as well as reindexing of partitions.
-}
savePartition :: (Default s, ApplyDelta i o s)
  => PartitionKey
  -> PartitionPropState i o s
  -> SM i o s ()
savePartition key partition = SM $ do
  Legionary {persistence, index} <- ask
  oldTags <- index . P.ask <$> unSM (getPartition key)
  let
    currentTags = index (P.ask partition)
    {- TODO: maybe use Set.mapMonotonic for performance?  -}
    obsoleteRecords = Set.map (flip IndexRecord key) (oldTags \\ currentTags)
    newRecords = Set.map (flip IndexRecord key) currentTags

  $(logDebug) . pack
    $ "Tagging " ++ show key ++ " with: "
    ++ show (currentTags, obsoleteRecords, newRecords)

  ns@NodeState {partitions, nsIndex} <- lift get
  lift3 (saveState persistence key (
      if P.participating partition
        then Just (P.getPowerState partition)
        else Nothing
    ))
  lift $ put ns {
      partitions = if P.idle partition
        then
          {-
            Remove the partition from the working cache because there
            is no remaining work that needs to be done to propagage
            its changes.
          -}
          Map.delete key partitions
        else
          Map.insert key partition partitions,
      nsIndex = (nsIndex \\ obsoleteRecords) `Set.union` newRecords
    }


{- | Borrowed from 'lens', like @flip fmap@. -}
(<&>) :: (Functor f) => f a -> (a -> b) -> f b
(<&>) = flip fmap


{- | Lift from two levels down in a monad transformation stack. -}
lift2
  :: (
      MonadTrans a,
      MonadTrans b,
      Monad m,
      Monad (b m)
    )
  => m r
  -> a (b m) r
lift2 = lift . lift


{- | Lift from three levels down in a monad transformation stack. -}
lift3
  :: (
      MonadTrans a,
      MonadTrans b,
      MonadTrans c,
      Monad m,
      Monad (c m),
      Monad (b (c m))
    )
  => m r
  -> a (b (c m)) r
lift3 = lift . lift . lift


