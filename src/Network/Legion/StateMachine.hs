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
  newNodeState,

  -- * State machine inputs.
  userRequest,
  partitionMerge,
  clusterMerge,
  eject,
  join,
  minimumCompleteServiceSet,
  search,

  joinNext,
  joinNextResponse,

  -- * State machine outputs.
  UserResponse(..),

  -- * State inspection
  getPeers,
  getPartition,
) where

import Control.Monad (void, unless)
import Control.Monad.Catch (throwM, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger (MonadLogger, logDebug, logError,
  MonadLoggerIO, logWarn)
import Control.Monad.Trans.Class (lift)
import Data.Bool (bool)
import Data.Conduit ((=$=), runConduit, transPipe, awaitForever)
import Data.Default.Class (Default)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Set (Set, (\\), member)
import Data.Text (pack)
import Network.Legion.Application (getState, saveState, list)
import Network.Legion.BSockAddr (BSockAddr)
import Network.Legion.ClusterState (ClusterPowerState, ClusterPowerStateT)
import Network.Legion.Distribution (Peer, newPeer, RebalanceAction(Invite,
  Drop))
import Network.Legion.Index (IndexRecord(IndexRecord), stTag, stKey,
  irTag, irKey, SearchTag(SearchTag), indexEntries, Indexable)
import Network.Legion.KeySet (KeySet)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState, PartitionPowerStateT)
import Network.Legion.PowerState (Event)
import Network.Legion.PowerState.Monad (PropAction(Send, DoNothing))
import Network.Legion.StateMachine.Monad (SM, NodeState(NodeState),
  ClusterAction(PartitionMerge, ClusterMerge, PartitionJoin),
  self, cluster, partitions, nsIndex, getPersistence, getNodeState,
  modifyNodeState, pushActions, joins, lastRebalance)
import qualified Data.Conduit.List as CL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.ClusterState as C
import qualified Network.Legion.Distribution as D
import qualified Network.Legion.KeySet as KS
import qualified Network.Legion.PowerState as PS
import qualified Network.Legion.PowerState.Monad as PM


{- | Make a new node state. -}
newNodeState :: Peer -> ClusterPowerState -> NodeState e o s
newNodeState self cluster =
  NodeState {
      self,
      cluster,
      partitions = Map.empty,
      nsIndex = Set.empty,
      joins = Map.empty,
      lastRebalance = minBound
    }


{- | Handle a user request. -}
userRequest :: (
      Default s,
      Eq e,
      Event e o s,
      Indexable s,
      MonadLoggerIO m,
      MonadThrow m,
      Show e,
      Show s
    )
  => PartitionKey
  -> e
  -> SM e o s m (UserResponse o)
userRequest key request = do
  NodeState {self, cluster} <- getNodeState
  let routes = C.findRoute key cluster
  if self `Set.member` routes
    then do
      (response, _) <- runPartitionPowerStateT key (
          PM.event request
        )
      return (Respond response)

    else case Set.toList routes of
      [] -> do
        let msg = "No routes for key: " ++ show key
        $(logError) . pack $ msg
        error msg
      peer:_ -> return (Forward peer)


{- |
  Handle the state transition for a partition merge event. Returns 'Left'
  if there is an error, and 'Right' if everything went fine.
-}
partitionMerge :: (
      Default s,
      Eq e,
      Event e o s,
      Indexable s,
      MonadLoggerIO m,
      MonadThrow m,
      Show e,
      Show s
    )
  => PartitionKey
  -> PartitionPowerState e o s
  -> SM e o s m ()
partitionMerge key foreignPartition =
  void $ runPartitionPowerStateT key (PM.merge foreignPartition)


{- | Handle the state transition for a cluster merge event. -}
clusterMerge :: (
      Default s,
      Eq e,
      Event e o s,
      Indexable s,
      MonadLoggerIO m,
      MonadThrow m,
      Show e,
      Show s
    )
  => ClusterPowerState
  -> SM e o s m ()
clusterMerge foreignCluster = do
  runClusterPowerStateT (PM.merge foreignCluster)
  nodeState@NodeState {lastRebalance, cluster, self} <- getNodeState
  $(logDebug) . pack
    $ "Next Rebalance: "
    ++ show (lastRebalance, C.nextAction cluster, nodeState)
  case C.nextAction cluster of
    (ord, Invite peer keys) | ord > lastRebalance && peer == self -> do
      {-
        The current action is an Invite, and this peer is the target.

        Send the join request message to every peer, update lastRebalance
        so we don't repeat this on every trivial cluster merge, update
        the expected joins so we can keep track of progress, then sit
        back and wait.
      -}
      let
        askPeers =
          Set.toList . Set.delete self . Map.keysSet . C.getPeers $ cluster
      pushActions [
          PartitionJoin p keys
          | p <- askPeers
        ]
      modifyNodeState (\ns -> ns {
          joins = Map.fromList [
              (p, keys)
              | p <- askPeers
            ],
          lastRebalance = ord
        })
    (ord, Drop peer keys) | ord > lastRebalance && peer == self -> do
      persistence <- getPersistence
      runConduit (
          transPipe liftIO (list persistence)
          =$= CL.map fst
          =$= CL.filter (`KS.member` keys)
          =$= awaitForever (\key ->
              lift $ runPartitionPowerStateT key (
                  PM.disassociate self
                )
            )
        )
      modifyNodeState (\ns -> ns {
          lastRebalance = ord
        })
      runClusterPowerStateT C.finishRebalance
    _ -> return ()


{- | Eject a peer from the cluster.  -}
eject :: (MonadLogger m, MonadThrow m) => Peer -> SM e o s m ()
eject peer = runClusterPowerStateT (C.eject peer)


{- | Handle a peer join request.  -}
join :: (MonadIO m, MonadThrow m)
  => BSockAddr
  -> SM e o s m (Peer, ClusterPowerState)
join peerAddr = do
  peer <- newPeer
  void $ runClusterPowerStateT (C.joinCluster peer peerAddr)
  NodeState {cluster} <- getNodeState
  return (peer, cluster)


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
minimumCompleteServiceSet :: (Monad m) => SM e o s m (Set Peer)
minimumCompleteServiceSet = do
  NodeState {cluster} <- getNodeState
  return (D.minimumCompleteServiceSet (C.getDistribution cluster))


{- |
  Search the index, and return the first record that is __strictly
  greater than__ the provided search tag, if such a record exists.
-}
search :: (Monad m) => SearchTag -> SM e o s m (Maybe IndexRecord)
search SearchTag {stTag, stKey = Nothing} = do
  NodeState {nsIndex} <- getNodeState
  return (Set.lookupGE IndexRecord {irTag = stTag, irKey = minBound} nsIndex)
search SearchTag {stTag, stKey = Just key} = do
  NodeState {nsIndex} <- getNodeState
  return (Set.lookupGT IndexRecord {irTag = stTag, irKey = key} nsIndex)


{- |
  Allow a peer to participate in the replication of the partition that is
  __greater than or equal to__ the indicated partition key. Returns @Nothing@
  if there is no such partition, or @Just (key, partition)@ where @key@ is the
  partition key that was joined and @partition@ is the resulting partition
  power state.
-}
joinNext :: (
      Default s,
      Eq e,
      Event e o s,
      Indexable s,
      MonadLoggerIO m,
      MonadThrow m
    )
  => Peer
  -> KeySet
  -> SM e o s m (Maybe (PartitionKey, PartitionPowerState e o s))
joinNext peer askKeys = do
  persistence <- getPersistence
  runConduit (
      transPipe liftIO (list persistence)
      =$= CL.filter ((`KS.member` askKeys) . fst)
      =$= CL.head
    ) >>= \case
      Nothing -> return Nothing
      Just (gotKey, partition) -> do
        {-
          This is very similar to the 'runPartitionPowerStateT' code,
          but there are some important differences. First, 'list' has
          already done to the trouble of fetching the partition value,
          so we don't want to have 'runPartitionPowerStateT' do it
          again. Second, and more importantly, 'runPartitionPowerStateT'
          will cause a 'PartitionMerge' message to be sent to @peer@, but
          that message would be redundant, because it contains a subset
          of the information contained within the 'JoinNextResponse'
          message that this function produces.
        -}
        NodeState {self} <- getNodeState
        PM.runPowerStateT self partition (do
            PM.participate peer
            PM.acknowledge
          ) >>= \case
            Left err -> throwM err
            Right ((), action, partition2, _infOutputs) -> do
              case action of
                Send -> pushActions [
                    PartitionMerge p gotKey partition2
                    | p <- Set.toList (PS.allParticipants partition2)
                      {-
                        Don't send a 'PartitionMerge' to @peer@. We
                        are already going to send it a more informative
                        'JoinNextResponse'
                      -}
                    , p /= peer
                    , p /= self
                  ]
                DoNothing -> return ()
              savePartition gotKey partition2
              return (Just (gotKey, partition2))


{- | Receive the result of a JoinNext request. -}
joinNextResponse :: (
      Default s,
      Eq e,
      Event e o s,
      Indexable s,
      MonadLoggerIO m,
      MonadThrow m,
      Show e,
      Show s
    )
  => Peer
  -> Maybe (PartitionKey, PartitionPowerState e o s)
  -> SM e o s m ()
joinNextResponse peer response = do
  NodeState {cluster, lastRebalance} <- getNodeState
  if lastRebalance > fst (C.nextAction cluster)
    then
      {- We are receiving messages from an old rebalance. Log and ignore. -}
      $(logWarn) . pack
        $ "Received an old join response: "
        ++ show (peer, response, cluster, lastRebalance)
    else do
      case response of
        Just (key, partition) -> do
          partitionMerge key partition
          NodeState {joins} <- getNodeState
          case (KS.\\ KS.fromRange minBound key) <$> Map.lookup peer joins of
            Nothing ->
              {- An unexpected peer sent us this message, Ignore. TODO log. -}
              return ()
            Just needsJoinSet -> do
              unless (KS.null needsJoinSet)
                (pushActions [PartitionJoin peer needsJoinSet])
              modifyNodeState (\ns -> ns {
                  joins = Map.filter
                    (not . KS.null)
                    (Map.insert peer needsJoinSet joins)
                })
        Nothing ->
          modifyNodeState (\ns@NodeState {joins} -> ns {
              joins = Map.delete peer joins
            })
      Map.null . joins <$> getNodeState >>= bool
        (return ())
        (runClusterPowerStateT C.finishRebalance)


{- |
  The type of response to a user request, either forward to another node,
  or respond directly.
-}
data UserResponse o
  = Forward Peer
  | Respond o


{- | Get the known peer data from the cluster. -}
getPeers :: (Monad m) => SM e o s m (Map Peer BSockAddr)
getPeers = C.getPeers . cluster <$> getNodeState


{- | Gets a partition state. -}
getPartition :: (Default s, MonadIO m)
  => PartitionKey
  -> SM e o s m (PartitionPowerState e o s)
getPartition key = do
  persistence <- getPersistence
  NodeState {partitions, cluster} <- getNodeState
  case Map.lookup key partitions of
    Nothing ->
      fromMaybe (PS.new key (C.findOwners key cluster)) <$>
        liftIO (getState persistence key)
    Just partition -> return partition


{- |
  Saves a partition state. This function automatically handles the cache
  for active propagations, as well as reindexing of partitions.
-}
savePartition :: (Default s, Event e o s, Indexable s, MonadLoggerIO m)
  => PartitionKey
  -> PartitionPowerState e o s
  -> SM e o s m ()
savePartition key partition = do
  persistence <- getPersistence
  oldTags <- indexEntries . PS.projectedValue <$> getPartition key
  let
    currentTags = indexEntries (PS.projectedValue partition)
    {- TODO: maybe use Set.mapMonotonic for performance?  -}
    obsoleteRecords = Set.map (flip IndexRecord key) (oldTags \\ currentTags)
    newRecords = Set.map (flip IndexRecord key) currentTags

  $(logDebug) . pack
    $ "Tagging " ++ show key ++ " with: "
    ++ show (currentTags, obsoleteRecords, newRecords)

  NodeState {self} <- getNodeState
  liftIO (saveState persistence key (
      if self `member` PS.allParticipants partition
        then Just partition
        else Nothing
    ))
  modifyNodeState (\ns@NodeState {partitions, nsIndex} ->
      nsIndex `seq`
      ns {
          partitions = if Set.null (PS.divergent partition)
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
    )


-- {- |
--   Create the log message for origin conflict errors.  The reason this
--   function only creates the log message, instead of doing the logging
--   as well, is because doing the logging here would screw up the source
--   location that the template-haskell logging functions generate for us.
-- -}
-- originError :: (Show o) => DifferentOrigins o -> Text
-- originError (DifferentOrigins a b) = pack
--   $ "Tried to merge powerstates with different origins: "
--   ++ show (a, b)


{- | Run a partition-flavored 'PowerStateT' in the 'SM' monad. -}
runPartitionPowerStateT :: (
      Default s,
      Eq e,
      Event e o s,
      Indexable s,
      MonadLoggerIO m,
      MonadThrow m,
      Show e,
      Show s
    )
  => PartitionKey
  -> PartitionPowerStateT e o s (SM e o s m) a
  -> SM e o s m (a, PartitionPowerState e o s)
runPartitionPowerStateT key m = do
  NodeState {self} <- getNodeState
  partition <- getPartition key
  PM.runPowerStateT self partition (m <* PM.acknowledge) >>= \case
    Left err -> throwM err
    Right (a, action, partition2, _infOutputs) -> do
      case action of
        Send -> pushActions [
            PartitionMerge p key partition2
            | p <- Set.toList (PS.allParticipants partition2)
            , p /= self
          ]
        DoNothing -> return ()
      $(logDebug) . pack
        $ "Partition update: " ++ show partition
        ++ " --> " ++ show partition2 ++ " :: " ++ show action
      savePartition key partition2
      return (a, partition2)


{- |
  Run a clusterstate-flavored 'PowerStateT' in the 'SM' monad,
  automatically acknowledging the resulting power state.
-}
runClusterPowerStateT :: (MonadThrow m)
  => ClusterPowerStateT (SM e o s m) a
  -> SM e o s m a
runClusterPowerStateT m = do
  NodeState {cluster, self} <- getNodeState
  PM.runPowerStateT self cluster (m <* PM.acknowledge) >>= \case
    Left err -> throwM err
    Right (a, action, cluster2, _outputs) -> do
      case action of
        Send -> pushActions [
            ClusterMerge p cluster2
            | p <- Set.toList (PS.allParticipants cluster2)
            , p /= self
          ]
        DoNothing -> return ()
      modifyNodeState (\ns -> ns {cluster = cluster2})
      return a


