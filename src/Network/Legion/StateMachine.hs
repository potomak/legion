{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  This module contains the state machine implementation of a legion node.

  Discussion:

  This is a first attempt to discover a pure legion state machine and isolated
  it from the runtime IO considerations. It is obviously not perfect, because
  everything still lives in 'LIO', which is 'IO'-backed; but mostly this is
  because access to the persistence layer still happens here. Once we pull that
  out into the 'Network.Legion.Runtime' module we should be clear to remove IO
  and make this thing look more like a pure state machine. - Rick
-}
module Network.Legion.StateMachine (
  stateMachine,
  LInput(..),
  LOutput(..),
  JoinRequest(..),
  JoinResponse(..),
  AdminMessage(..),
  NodeState,
  Forwarded(..),
  PeerMessage(..),
  PeerMessagePayload(..),
  MessageId,
  next,
  newNodeState,
) where

import Prelude hiding (lookup)

import Control.Exception (throw)
import Control.Monad (unless)
import Control.Monad.Catch (try, SomeException, MonadCatch, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger (logDebug, logWarn, logError, logInfo,
  MonadLogger)
import Control.Monad.Trans.Class (MonadTrans, lift)
import Control.Monad.Trans.State (StateT, runStateT, get, put)
import Data.Aeson (ToJSON, toJSON, object, (.=), encode)
import Data.Binary (Binary)
import Data.ByteString.Lazy (toStrict)
import Data.Conduit (Source, Conduit, ($$), await, awaitForever,
  transPipe, ConduitM, yield, ($=))
import Data.Default.Class (Default)
import Data.Map (Map, insert, lookup)
import Data.Maybe (fromMaybe)
import Data.Set (member, minView, (\\))
import Data.Text (pack, unpack)
import Data.Text.Encoding (decodeUtf8)
import Data.Time.Clock (getCurrentTime)
import Data.UUID (UUID)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Legion.Application (Legionary, LegionConstraints,
  Persistence(getState, saveState, list), Legionary(Legionary,
  persistence, handleRequest), RequestMsg)
import Network.Legion.BSockAddr (BSockAddr)
import Network.Legion.ClusterState (claimParticipation, ClusterPropState,
  getPeers, getDistribution, ClusterPowerState)
import Network.Legion.Distribution (rebalanceAction, RebalanceAction(
  Invite), Peer, newPeer)
import Network.Legion.KeySet (union, KeySet)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState, PartitionPropState)
import Network.Legion.PowerState (ApplyDelta)
import Network.Legion.UUID (getUUID)
import qualified Data.Conduit.List as CL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Network.Legion.ClusterState as C
import qualified Network.Legion.KeySet as KS
import qualified Network.Legion.PartitionState as P


{- |
  This conduit houses the main legionary state machine. The conduit's
  input, internal state, and output are analogous to a "real" state
  machine's input, state, and output. If this seems like an odd use of
  conduit, that's ok.  Hopefully we can make this look more like a pure
  state machine once we remove 'IO' from this module.
-}
stateMachine :: (LegionConstraints i o s)
  => Legionary i o s
  -> NodeState i o s
  -> Conduit (LInput i o s) LIO (LOutput i o s)
stateMachine l n = awaitForever (\msg -> do
    newState <- runStateMT n $ do
      handleMessage l msg
      heartbeat
      migrate l
      propagate
      rebalance l
      logState
    stateMachine l newState
  )
  where
    logState = lift . logNodeState =<< getS


{- | Handle one incomming message.  -}
handleMessage :: (LegionConstraints i o s)
  => Legionary i o s
  -> LInput i o s
  -> StateM i o s ()

handleMessage l msg = do
  NodeState {cluster} <- getS
  let
    {- | Return `True` if the peer is a known peer, false otherwise.  -}
    known peer = peer `member` C.allParticipants cluster
  $(logDebug) . pack $ "Receiving: " ++ show msg
  case msg of
    P peerMsg@PeerMessage {source} ->
      if known source
        then handlePeerMessage l peerMsg
        else
          $(logWarn) . pack
            $ "Dropping message from unknown peer: " ++ show source
    R ((key, request), respond) ->
      {- TODO distribute requests evenly accross the participating peers. -}
      case minView (C.findPartition key cluster) of
        Nothing ->
          $(logError) . pack
            $ "Keyspace does not contain key: " ++ show key ++ ". This "
            ++ "is a very bad thing and probably means there is a bug, "
            ++ "or else this node has not joined a cluster yet."
        Just (peer, _) ->
          forward peer key request respond
    J m -> handleJoinRequest m
    A m -> handleAdminMessage l m


{- | Handles one incomming message from a peer. -}
handlePeerMessage :: (LegionConstraints i o s)
  => Legionary i o s
  -> PeerMessage i o s
  -> StateM i o s ()

handlePeerMessage -- PartitionMerge
    Legionary {
        persistence
      }
    msg@PeerMessage {
        source,
        payload = PartitionMerge key ps
      }
  = do
    nodeState@NodeState {self, propStates, cluster} <- getS
    propState <- lift $ maybe
      (getStateL persistence self cluster key)
      return
      (lookup key propStates)
    case P.mergeEither source ps propState of
      Left err ->
        $(logWarn) . pack
          $ "Can't apply incomming partition action message "
          ++ show msg ++ "because of: " ++ show err
      Right newPropState -> do
        $(logDebug) "Saving because of PartitionMerge"
        lift $ saveStateL persistence key (
            if P.participating newPropState
              then Just (P.getPowerState newPropState)
              else Nothing
          )
        putS nodeState {
            propStates = if P.complete newPropState
              then Map.delete key propStates
              else insert key newPropState propStates
          }

handlePeerMessage -- ForwardRequest
    Legionary {handleRequest, persistence}
    msg@PeerMessage {
        payload = ForwardRequest key request,
        source,
        messageId
      }
  = do
    ns@NodeState {self, cluster, propStates} <- getS
    let owners = C.findPartition key cluster
    if self `member` owners
      then do
        let
          respond = send source . ForwardResponse messageId

        -- TODO 
        --   - figure out some slick concurrency here, by maintaining
        --       a map of keys that are currently being accessed or
        --       something
        -- 
        either (respond . rethrow) respond =<< try (do 
            prop <- lift $ getStateL persistence self cluster key
            let response = handleRequest key request (P.ask prop)
                newProp = P.delta request prop
            $(logDebug) "Saving because of ForwardRequest"
            lift $ saveStateL persistence key (Just (P.getPowerState newProp))
            $(logInfo) . pack
              $ "Handling user request: " ++ show request
            $(logDebug) . pack
              $ "Request details request: " ++ show prop ++ " ++ "
              ++ show request ++ " --> " ++ show (response, newProp)
            putS ns {propStates = insert key newProp propStates}
            return response
          )
      else
        {-
          we don't own the key after all, someone was wrong to forward
          us this request.
        -}
        case minView owners of
          Nothing -> $(logError) . pack
            $ "Can't find any owners for the key: " ++ show key
          Just (peer, _) ->
            emit (Send peer msg)
  where
    {- |
      rethrow is just a reification of `throw`.
    -}
    rethrow :: SomeException -> a
    rethrow = throw

handlePeerMessage -- ForwardResponse
    Legionary {}
    msg@PeerMessage {
        payload = ForwardResponse messageId response
      }
  = do
    nodeState@NodeState {forwarded} <- getS
    case lookup messageId (unF forwarded) of
      Nothing -> $(logWarn) . pack
        $  "This peer received a response for a forwarded request that it "
        ++ "didn't send. The only time you might expect to see this is if "
        ++ "this peer recently crashed and was immediately restarted. If "
        ++ "you are seeing this in other circumstances then probably "
        ++ "something is very wrong at the network level. The message was: "
        ++ show msg
      Just respond ->
        lift $ respond response
    putS nodeState {
        forwarded = F . Map.delete messageId . unF $ forwarded
      }

handlePeerMessage -- ClusterMerge
    Legionary {}
    msg@PeerMessage {
        source,
        payload = ClusterMerge ps
      }
  = do
    nodeState@NodeState {migration, cluster} <- getS
    case C.mergeEither source ps cluster of
      Left err ->
        $(logWarn) . pack
          $ "Can't apply incomming cluster action message "
          ++ show msg ++ "because of: " ++ show err
      Right (newCluster, newMigration) -> do
        emit . NewPeers . getPeers $ newCluster
        putS nodeState {
            migration = migration `union` newMigration,
            cluster = newCluster
          }


{- | Handle a join request message -}
handleJoinRequest
  :: (JoinRequest, JoinResponse -> LIO ())
  -> StateM i o s ()

handleJoinRequest (JoinRequest peerAddr, respond) = do
  ns@NodeState {cluster} <- getS
  peer <- lift newPeer
  let newCluster = C.joinCluster peer peerAddr cluster
  emit .  NewPeers . getPeers $ newCluster
  lift $ respond (JoinOk peer (C.getPowerState newCluster))
  putS ns {cluster = newCluster}


{- |
  Handle a message from the admin service.
-}
handleAdminMessage
  :: Legionary i o s
  -> AdminMessage i o s
  -> StateM i o s ()
handleAdminMessage _ (GetState respond) =
  lift . respond =<< getS
handleAdminMessage Legionary {persistence} (GetPart key respond) = lift $ do
  partitionVal <- lift (getState persistence key)
  respond partitionVal
handleAdminMessage _ (Eject peer respond) = do
    {-
      TODO: we should attempt to notify the ejected peer that it has
      been ejected instead of just cutting it off and washing our hands
      of it. I have a vague notion that maybe ejected peers should be
      permanently recorded in the cluster state so that if they ever
      reconnect then we can notify them that they are no longer welcome
      to participate.

      On a related note, we need to think very hard about the split brain
      problem. A random thought about that is that we should consider the
      extreme case where the network just fails completely and every node
      believes that every other node should be or has been ejected. This
      would obviously be catastrophic in terms of data durability unless
      we have some way to reintegrate an ejected node. So, either we
      have to guarantee that such a situation can never happen, or else
      implement a reintegration strategy.  It might be acceptable for
      the reintegration strategy to be very costly if it is characterized
      as an extreme recovery scenario.

      Question: would a reintegration strategy become less costly if the
      "next state id" for a peer were global across all power states
      instead of local to each power state?
    -}
    modifyS eject
    lift $ respond ()
  where
    eject ns@NodeState {cluster} = ns {cluster = C.eject peer cluster}


{- | Update all of the propagation states with the current time.  -}
heartbeat :: StateM i o s ()
heartbeat = do
  now <- liftIO getCurrentTime
  ns@NodeState {cluster, propStates} <- getS
  putS ns {
      cluster = C.heartbeat now cluster,
      propStates = Map.fromAscList [
          (k, P.heartbeat now p)
          | (k, p) <- Map.toAscList propStates
        ]
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
migrate :: (LegionConstraints i o s) => Legionary i o s -> StateM i o s ()
migrate Legionary{persistence} = do
    ns@NodeState {migration} <- getS
    unless (KS.null migration) $
      putS =<< lift (
          listL persistence
          $= CL.filter ((`KS.member` migration) . fst)
          $$ accum ns {migration = KS.empty}
        )
  where
    accum ns@NodeState {self, cluster, propStates} = await >>= \case
      Nothing -> return ns
      Just (key, ps) -> 
        let
          origProp = fromMaybe (P.initProp self ps) (lookup key propStates)
          newPeers_ = C.findPartition key cluster \\ P.projParticipants origProp
          {- This 'P.participate' is where the magic happens. -}
          newProp = foldr P.participate origProp (Set.toList newPeers_)
        in do
          $(logDebug) . pack $ "Migrating: " ++ show key
          lift (saveStateL persistence key (Just (P.getPowerState newProp)))
          accum ns {
              propStates = Map.insert key newProp propStates
            }


{- |
  Handle all cluster and partition state propagation actions, and return
  an updated node state.
-}
propagate :: (LegionConstraints i o s) => StateM i o s ()
propagate = do
    ns@NodeState {cluster, propStates} <- getS
    let (peers, ps, cluster2) = C.actions cluster
    $(logDebug) . pack $ "Cluster Actions: " ++ show (peers, ps)
    mapM_ (doClusterAction ps) (Set.toList peers)
    propStates2 <- mapM doPartitionActions (Map.toList propStates)
    putS ns {
        cluster = cluster2,
        propStates = Map.fromAscList [
            (k, p)
            | (k, p) <- propStates2
            , not (P.complete p)
          ]
      }
  where
    doClusterAction ps peer =
      send peer (ClusterMerge ps)

    doPartitionActions (key, propState) = do
        let (peers, ps, propState2) = P.actions propState
        mapM_ (perform ps) (Set.toList peers)
        return (key, propState2)
      where
        perform ps peer =
          send peer (PartitionMerge key ps)


{- |
  Figure out if any rebalancing actions must be taken by this node, and kick
  them off if so.
-}
rebalance :: (LegionConstraints i o s) => Legionary i o s -> StateM i o s ()
rebalance _ = do
  ns@NodeState {self, cluster} <- getS
  let
    allPeers = (Set.fromList . Map.keys . getPeers) cluster
    dist = getDistribution cluster
    action = rebalanceAction self allPeers dist
  $(logDebug) . pack $ "The rebalance action is: " ++ show action
  putS ns {
      cluster = case action of
        Nothing -> cluster
        Just (Invite ks) -> claimParticipation self ks cluster
    }


{- | This is the type of input accepted by the legionary state machine. -}
data LInput i o s
  = P (PeerMessage i o s)
  | R (RequestMsg i o)
  | J (JoinRequest, JoinResponse -> LIO ())
  | A (AdminMessage i o s)

instance (Show i, Show o, Show s) => Show (LInput i o s) where
  show (P m) = "(P " ++ show m ++ ")"
  show (R ((p, i), _)) = "(R ((" ++ show p ++ ", " ++ show i ++ "), _))"
  show (J (jr, _)) = "(J (" ++ show jr ++ ", _))"
  show (A a) = "(A (" ++ show a ++ "))"


{- | This is the type of output produced by the legionary state machine. -}
data LOutput i o s
  = Send Peer (PeerMessage i o s)
  | NewPeers (Map Peer BSockAddr)


{- | A helper function to log the state of the node: -}
logNodeState :: (LegionConstraints i o s) => NodeState i o s -> LIO ()
logNodeState ns = $(logDebug) . pack
    $ "The current node state is: " ++ show ns


{- | Like `getState`, but in LIO, and provides the correct bottom value.  -}
getStateL :: (ApplyDelta i s, Default s)
  => Persistence i s
  -> Peer
  -> ClusterPropState
  -> PartitionKey
  -> LIO (PartitionPropState i s)

getStateL p self cluster key =
  {- dp == default participants -}
  let dp = C.findPartition key cluster
  in maybe
      (P.new key self dp)
      (P.initProp self)
      <$> lift (getState p key)


{- | Like `saveState`, but in LIO.  -}
saveStateL
  :: Persistence i s
  -> PartitionKey
  -> Maybe (PartitionPowerState i s)
  -> LIO ()
saveStateL p k = lift . saveState p k


{- | Like `list`, but in LIO.  -}
listL :: Persistence i s -> Source LIO (PartitionKey, PartitionPowerState i s)
listL p = transPipe lift (list p)


{- | This is the type of a join request message.  -}
data JoinRequest = JoinRequest BSockAddr
  deriving (Generic, Show)
instance Binary JoinRequest


{- | The response to a JoinRequst message -}
data JoinResponse
  = JoinOk Peer ClusterPowerState
  | JoinRejected String
  deriving (Generic)
instance Binary JoinResponse


{- |
  The type of messages sent by the admin service.
-}
data AdminMessage i o s
  = GetState (NodeState i o s -> LIO ())
  | GetPart PartitionKey (Maybe (PartitionPowerState i s) -> LIO ())
  | Eject Peer (() -> LIO ())

instance Show (AdminMessage i o s) where
  show (GetState _) = "(GetState _)"
  show (GetPart k _) = "(GetPart " ++ show k ++ " _)"
  show (Eject p _) = "(Eject " ++ show p ++ " _)"


{- | Defines the local state of a node in the cluster.  -}
data NodeState i o s = NodeState {
             self :: Peer,
          cluster :: ClusterPropState,
        forwarded :: Forwarded o,
       propStates :: Map PartitionKey (PartitionPropState i s),
        migration :: KeySet,
           nextId :: MessageId
  }
instance (Show i, Show s) => Show (NodeState i o s) where
  show = unpack . decodeUtf8 . toStrict . encode
{-
  The ToJSON instance is mainly for debugging. The Haskell-generated 'Show'
  instance is very hard to read.
-}
instance (Show i, Show s) => ToJSON (NodeState i o s) where
  toJSON (NodeState self cluster forwarded propStates migration nextId) =
    object [
        "self" .= show self,
        "cluster" .= cluster,
        "forwarded" .= [show k | k <- Map.keys (unF forwarded)],
        "propStates" .= Map.mapKeys show propStates,
        "migration" .= show migration,
        "nextId" .= show nextId
      ]


{- | A set of forwarded messages.  -}
newtype Forwarded o = F {unF :: Map MessageId (o -> LIO ())}
instance Show (Forwarded o) where
  show = show . Map.keys . unF


{- |
  The type of messages sent to us from other peers.
-}
data PeerMessage i o s = PeerMessage {
       source :: Peer,
    messageId :: MessageId,
      payload :: PeerMessagePayload i o s
  }
  deriving (Generic, Show)
instance (Binary i, Binary o, Binary s) => Binary (PeerMessage i o s)


{- |
  The data contained within a peer message.

  When we get around to implementing durability and data replication,
  the sustained inability to confirm that a node has received one of
  these messages should result in the ejection of that node from the
  cluster and the blacklisting of that node so that it can never re-join.
-}
data PeerMessagePayload i o s
  = PartitionMerge PartitionKey (PartitionPowerState i s)
  | ForwardRequest PartitionKey i
  | ForwardResponse MessageId o
  | ClusterMerge ClusterPowerState
  deriving (Generic, Show)
instance (Binary i, Binary o, Binary s) => Binary (PeerMessagePayload i o s)


data MessageId = M UUID Word64 deriving (Generic, Show, Eq, Ord)
instance Binary MessageId


{- |
  Generate the next message id in the sequence. We would normally use
  `succ` for this kind of thing, but making `MessageId` an instance of
  `Enum` really isn't appropriate.
-}
next :: MessageId -> MessageId
next (M sequenceId ord) = M sequenceId (ord + 1)


{- |
  Initialize a new sequence of messageIds
-}
newSequence ::  LIO MessageId
newSequence = lift $ do
  sid <- getUUID
  return (M sid 0)


{- |
  Make a new node state.
-}
newNodeState :: Peer -> ClusterPropState -> LIO (NodeState i o s)
newNodeState self cluster = do
  nextId <- newSequence
  return NodeState {
      self,
      nextId,
      cluster,
      forwarded = F Map.empty,
      propStates = Map.empty,
      migration = KS.empty
    }


send :: Peer -> PeerMessagePayload i o s -> StateM i o s ()
send peer payload = do
  ns@NodeState {self, nextId} <- getS
  emit (Send peer PeerMessage {
      source = self,
      messageId = nextId,
      payload
    })
  putS ns {nextId = next nextId}


{- |
  Forward a user request to a peer for handling, being sure to do all
  the node state accounting.
-}
forward
  :: Peer
  -> PartitionKey
  -> i
  -> (o -> IO ())
  -> StateM i o s ()
forward peer key request respond = do
  ns@NodeState {nextId, self, forwarded} <- getS
  emit (Send peer PeerMessage {
      source = self,
      messageId = nextId,
      payload = ForwardRequest key request
    })
  putS ns {
      nextId = next nextId,
      forwarded = F . insert nextId (lift . respond) . unF $ forwarded
    }


{- |
  The monad in which the internals of the state machine run. This is really
  just a conduit, but we wrap it because we only want to allow `yield`, which
  we have re-named `emit`.
-}
newtype StateMT i o s m r = StateMT {
    unStateMT ::
      StateT
        (NodeState i o s)
        (ConduitM (LInput i o s) (LOutput i o s) m)
        r
  } deriving (
    Functor, Applicative, Monad, MonadLogger, MonadCatch,
    MonadThrow, MonadIO
  )
{-
  We can lift things from the underlying monad straight to 'StateT',
  bypassing the `CondutM` layer.
-}
instance MonadTrans (StateMT i o s) where
  lift = StateMT . lift . lift


{- |
  The state machine monad, in LIO.
-}
type StateM i o s r = StateMT i o s LIO r


{- |
  Run the state machine monad, starting with the initial node state.
-}
runStateMT
  :: NodeState i o s
  -> StateMT i o s m ()
  -> ConduitM (LInput i o s) (LOutput i o s) m (NodeState i o s)
runStateMT ns = fmap snd . (`runStateT` ns) . unStateMT


{- |
  Emit some output from the state machine.
-}
emit :: LOutput i o s -> StateM i o s ()
emit = StateMT . lift . yield


{- |
  Get the node State.
-}
getS :: StateMT i o s m (NodeState i o s)
getS = StateMT get


{- |
  Put the node state.
-}
putS :: NodeState i o s -> StateMT i o s m ()
putS = StateMT . put


{- |
  Modify the node state.
-}
modifyS :: (NodeState i o s -> NodeState i o s) -> StateMT i o s m ()
modifyS f = putS . f =<< getS


