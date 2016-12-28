{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{- |
  This module contains the legion state machine monad and some
  primitives for manipulating the state. It is the foundation upon wish
  the 'Network.Legion.StateMachine' module is built. It is separate from
  that module because some of the primitives we export here go some small
  way to avoiding bugs that might arise if that module had direct access
  to the internals of this monad.
-}
module Network.Legion.StateMachine.Monad (
  -- * Run the monad
  runSM,

  -- * State Inspection
  getPersistence,
  getNodeState,

  -- * State Modification
  modifyNodeState,

  -- * Other symbols
  SM,
  NodeState(..),
  ClusterAction(..),
) where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Logger (MonadLogger)
import Control.Monad.Trans.Class (lift, MonadTrans)
import Control.Monad.Trans.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans.State (StateT, runStateT, get, modify)
import Data.Aeson (ToJSON, toJSON, object, (.=), encode)
import Data.ByteString.Lazy (toStrict)
import Data.Map (Map)
import Data.Set (Set)
import Data.Text (unpack)
import Data.Text.Encoding (decodeUtf8)
import Network.Legion.Application (Persistence)
import Network.Legion.ClusterState (ClusterPowerState, ClusterPropState)
import Network.Legion.Distribution (Peer)
import Network.Legion.Index (IndexRecord)
import Network.Legion.KeySet (KeySet)
import Network.Legion.Lift (lift2)
import Network.Legion.PartitionKey (PartitionKey)
import Network.Legion.PartitionState (PartitionPowerState, PartitionPropState)
import qualified Data.Map as Map


{- |
  Run an SM action.
-}
runSM
  :: Persistence e o s
  -> NodeState e o s
  -> SM e o s m a
  -> m (a, NodeState e o s)
runSM p ns =
  (`runStateT` ns)
  . (`runReaderT` p)
  . unSM


{- | Get the handle to the persistence layer. -}
getPersistence :: (Monad m) => SM e o s m (Persistence e o s)
getPersistence = SM ask


{- | Get the current node state. -}
getNodeState :: (Monad m) => SM e o s m (NodeState e o s)
getNodeState = (SM . lift) get


{- | Update current node state. -}
modifyNodeState :: (Monad m)
  => (NodeState e o s -> NodeState e o s)
  -> SM e o s m ()
modifyNodeState = SM . lift . modify


{- |
  This monad encapsulates the global state of the legion node (not
  counting the runtime stuff, like open connections and what have
  you).

  The main reason that the state is hidden behind a monad is because part
  of the sate (i.e. the partition data) lives behind 'IO'.  Therefore,
  if we want to model the global state of the node as a single unit,
  we have to do so using a monad.
-}
newtype SM e o s m a = SM {
    unSM ::
      ReaderT (Persistence e o s) (
      StateT (NodeState e o s)
      m) a
  }
  deriving (Functor, Applicative, Monad, MonadLogger, MonadIO)
instance MonadTrans (SM e o s) where
  lift = SM . lift2


{- |
  This is the portion of the local node state that is not persistence
  related.
-}
data NodeState e o s = NodeState {
             self :: Peer,
          cluster :: ClusterPropState,
       partitions :: Map PartitionKey (PartitionPropState e o s),
        migration :: KeySet,
          nsIndex :: Set IndexRecord
  }
instance (Show e, Show s) => Show (NodeState e o s) where
  show = unpack . decodeUtf8 . toStrict . encode
{-
  The ToJSON instance is mainly for debugging. The Haskell-generated 'Show'
  instance is very hard to read.
-}
instance (Show e, Show s) => ToJSON (NodeState e o s) where
  toJSON (NodeState self_ cluster_ partitions_ migration_ nsIndex_) =
    object [
              "self" .= show self_,
           "cluster" .= cluster_,
        "partitions" .= Map.mapKeys show partitions_,
         "migration" .= show migration_,
           "nsIndex" .= show nsIndex_
      ]


{- |
  These are the actions that a node can take which allow it to coordinate
  with other nodes. It is up to the runtime system to implement the
  actions.
-}
data ClusterAction e o s
  = ClusterMerge Peer ClusterPowerState
  | PartitionMerge Peer PartitionKey (PartitionPowerState e o s)


