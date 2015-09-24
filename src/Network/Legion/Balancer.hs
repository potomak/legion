{- |
  This module contains the rebalancer daemon.
-}
module Network.Legion.Balancer(
) where

import Control.Concurrent.STM.TVar (TVar)
import Network.Legion.Distribution (KeySet, KeyDistribution)


{- |
  A handle on the balancer process.
-}
data Balancer = B {
    keyspace :: TVar
  }


{- |
  The type of control messages that the balancer can send to the main
  legion process.
-}
data BalancerMessage =
  StopService KeySet
    -- ^ Tells the legion process to stop servicing keys in the key
    --   set. This gives the balancer process a chance to migrate the
    --   values in that set to another node safely.

{- |
  The balancer process.
-}
balancer :: Chan BalancerMessage -> TVar KeyDistribution ->  IO ()
balancer control keyspaceT = do


