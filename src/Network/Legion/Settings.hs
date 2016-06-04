{- |
  This module contains the user settings.
-}
module Network.Legion.Settings (
  LegionarySettings(..),
) where

import Network.Socket (SockAddr)
import Network.Wai.Handler.Warp (HostPreference, Port)

{- | Settings used when starting up the legion framework.  -}
data LegionarySettings = LegionarySettings {
    peerBindAddr :: SockAddr,
      {- ^
        The address on which the legion framework will listen for
        rebalancing and cluster management commands.
      -}
    joinBindAddr :: SockAddr,
      {- ^
        The address on which the legion framework will listen for cluster
        join requests.
      -}
    adminHost :: HostPreference,
      {- ^
        The host address on which the admin service should run.
      -}
    adminPort :: Port
      {- ^
        The host port on which the admin service should run.
      -}
  }


