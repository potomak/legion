{-# LANGUAGE MultiParamTypeClasses #-}
{- |
  This module contains a convenience class for specifying all the constraints
  necessary on the Legion data types.
-}
module Network.Legion.Constraints (
  LegionConstraints
) where

import Data.Binary (Binary)
import Network.Legion.Bottom (Bottom)
import Network.Legion.PowerState (ApplyDelta)

{- |
  This is a more convenient way to write the somewhat unwieldy set of
  constraints
   
  > (
  >   ApplyDelta i s, Bottom s, Binary i, Binary o, Binary s, Show i,
  >   Show o, Show s, Eq i
  > )
-}
class (
    ApplyDelta i s, Bottom s, Binary i, Binary o, Binary s, Show i,
    Show o, Show s, Eq i
  ) => LegionConstraints i o s where


