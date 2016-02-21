{- |
  This module defines the specialized logging monad in which legion
  opperates.
-}
module Network.Legion.LIO (
  LIO
) where

import Control.Monad.Logger (LoggingT)


{- |
  The logging monad in wich legion operates.
-}
type LIO = LoggingT IO


