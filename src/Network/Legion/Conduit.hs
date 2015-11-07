{- |
  This module contains some handy conduit abstractions.
-}
module Network.Legion.Conduit (
  chanToSource,
  chanToSink,
  merge
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Monad (void, forever)
import Control.Monad.Trans.Class (lift)
import Data.Conduit (Source, Sink, ($$), await, ($=), yield, await)
import qualified Data.Conduit.List as CL (map)

{- |
  Convert a chanel into a Source.
-}
chanToSource :: Chan a -> Source IO a
chanToSource chan = forever $ yield =<< lift (readChan chan)


{- |
 Convert an chanel into a Sink.
-}
chanToSink :: Chan a -> Sink a IO ()
chanToSink chan = do
  val <- await
  case val of
    Nothing -> return ()
    Just v -> do
      lift (writeChan chan v)
      chanToSink chan


{- |
  Merge two sources into one source. This is a concurrency abstraction.
  The resulting source will produce items from either of the input sources
  as they become available. As you would expect from a multi-producer,
  single-consumer concurrency abstraction, the ordering of items produced
  by each source is consistent relative to other items produced by
  that same source, but the interleaving of items from both sources
  is nondeterministic.
-}
merge :: Source IO a -> Source IO b -> Source IO (Either a b)
merge left right = do
  chan <- lift newChan
  (lift . void . forkIO) (left $= CL.map Left $$ chanToSink chan)
  (lift . void . forkIO) (right $= CL.map Right $$ chanToSink chan)
  chanToSource chan


