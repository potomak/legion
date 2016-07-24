{- |
  This module contains some handy conduit abstractions.
-}
module Network.Legion.Conduit (
  chanToSource,
  chanToSink,
  merge,
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Monad (void, forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Conduit (Source, Sink, ($$), ($=), yield, awaitForever)
import qualified Data.Conduit.List as CL (map)

{- |
  Convert a channel into a Source.
-}
chanToSource :: (MonadIO io) => Chan a -> Source io a
chanToSource chan = forever $ yield =<< liftIO (readChan chan)


{- |
 Convert a channel into a Sink.
-}
chanToSink :: (MonadIO io) => Chan a -> Sink a io ()
chanToSink chan = awaitForever (liftIO . writeChan chan)


{- |
  Merge two sources into one source. This is a concurrency abstraction.
  The resulting source will produce items from either of the input sources
  as they become available. As you would expect from a multi-producer,
  single-consumer concurrency abstraction, the ordering of items produced
  by each source is consistent relative to other items produced by
  that same source, but the interleaving of items from both sources
  is nondeterministic.
-}
merge :: (MonadIO io) => Source IO a -> Source IO b -> Source io (Either a b)
merge left right = do
  chan <- liftIO newChan
  (liftIO . void . forkIO) (left $= CL.map Left $$ chanToSink chan)
  (liftIO . void . forkIO) (right $= CL.map Right $$ chanToSink chan)
  chanToSource chan


