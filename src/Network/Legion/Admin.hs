{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{- |
  This module contains the admin interface code.
-}
module Network.Legion.Admin (
  runAdmin,
) where

import Canteven.HTTP (requestLogging, logExceptionsAndContinue)
import Control.Concurrent (forkIO, newChan, newEmptyMVar, writeChan,
  putMVar, takeMVar, Chan)
import Control.Monad (void)
import Control.Monad.Logger (askLoggerIO, runLoggingT)
import Control.Monad.Trans.Class (lift)
import Data.Conduit (Source)
import Data.Default.Class (def)
import Data.Text.Encoding (encodeUtf8)
import Data.Text.Lazy (Text, pack)
import Data.Version (showVersion)
import Network.Legion.Application (LegionConstraints)
import Network.Legion.Conduit (chanToSource)
import Network.Legion.LIO (LIO)
import Network.Legion.PartitionKey (PartitionKey(K))
import Network.Legion.StateMachine (AdminMessage(GetState, GetPart))
import Network.Wai (Middleware, modifyResponse)
import Network.Wai.Handler.Warp (HostPreference, defaultSettings, Port,
  setHost, setPort)
import Network.Wai.Middleware.AddHeaders (addHeaders)
import Network.Wai.Middleware.StripHeaders (stripHeader)
import Paths_legion (version)
import Web.Scotty.Resource.Trans (resource, get)
import Web.Scotty.Trans (Options, scottyOptsT, settings, ScottyT, text,
  ActionT, param, middleware)
import qualified Data.Text as T

{- |
  Start the admin service in a background thread.
-}
runAdmin :: (LegionConstraints i o s)
  => Port
  -> HostPreference
  -> LIO (Source LIO (AdminMessage i o s))
runAdmin addr host = do
  logging <- askLoggerIO
  chan <- lift newChan
  void . lift . forkIO . (`runLoggingT` logging) $
    let
      website :: ScottyT Text LIO ()
      website = do
        middleware
          $ requestLogging logging
          . setServer
          . logExceptionsAndContinue logging

        resource "/clusterstate" $
          get $ do
            val <- send chan GetState
            text (pack (show val))
        resource "/propstate/:key" $
          get $ do
            key <- K . read <$> param "key"
            val <- send chan (GetPart key)
            text (pack (show val))
    in scottyOptsT (options addr host) (`runLoggingT` logging) website
  return (chanToSource chan)
  where
    send
      :: Chan (AdminMessage i o s)
      -> ((a -> LIO ()) -> AdminMessage i o s)
      -> ActionT Text LIO a
    send chan msg = lift . lift $ do
      mvar <- newEmptyMVar
      writeChan chan (msg (lift . putMVar mvar))
      takeMVar mvar


{- |
  Build some warp settings based on the configured socket address.
-}
options :: Port -> HostPreference -> Options
options port host = def {
    settings =
      setPort port
      . setHost host
      $ defaultSettings
  }


setServer :: Middleware
setServer = addServerHeader . stripServerHeader
  where
    {- |
      Strip the server header
    -}
    stripServerHeader :: Middleware
    stripServerHeader = modifyResponse (stripHeader "Server") 

    {- |
      Add our own server header.
    -}
    addServerHeader :: Middleware
    addServerHeader = addHeaders [("Server", serverValue)]

    {- |
      The value of the @Server:@ header.
    -}
    serverValue =
      encodeUtf8 (T.pack ("legion-admin/" ++ showVersion version))

