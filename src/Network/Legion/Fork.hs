{-# LANGUAGE TemplateHaskell #-}
{- |
  This module holds `forkC`, because we use it in at  least two other modules.
-}
module Network.Legion.Fork (
  forkC
) where

import Control.Concurrent (forkIO)
import Control.Exception (SomeException, try)
import Control.Monad (void)
import Control.Monad.Logger (logError, askLoggerIO, runLoggingT)
import Control.Monad.Trans.Class (lift)
import Data.Text (pack)
import Network.Legion.LIO (LIO)
import System.Exit (ExitCode(ExitFailure))
import System.IO (hPutStrLn, stderr)
import System.Posix.Process (exitImmediately)

{- |
  Forks a critical thread. "Critical" in this case means that if the thread
  crashes for whatever reason, then the program cannot continue correctly, so
  we should crash the program instead of running in some kind of zombie broken
  state.
-}
forkC
  :: String
    -- ^ The name of the critical thread, used for logging.
  -> LIO ()
    -- ^ The IO to execute.
  -> LIO ()
forkC name io = do
  logging <- askLoggerIO
  lift . void . forkIO $ do
    result <- try (runLoggingT io logging)
    case result of
      Left err -> do
        let msg =
              "Exception caught in critical thread " ++ show name
              ++ ". We are crashing the entire program because we can't "
              ++ "continue without this thread. The error was: "
              ++ show (err :: SomeException)
        -- write the message to every place we can think of.
        (`runLoggingT` logging) . $(logError) . pack $ msg
        putStrLn msg
        hPutStrLn stderr msg
        exitImmediately (ExitFailure 1)
      Right v -> return v

