{- |
  This module holds `forkC`, because we use it in at  least two other modules.
-}
module Network.Legion.Fork (
  forkC
) where

import Control.Concurrent (forkIO)
import Control.Exception (try, SomeException)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO, liftIO)
import System.Exit (ExitCode(ExitFailure))
import System.IO (hPutStrLn, stderr)
import System.Posix.Process (exitImmediately)
import qualified System.Log.Logger as L (errorM)

{- |
  Forks a critical thread. "Critical" in this case means that if the thread
  crashes for whatever reason, then the program cannot continue correctly, so
  we should crash the program instead of running in some kind of zombie broken
  state.
-}
forkC
  :: String
    -- ^ The name of the critical thread, used for logging.
  -> IO ()
    -- ^ The IO to execute.
  -> IO ()
forkC name io =
  void . forkIO $ do
    result <- try io
    case result of
      Left err -> do
        let msg =
              "Exception caught in critical thread " ++ show name
              ++ ". We are crashing the entire program because we can't "
              ++ "continue without this thread. The error was: "
              ++ show (err :: SomeException)
        -- write the message to every place we can think of.
        errorM msg
        putStrLn msg
        hPutStrLn stderr msg
        exitImmediately (ExitFailure 1)
      Right v -> return v


{- |
  Shorthand logging.
-}
errorM :: (MonadIO io) => String -> io ()
errorM = liftIO . L.errorM "legion"


