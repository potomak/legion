{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  This module contains template-haskell utilities for compileing the
  admin interface.
-}
module Network.Legion.Admin.TH (
  adminSite,
) where

import Control.Applicative ((<$>))
import Control.Monad (join)
import Control.Monad.IO.Class (MonadIO)
import Data.List ((\\))
import Data.Maybe (catMaybes)
import Data.String (IsString, fromString)
import Data.Text.Encoding (decodeUtf8)
import Language.Haskell.TH (TExp, Q, runIO)
import Network.Mime (defaultMimeLookup)
import System.Directory (getDirectoryContents)
import System.FilePath.Posix (combine)
import System.Posix.Files (isRegularFile, isDirectory, getFileStatus)
import Web.Scotty.Format.Trans (respondTo, format)
import Web.Scotty.Resource.Trans (resource, get)
import Web.Scotty.Trans (ScottyT, setHeader, text, ScottyError, literal,
  redirect)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL

{- | Reads the static files that make up the admin user interface. -}
readStaticFiles :: FilePath -> IO [(FilePath, String)]
readStaticFiles base =
  let
    {- | shorthand combine for infix use. -}
    c = combine

    findAll :: FilePath -> IO [FilePath]
    findAll dir = do
        contents <-
          (\\ [".", ".."]) <$> getDirectoryContents (base `c` dir)
        dirs <- catMaybes <$> mapM justDir contents
        files <- catMaybes <$> mapM justFile contents
        more <- concat <$> mapM (findAll . combine dir) dirs
        return $ (combine dir <$> files) ++ more
      where
        justFile :: FilePath -> IO (Maybe FilePath)
        justFile filename = do
          isfile <-
            isRegularFile <$>
              getFileStatus (base `c` dir `c` filename)
          return $ if isfile then Just filename else Nothing

        justDir :: FilePath -> IO (Maybe FilePath)
        justDir filename = do
          isdir <-
            isDirectory <$>
              getFileStatus (base `c` dir `c` filename)
          return $ if isdir then Just filename else Nothing
  in do
    allFiles <- findAll "."
    allContent <- mapM (readFile . combine base) allFiles
    return (zip (drop 1 <$> allFiles) allContent)


{- | Construct a 'ScottyT' that serves the admin interface. -}
adminSite :: (MonadIO m, ScottyError e) => Q (TExp (ScottyT e m ()))
adminSite = join . runIO $ do
    files <- readStaticFiles "admin"
    mapM_ (printResource . fst) files
    return $ [||
        let
          {- | Build the scotty resources associated with each static file. -}
          buildResources :: (MonadIO m, ScottyError e)
            => [(FilePath, String)]
            -> ScottyT e m ()
          buildResources resources = do
              mapM_ buildResource resources
              {- Special case for index.html alias. -}
              resource (literal "/") $ get (redirect "/index.html")
            where
              {- | Build the scotty resource for one static file. -}
              buildResource :: (MonadIO m, ScottyError e)
                => (FilePath, String)
                -> ScottyT e m ()
              buildResource (filename, content) =
                let
                  contentType :: (IsString a) => a
                  contentType =
                    fromString
                    . T.unpack
                    . decodeUtf8
                    . defaultMimeLookup
                    . T.pack
                    $ filename

                in
                  resource (literal filename) $
                    get $
                      respondTo $
                        format contentType $ do
                          setHeader "Content-Type" contentType
                          text (TL.pack content)

        in buildResources files
      ||]
  where
    printResource :: String -> IO ()
    printResource file =
      putStrLn ("Generating admin resource for: " ++ show file)


