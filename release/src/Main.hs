-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Main (main) where

import Control.Monad.IO.Class
import Control.Monad.Logger
import Data.Foldable
import Path
import Path.IO

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified System.Directory as Dir

import Options
import Types
import Util

main :: IO ()
main = do
  ciCommand <- parseCiCommand
  case ciCommand of
      CmdBintray opts@Options{..} -> runLog opts $ do
          rootDir <- parseAbsDir =<< liftIO (Dir.canonicalizePath ".")
          releaseDir <- parseAbsDir =<< liftIO (Dir.makeAbsolute optsReleaseDir)
          liftIO $ createDirIfMissing True releaseDir
          os <- whichOS
          $logInfo "Checking if we should release"
          sdkVersion <- readVersionAt "HEAD"
          release <- isReleaseCommit "HEAD"
          let upload = if release then optsPerformUpload else PerformUpload False
          -- NOTE(MH): We add 100 to get version numbers for the individual
          -- components which are bigger than all version numbers we used
          -- before moving to the new daml repo.
          let compVersion = sdkVersion{versionMajor = 100 + versionMajor sdkVersion}
          mavenDeps <- getMavenDependencies rootDir
          artifacts <- buildAllComponents mavenDeps releaseDir os (renderVersion sdkVersion) (renderVersion compVersion)
          if getPerformUpload upload
              then do
                  $logInfo "Make release"
                  for_ optsSlackReleaseMessageFile $ \fp -> do
                      $logInfo ("Writing slack release message to "# T.pack fp)
                      liftIO (BS.writeFile fp (T.encodeUtf8 (slackReleaseMessage os (renderVersion sdkVersion))))
                  releaseToBintray upload releaseDir artifacts
              else do
                  $logInfo "Make dry run of release"
                  liftIO $ for_ optsSlackReleaseMessageFile (`BS.writeFile` "")
  where
    runLog Options{..} m0 = do
        let m = filterLogger (\_ ll -> ll >= optsLogLevel) m0
        if optsFullLogging then runStdoutLoggingT m else runFastLoggingT m
