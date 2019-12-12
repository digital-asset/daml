-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options (
    Options(..),
    parseOptions,
  ) where

import           Control.Monad.Logger
import           Data.Monoid ((<>))
import           Options.Applicative
import           Options.Applicative.Types (readerAsk, readerError)

import Types

parseOptions :: IO Options
parseOptions =
  execParser (info (optsParser <**> helper) fullDesc)

data Options = Options
  { optsArtifacts :: FilePath
  , optsPerformUpload :: PerformUpload
  , optsReleaseDir :: FilePath
  , optsSlackReleaseMessageFile :: Maybe FilePath
  , optsFullLogging :: Bool
  , optsLogLevel :: LogLevel
  , optsAllArtifacts :: AllArtifacts
  , optsLocallyInstallJars :: Bool
  } deriving (Eq, Show)

optsParser :: Parser Options
optsParser = Options
  <$> strOption (long "artifacts" <> help "Path to yaml file listing the artifacts to be released")
  <*> (PerformUpload <$> switch (long "upload" <> help "upload artifacts to bintray and Maven Central. If false, we don't upload artifacts to Maven Central or bintray even when the last commit is a release commit."))
  <*> option str (long "release-dir" <> help "specify full path to release directory")
  <*> option (Just <$> str) (long "slack-release-message" <> help "if present will write out what to write in slack. if there are no releases the file will be empty" <> value Nothing)
  <*> switch (long "full-logging" <> help "full logging detail")
  <*> option readLogLevel (long "log-level" <> metavar "debug|info|warn|error (default: info)" <> help "Specify log level during release run" <> value LevelInfo )
  <*> (AllArtifacts <$> switch (long "all-artifacts" <> help "Produce all artifacts including platform-independent artifacts on MacOS"))
  <*> switch (long "install-head-jars" <> help "install jars to ~/.m2")
  where
    readLogLevel :: ReadM LogLevel
    readLogLevel = do
      s <- readerAsk
      case s of
        "debug" -> return LevelDebug
        "info"  -> return LevelInfo
        "warn"  -> return LevelWarn
        "error" -> return LevelError
        _       -> readerError "log-level must be one of debug|info|warn|error"
