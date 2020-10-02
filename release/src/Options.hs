-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options (
    Options(..),
    parseOptions,
  ) where

import           Data.Monoid ((<>))
import           Options.Applicative

import Types

parseOptions :: IO Options
parseOptions =
  execParser (info (optsParser <**> helper) fullDesc)

data Options = Options
  { optsPerformUpload :: PerformUpload
  , optsReleaseDir :: FilePath
  , optsLocallyInstallJars :: Bool
  } deriving (Eq, Show)

optsParser :: Parser Options
optsParser = Options
  <$> (PerformUpload <$> switch (long "upload" <> help "upload java/scala artifacts to Maven Central and typescript artifacts to the npm registry."))
  <*> option str (long "release-dir" <> help "specify full path to release directory")
  <*> switch (long "install-head-jars" <> help "install jars to ~/.m2")
