-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Options (
    Options(..),
    parseOptions,
  ) where

import qualified Data.Text as T
import Options.Applicative

import Types

parseOptions :: IO Options
parseOptions =
  execParser (info (optsParser <**> helper) fullDesc)

data Options = Options
  { optsPerformUpload :: PerformUpload
  , optsReleaseDir :: FilePath
  , optsLocallyInstallJars :: Bool
  , optIncludeTypescript :: IncludeTypescript
  , optScalaVersions :: [T.Text]
  , optIncludeDocs :: IncludeDocs
  } deriving (Eq, Show)

optsParser :: Parser Options
optsParser = Options
  <$> (PerformUpload <$> switch (long "upload" <> help "upload java/scala artifacts to Maven Central and typescript artifacts to the npm registry."))
  <*> option str (long "release-dir" <> help "specify full path to release directory")
  <*> switch (long "install-head-jars" <> help "install jars to ~/.m2")
  <*> fmap (IncludeTypescript . not) (switch (long "no-ts" <> help "Do not build and upload typescript packages"))
  <*> (some (strOption (long "scala-version" <> help "Scala version to build JARs for")) <|> pure defaultScalaVersions)
  <*> fmap (IncludeDocs . not) (switch (long "no-docs" <> help "Do not build and install documentation"))

-- Keep in sync with /bazel_tools/scala_version.bzl and /nix/nixpkgs.nix
defaultScalaVersions :: [T.Text]
defaultScalaVersions = ["2.13.11"]
