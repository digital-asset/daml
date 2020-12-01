-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell, MultiWayIf #-}
module Main (main) where

import Control.Lens (view)
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Exception
import qualified Data.SemVer as SemVer
import Data.Yaml
import qualified Data.Set as Set
import qualified Data.List.Extra as List
import Path
import Path.IO hiding (removeFile)

import qualified Data.Text as T
import qualified Data.Text.IO as T.IO
import qualified Data.Maybe as Maybe
import qualified System.Directory as Dir
import System.Exit
import System.Process

import Maven
import Options
import Types
import Upload
import Util

import qualified SdkVersion

main :: IO ()
main = do
  opts@Options{..} <- parseOptions
  runLog opts $ do
      releaseDir <- parseAbsDir =<< liftIO (Dir.makeAbsolute optsReleaseDir)
      liftIO $ createDirIfMissing True releaseDir
      mvnArtifacts :: [Artifact (Maybe ArtifactLocation)] <- decodeFileThrow "release/artifacts.yaml"

      Right mvnVersion <- pure $ SemVer.fromText $ T.pack SdkVersion.mvnVersion
      let mvnTargets = concatMap buildTargets mvnArtifacts
      $logInfo "Building all targets"
      liftIO $ callProcess "bazel" ("build" : map (T.unpack . getBazelTarget) mvnTargets)

      bazelLocations <- liftIO getBazelLocations
      $logInfo "Reading metadata from pom files"
      mvnArtifacts <- liftIO $ mapM (resolvePomData bazelLocations mvnVersion) mvnArtifacts

      -- all known targets uploaded to maven, that are not deploy Jars
      -- we don't check dependencies for deploy jars as they are single-executable-jars
      let nonDeployJars = filter (not . isDeployJar . artReleaseType) mvnArtifacts
      let allMavenTargets = Set.fromList $ fmap (T.unpack . getBazelTarget . artTarget) mvnArtifacts

      -- check that all maven artifacts use com.daml as groupId
      let nonComDamlGroupId = filter (\a -> "com.daml" /= (groupIdString $ pomGroupId $ artMetadata a)) mvnArtifacts
      when (not (null nonComDamlGroupId)) $ do
          $logError "Some artifacts don't use com.daml as groupId!"
          forM_ nonComDamlGroupId $ \artifact -> do
              $logError ("\t- "# getBazelTarget (artTarget artifact))
          liftIO exitFailure

      -- check that no artifact id is used more than once
      let groupedArtifacts = List.groupOn (pomArtifactId . artMetadata) mvnArtifacts
      let duplicateArtifactIds = filter (\artifacts -> length artifacts > 1) groupedArtifacts
      when (not (null duplicateArtifactIds)) $ do
          $logError "Some artifacts use the same artifactId!"
          forM_ duplicateArtifactIds $ \artifacts -> do
              $logError (pomArtifactId $ artMetadata $ head artifacts)
              forM_ artifacts $ \artifact -> do
                  $logError ("\t- "# getBazelTarget (artTarget artifact))
          liftIO exitFailure

      -- find out all the missing internal dependencies
      missingDepsForAllArtifacts <- do
          let bazelQueryDeps target = do
              let query = "kind(\"(java|scala)_library\", deps(" <> target <> ")) intersect //..."
              liftIO $ lines <$> readCreateProcess (proc "bazel" ["query", query]) ""

          -- run a Bazel query to find all internal Java and Scala library dependencies
          let targets = map (getBazelTarget . artTarget) nonDeployJars
          internalDeps <- bazelQueryDeps ("set(" <> T.unpack (T.intercalate " " targets) <> ")")
          -- check if a dependency is not already a maven target from artifacts.yaml
          let missingDeps = filter (`Set.notMember` allMavenTargets) internalDeps
          if null missingDeps
              then
                  return []
              else do
                  -- if there's a missing artifact, find out what depends on it by querying each
                  -- artifact separately, one by one, so that the error message is more useful
                  -- (this is slow, so we don't do it unless we have to)
                  maybeMissingDeps <- forM nonDeployJars $ \a -> do
                      internalDeps <- bazelQueryDeps (T.unpack (getBazelTarget (artTarget a)))
                      let missingDeps = filter (`Set.notMember` allMavenTargets) internalDeps
                      if null missingDeps then return Nothing else return (Just (a, missingDeps))
                  return $ Maybe.catMaybes maybeMissingDeps

      -- now we can report all the missing dependencies per artifact
      when (not (null missingDepsForAllArtifacts)) $ do
          $logError "Some internal dependencies are not published to Maven Central!"
          forM_ missingDepsForAllArtifacts $ \(artifact, missingDeps) -> do
              $logError (getBazelTarget (artTarget artifact))
              forM_ missingDeps $ \dep -> $logError ("\t- "# T.pack dep)
          liftIO exitFailure

      mvnFiles <- fmap concat $ forM mvnArtifacts $ \artifact ->
          map (artifact,) <$> artifactFiles artifact
      forM_ mvnFiles $ \(_, (inp, outp)) ->
          copyToReleaseDir bazelLocations releaseDir inp outp

      mvnUploadArtifacts <- concatMapM mavenArtifactCoords mvnArtifacts
      validateMavenArtifacts releaseDir mvnUploadArtifacts

      -- NPM packages we want to publish
      let npmPackages =
              [ "//language-support/ts/daml-types"
              , "//language-support/ts/daml-ledger"
              , "//language-support/ts/daml-react"
              ]
      -- make sure the NPM packages can be built
      $logInfo "Building language-support TypeScript packages"
      liftIO $ callProcess "bazel" $ ["build"] ++ npmPackages

      if  | getPerformUpload optsPerformUpload -> do
              $logInfo "Uploading to Maven Central"
              mavenUploadConfig <- mavenConfigFromEnv
              if not (null mvnUploadArtifacts)
                  then
                    uploadToMavenCentral mavenUploadConfig releaseDir mvnUploadArtifacts
                  else
                    $logInfo "No artifacts to upload to Maven Central"

              $logDebug "Uploading npm packages"
              -- We can't put an .npmrc file in the root of the directory because other bazel npm
              -- code picks it up and looks for the token which is not yet set before the release
              -- phase.
              let npmrcPath = ".npmrc"
              liftIO $ bracket_
                (writeFile npmrcPath "//registry.npmjs.org/:_authToken=${NPM_TOKEN}")
                (Dir.removeFile npmrcPath)
                (forM_ npmPackages
                  $ \rule -> liftIO $ callCommand $ unwords $
                      ["bazel", "run", rule <> ":npm_package.publish", "--", "--access", "public"] <>
                      [ x | isSnapshot mvnVersion, x <- ["--tag", "next"] ])

          | optsLocallyInstallJars -> do
              pom <- generateAggregatePom bazelLocations mvnArtifacts
              pomPath <- (releaseDir </>) <$> parseRelFile "pom.xml"
              liftIO $ T.IO.writeFile (toFilePath pomPath) pom
              exitCode <- liftIO $ withCreateProcess ((proc "mvn" ["initialize"]) { cwd = Just (toFilePath releaseDir) }) $ \_ _ _ mvnHandle ->
                  waitForProcess mvnHandle
              unless (exitCode == ExitSuccess) $ do
                  $logError "Failed to install JARs locally."
                  liftIO exitFailure

          | otherwise ->
              $logInfo "Dry run selected: not uploading, not installing"
  where
    runLog Options{..} m0 = do
        let m = filterLogger (\_ ll -> ll >= LevelDebug) m0
        runFastLoggingT m

isSnapshot :: SemVer.Version -> Bool
isSnapshot v = List.notNull (view SemVer.release v)
