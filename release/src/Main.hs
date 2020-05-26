-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell, MultiWayIf #-}
module Main (main) where

import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Exception
import Data.Yaml
import qualified Data.Set as Set
import qualified Data.List.Extra as List
import Path
import Path.IO hiding (removeFile)

import qualified Data.Text as T
import qualified Data.Maybe as Maybe
import qualified System.Directory as Dir
import System.Exit
import System.Process

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

      let mvnVersion = Version $ T.pack SdkVersion.mvnVersion
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
      missingDepsForAllArtifacts <- forM nonDeployJars $ \a -> do
          -- run a bazel query to find all internal java and scala library dependencies
          -- We exclude the scenario service and the script service to avoid a false dependency on scala files
          -- originating from genrules that use damlc. This is a bit hacky but
          -- given that it’s fairly unlikely to accidentally introduce a dependency on the scenario
          -- service it doesn’t seem worth fixing properly.
          let bazelQueryCommand = shell $
                  "bazel query 'kind(\"(scala|java)_library\", deps(" ++
                  (T.unpack . getBazelTarget . artTarget) a ++
                  ")) intersect //...'"
          internalDeps <- liftIO $ lines <$> readCreateProcess bazelQueryCommand ""
          -- check if a dependency is not already a maven target from artifacts.yaml
          let missingDeps = filter (`Set.notMember` allMavenTargets) internalDeps
          return (a, missingDeps)

      let onlyMissing = filter (not . null . snd) missingDepsForAllArtifacts
      -- now we can report all the missing dependencies per artifact
      when (not (null onlyMissing)) $ do
                  $logError "Some internal dependencies are not published to maven central!"
                  forM_ onlyMissing $ \(artifact, missingDeps) -> do
                      $logError (getBazelTarget (artTarget artifact))
                      forM_ missingDeps $ \dep -> $logError ("\t- "# T.pack dep)
                  liftIO exitFailure

      mvnFiles <- fmap concat $ forM mvnArtifacts $ \a -> do
          fs <- artifactFiles a
          pure $ map (a,) fs
      mapM_ (\(_, (inp, outp)) -> copyToReleaseDir bazelLocations releaseDir inp outp) mvnFiles

      mvnUploadArtifacts <- concatMapM mavenArtifactCoords mvnArtifacts
      validateMavenArtifacts releaseDir mvnUploadArtifacts

      -- npm packages we want to publish.
      let npmPackages =
            [ "//language-support/ts/daml-types"
            , "//language-support/ts/daml-ledger"
            , "//language-support/ts/daml-react"
            ]
      -- make sure the npm packages can be build.
      $logDebug "Building language-support typescript packages"
      forM_ npmPackages $ \rule -> liftIO $ callCommand $ "bazel build " <> rule

      if | getPerformUpload optsPerformUpload -> do
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
                  $ \rule -> liftIO $ callCommand $ "bazel run " <> rule <> ":npm_package.publish -- --access public")

         | optsLocallyInstallJars -> do
              let lib_jars = filter (\(mvn,_) -> (artifactType mvn == "jar" || artifactType mvn == "pom") && Maybe.isNothing (classifier mvn)) mvnUploadArtifacts
              forM_ lib_jars $ \(mvn_coords, path) -> do
                  let args = ["install:install-file",
                              "-Dfile=" <> pathToString releaseDir <> pathToString path,
                              "-DgroupId=" <> (groupIdString $ groupId mvn_coords),
                              "-DartifactId=" <> (T.unpack $ artifactId mvn_coords),
                              "-Dversion=0.0.0",
                              "-Dpackaging=" <> (T.unpack $ artifactType mvn_coords)]
                  liftIO $ callProcess "mvn" args
         | otherwise -> $logInfo "Dry run selected: not uploading, not installing"
  where
    runLog Options{..} m0 = do
        let m = filterLogger (\_ ll -> ll >= LevelDebug) m0
        runFastLoggingT m
