-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell, MultiWayIf #-}
module Main (main) where

import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Exception
import Data.Yaml
import qualified Data.Set as Set
import qualified Data.List as List
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

      let mavenUploadArtifacts = filter (\a -> getMavenUpload $ artMavenUpload a) mvnArtifacts
      -- all known targets uploaded to maven, that are not deploy Jars
      -- we don't check dependencies for deploy jars as they are single-executable-jars
      let nonDeployJars = filter (not . isDeployJar . artReleaseType) mavenUploadArtifacts
      let allMavenTargets = Set.fromList $ fmap (T.unpack . getBazelTarget . artTarget) mavenUploadArtifacts

      -- first find out all the missing internal dependencies
      missingDepsForAllArtifacts <- forM nonDeployJars $ \a -> do
          -- run a bazel query to find all internal java and scala library dependencies
          -- We exclude the scenario service and the script service to avoid a false dependency on scala files
          -- originating from genrules that use damlc. This is a bit hacky but
          -- given that it’s fairly unlikely to accidentally introduce a dependency on the scenario
          -- service it doesn’t seem worth fixing properly.
          let bazelQueryCommand = shell $
                  "bazel query 'kind(\"(scala|java)_library\", deps(" ++
                  (T.unpack . getBazelTarget . artTarget) a ++
                  ")) intersect //... " ++
                  "except (//compiler/scenario-service/protos:scenario_service_java_proto + //compiler/repl-service/protos:repl_service_java_proto + //daml-script/runner:script-runner-lib)'"
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

      mvnUploadArtifacts <- concatMapM mavenArtifactCoords mavenUploadArtifacts
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
              $logInfo "Uploading to Bintray"
              releaseToBintray releaseDir (map (\(a, (_, outp)) -> (a, outp)) mvnFiles)

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
                              "-DgroupId=" <> foldr (<>) "" (List.intersperse "." $ map T.unpack $ groupId mvn_coords),
                              "-DartifactId=" <> (T.unpack $ artifactId mvn_coords),
                              "-Dversion=100.0.0",
                              "-Dpackaging=" <> (T.unpack $ artifactType mvn_coords)]
                  liftIO $ callProcess "mvn" args
         | otherwise -> $logInfo "Dry run selected: not uploading, not installing"
  where
    runLog Options{..} m0 = do
        let m = filterLogger (\_ ll -> ll >= LevelDebug) m0
        runFastLoggingT m
