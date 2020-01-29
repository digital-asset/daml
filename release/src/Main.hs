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

main :: IO ()
main = do
  opts@Options{..} <- parseOptions
  runLog opts $ do
      releaseDir <- parseAbsDir =<< liftIO (Dir.makeAbsolute optsReleaseDir)
      liftIO $ createDirIfMissing True releaseDir
      $logInfo "Checking if we should release"
      sdkVersion <- readVersionAt "HEAD"
      release <- isReleaseCommit
      let upload = if release then optsPerformUpload else PerformUpload False
      -- NOTE(MH): We add 100 to get version numbers for the individual
      -- components which are bigger than all version numbers we used
      -- before moving to the new daml repo.
      let compVersion = sdkVersion{versionMajor = 100 + versionMajor sdkVersion}

      artifacts :: [Artifact (Maybe ArtifactLocation)] <- decodeFileThrow optsArtifacts

      let targets = concatMap buildTargets artifacts
      $logInfo "Building all targets"
      liftIO $ callProcess "bazel" ("build" : map (T.unpack . getBazelTarget) targets)

      bazelLocations <- liftIO getBazelLocations
      $logInfo "Reading metadata from pom files"
      artifacts <- liftIO $ mapM (resolvePomData bazelLocations sdkVersion compVersion) artifacts

      let mavenUploadArtifacts = filter (\a -> getMavenUpload $ artMavenUpload a) artifacts

      -- all known targets uploaded to maven, that are not deploy Jars
      -- we don't check dependencies for deploy jars as they are single-executable-jars
      let nonDeployJars = filter (not . isDeployJar . artReleaseType) mavenUploadArtifacts
      let allMavenTargets = Set.fromList $ fmap (T.unpack . getBazelTarget . artTarget) mavenUploadArtifacts

      -- first find out all the missing internal dependencies
      missingDepsForAllArtifacts <- forM nonDeployJars $ \a -> do
          -- run a bazel query to find all internal java and scala library dependencies
          -- We exclude the scenario service proto to avoid a false dependency on scala files
          -- originating from genrules that use damlc. This is a bit hacky but
          -- given that it’s fairly unlikely to accidentally introduce a dependency on the scenario
          -- service it doesn’t seem worth fixing properly.
          if getIgnoreMissingDeps optsIgnoreMissingDeps
              then pure (a, [])
              else do
                let bazelQueryCommand = shell $ "bazel query 'kind(\"(scala|java)_library\", deps(" ++ (T.unpack . getBazelTarget . artTarget) a ++ ")) intersect //... except //compiler/scenario-service/protos:scenario_service_java_proto'"
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

      files <- fmap concat $ forM artifacts $ \a -> do
          fs <- artifactFiles optsAllArtifacts a
          pure $ map (a,) fs
      mapM_ (\(_, (inp, outp)) -> copyToReleaseDir bazelLocations releaseDir inp outp) files

      uploadArtifacts <- concatMapM (mavenArtifactCoords optsAllArtifacts) mavenUploadArtifacts
      validateMavenArtifacts releaseDir uploadArtifacts

      -- npm packages we want to publish.
      let npmPackages =
            [ "//language-support/ts/daml-types"
            , "//language-support/ts/daml-ledger"
            ]
      -- make sure the npm packages can be build.
      $logDebug "Building language-support typescript packages"
      forM_ npmPackages $ \rule -> liftIO $ callCommand $ "bazel build " <> rule

      if | getPerformUpload upload -> do
              $logInfo "Make release"
              releaseToBintray upload releaseDir (map (\(a, (_, outp)) -> (a, outp)) files)

              -- Uploading to Maven Central
              mavenUploadConfig <- mavenConfigFromEnv
              if not (null uploadArtifacts)
                  then
                    uploadToMavenCentral mavenUploadConfig releaseDir uploadArtifacts
                  else
                    $logInfo "No artifacts to upload to Maven Central"

              let npmrcPath = ".npmrc"
              -- We can't put an .npmrc file in the root of the directory because other bazel npm
              -- code picks it up and looks for the token which is not yet set before the release
              -- phase.
              $logDebug "Uploading npm packages"
              liftIO $ bracket
                (writeFile npmrcPath "//registry.npmjs.org/:_authToken=${NPM_TOKEN}")
                (\() -> Dir.removeFile npmrcPath)
                (\() -> forM_ npmPackages
                  $ \rule -> liftIO $ callCommand $ "bazel run " <> rule <> ":npm_package.publish --access public")

              -- set variables for next steps in Azure pipelines
              liftIO . putStrLn $ "##vso[task.setvariable variable=has_released;isOutput=true]true"
              liftIO . putStrLn . T.unpack $ "##vso[task.setvariable variable=release_tag]" # renderVersion sdkVersion
         | optsLocallyInstallJars -> do
              let lib_jars = filter (\(mvn,_) -> (artifactType mvn == "jar" || artifactType mvn == "pom") && Maybe.isNothing (classifier mvn)) uploadArtifacts
              forM_ lib_jars $ \(mvn_coords, path) -> do
                  let args = ["install:install-file",
                              "-Dfile=" <> pathToString releaseDir <> pathToString path,
                              "-DgroupId=" <> foldr (<>) "" (List.intersperse "." $ map T.unpack $ groupId mvn_coords),
                              "-DartifactId=" <> (T.unpack $ artifactId mvn_coords),
                              "-Dversion=100.0.0",
                              "-Dpackaging=" <> (T.unpack $ artifactType mvn_coords)]
                  liftIO $ callProcess "mvn" args
         | otherwise -> $logInfo "Make dry run of release"
  where
    runLog Options{..} m0 = do
        let m = filterLogger (\_ ll -> ll >= optsLogLevel) m0
        if optsFullLogging then runStdoutLoggingT m else runFastLoggingT m
