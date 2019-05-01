-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Hidden test commands
module DA.Sdk.Cli.Test(module DA.Sdk.Cli.Test) where

import           DA.Sdk.Prelude             hiding (group)
import DA.Sdk.Cli.Command (TestAction(..))
import DA.Sdk.Cli.Metadata (Metadata)
import qualified DA.Sdk.Cli.Locations as L
import qualified DA.Sdk.Cli.Metadata as M
import qualified Data.Yaml                  as Yaml
import qualified System.Exit
import System.IO (stderr, hPrint)
import           DA.Sdk.Cli.Conf (defaultRepositoryURLs)
import qualified DA.Sdk.Cli.Sdk as Sdk
import qualified DA.Sdk.Cli.Template as Template
import DA.Sdk.Cli.Monad
import qualified DA.Sdk.Cli.SdkVersion     as SdkVersion
import qualified Data.Map.Strict as MS
import qualified DA.Sdk.Cli.Repository      as Repo
import DA.Sdk.Cli.Repository.Bintray
import DA.Sdk.Cli.Conf.Types         (RepositoryURLs(..))
import qualified Text.Pretty.Simple as PP
import qualified Turtle
import System.IO.Temp (withSystemTempDirectory)
import qualified Network.HTTP.Client.TLS as Client
import Servant.Client (ClientEnv(..))
import DA.Sdk.Cli.Monad.UserInteraction

runTestAction :: TestAction -> IO ()
runTestAction (ParseSDKYaml filepath) = do
    errOrMeta <- Yaml.decodeFileEither (pathToString filepath) :: IO (Either Yaml.ParseException Metadata)
    case errOrMeta of
      Left err   -> do
        hPrint stderr err
        System.Exit.exitFailure
      Right meta -> PP.pPrint meta

-- | Download and extract documentation packages from a specific SDK release.
-- This is used when compiling the main SDK documentation.
runTestAction (DownloadDocs user apiKey filepath targetPath) = do
    Right meta <- Yaml.decodeFileEither (pathToString filepath)
    mgr <- Client.newTlsManager
    let apiURL = repositoryAPIURL defaultRepositoryURLs
        dlURL  = repositoryDownloadURL defaultRepositoryURLs
    reqHandle <- makeBintrayRequestHandle dlURL (ClientEnv mgr apiURL Nothing)
    handle <- Repo.makeBintrayHandle reqHandle defaultRepositoryURLs credentials
    forM_ (MS.toList $ M._mGroups meta) $ \(_name, group) ->
      forM_ (MS.toList $ M._gPackages group) $ \(artifactId, package) ->
        when (M._pDoc package) $ do
          putText $ artifactId <> ": "
          errOrOk <- Sdk.installPackage handle False (L.FilePathOf targetPath) (L.FilePathOf targetPath)
                                    daRepository artifactId group package
          -- TODO: investigate whether targetPaths should be different in above line
          case errOrOk of
            Left err -> error $ show err
            Right _ -> display "ok."
  where
    credentials = Repo.Credentials (Just (Repo.BintrayCredentials user apiKey))

-- | Run the test suites of every template found in currently active SDK
-- version
runTestTemplates :: CliM ()
runTestTemplates = do
    sdkVersion <- SdkVersion.getActiveSdkVersion >>= SdkVersion.requireSdkVersion
    allTemplates <-
        (<>) <$> Template.getReleaseTemplates Nothing sdkVersion
             <*> Template.getBuiltinTemplates Nothing sdkVersion

    -- Run the test script for all templates that have it
    exitCodes <- forM allTemplates $ \(Template.TemplateInfo name mbPath version _desc _type) -> do
     let path       = fromMaybe "" mbPath
         testScript = path </> Template.templateScriptsDirectory </> "test"
     testExists <- Turtle.testfile testScript

     if testExists
     then liftIO $ withSystemTempDirectory "template-test" $ \tmpdir -> do
         let tmpdirPath = stringToPath tmpdir
         Turtle.cptree path tmpdirPath
         Turtle.cd tmpdirPath
         display $ "--- testing " <> name <> "-" <> version <> " ..."
         exitStatus <- Turtle.proc (pathToText $ Template.templateScriptsDirectory </> "test") [] empty
         when (exitStatus /= Turtle.ExitSuccess)
           $ display $ "--- tests for " <> name <> " failed."
         pure exitStatus

     else pure Turtle.ExitSuccess

    -- Exit with failure if any of the tests failed
    unless (all (== Turtle.ExitSuccess) exitCodes) $ do
      liftIO System.Exit.exitFailure
