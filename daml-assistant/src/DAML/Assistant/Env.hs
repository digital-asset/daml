-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Env
    ( Env (..)
    , DamlPath (..)
    , ProjectPath (..)
    , SdkPath (..)
    , SdkVersion (..)
    , getDamlEnv
    , testDamlEnv
    , getDamlPath
    , getProjectPath
    , getSdk
    , getDispatchEnv
    ) where

import DAML.Project.Config
import DAML.Project.Consts hiding (getDamlPath, getProjectPath)
import DAML.Project.Types
import DAML.Project.Util
import System.Directory
import System.FilePath
import System.Environment
import Control.Monad.Extra
import Control.Exception.Safe
import Data.List.Extra
import Data.Maybe
import Safe

-- | Calculate the environment variables in which to run daml-something.
getDamlEnv :: IO Env
getDamlEnv = do
    envDamlPath <- getDamlPath
    envProjectPath <- getProjectPath
    (envSdkVersion, envSdkPath) <- getSdk envDamlPath envProjectPath
    pure Env {..}

-- | Determine the viability of running sdk commands in the environment.
-- Returns the first failing test's error message.
testDamlEnv :: Env -> IO (Maybe Text)
testDamlEnv Env{..} = firstJustM (\(test, msg) -> unlessMaybeM test (pure msg))
    [ ( doesDirectoryExist (unwrapDamlPath envDamlPath)
      ,  "The daml home directory does not exist. Please check if DAML_HOME is incorrectly set, "
      <> "or run \n\n    daml install --initial\n\nto create the daml home directory and "
      <> "install the SDK." )
    , ( pure (isJust envSdkVersion)
      ,  "Could not determine SDK version. Please check if DAML_HOME is incorrectly set, or "
      <> "check the daml project config file, or run \"daml install\" to install the latest "
      <> "version of the SDK." )
    , ( maybe (pure False) (doesDirectoryExist . unwrapSdkPath) envSdkPath
      ,  "The DAML SDK directory does not exist. Please check if DAML_SDK or DAML_SDK_VERSION "
      <> "are incorrectly set, or run \"daml install\" to install the appropriate SDK version.")
    , ( maybe (pure True) (doesDirectoryExist . unwrapProjectPath) envProjectPath
      , "The project directory does not exist. Please check if DAML_PROJECT is incorrectly set.")
    ]

-- | Determine absolute path of daml home directory.
--
-- On Linux and Mac this is ~/.daml by default.
-- On Windows this is %APPDATA%/daml by default.
-- This default can be overriden with the DAML_HOME environment variable.
--
-- Raises an AssistantError if the path is missing.
getDamlPath :: IO DamlPath
getDamlPath = wrapErr "Determining daml home directory." $ do
    path <- required "Failed to determine daml path." =<< firstJustM id
        [ lookupEnv damlPathEnvVar
        , Just <$> getAppUserDataDirectory "daml"
        ]
    pure (DamlPath path)

-- | Calculate the project path. This is done by starting at the current
-- working directory, checking if "da.yaml" is present. If it is found,
-- that's the project path. Otherwise, go up one level and repeat
-- until you can't go up.
--
-- The project path can be overriden by passing the DAML_PROJECT
-- environment variable.
getProjectPath :: IO (Maybe ProjectPath)
getProjectPath = wrapErr "Detecting daml project." $ do
        pathM <- firstJustM id
            [ lookupEnv projectPathEnvVar
            , findM hasProjectConfig . ascendants =<< getCurrentDirectory
            ]
        pure (ProjectPath <$> pathM)

    where
        hasProjectConfig :: FilePath -> IO Bool
        hasProjectConfig p = doesFileExist (p </> projectConfigName)


-- | Calculate the current SDK version and path.
--
-- These can be overriden by the environment variables DAML_SDK_VERSION
-- and DAML_SDK_PATH (and it ought to be enough to supply one of these
-- and have the other be inferred).
getSdk :: DamlPath
       -> Maybe ProjectPath
       -> IO (Maybe SdkVersion, Maybe SdkPath)
getSdk damlPath projectPathM =
    wrapErr "Determining SDK version and path." $ do

        sdkVersion <- firstJustM id
            [ fmap (SdkVersion . pack) <$> lookupEnv sdkVersionEnvVar
            , fromConfig "SDK" (lookupEnv sdkPathEnvVar)
                               (readSdkConfig . SdkPath)
                               (fmap Just . sdkVersionFromSdkConfig)
            , fromConfig "project" (pure projectPathM)
                                    readProjectConfig
                                    sdkVersionFromProjectConfig
            , getLatestInstalledSdkVersion damlPath
            ]

        sdkPath <- firstJustM id
            [ fmap SdkPath <$> lookupEnv sdkPathEnvVar
            , pure (defaultSdkPath damlPath <$> sdkVersion)
            ]

        return (sdkVersion, sdkPath)

    where
        fromConfig :: Text
                   -> IO (Maybe path)
                   -> (path -> IO config)
                   -> (config -> Either AssistantError (Maybe SdkVersion))
                   -> IO (Maybe SdkVersion)
        fromConfig name lookupPath readConfig parseVersion =
            wrapErr ("Determining SDK version from " <> name <> " config.") $ do
                pathM <- lookupPath
                fmap join . forM pathM $ \path -> do
                    config <- readConfig path
                    fromRightM throwIO (parseVersion config)

-- | Determine the latest installed version of the SDK.
-- Currently restricts to versions with the prefix "nightly-",
-- but this is bound to change and be configurable.
getLatestInstalledSdkVersion :: DamlPath -> IO (Maybe SdkVersion)
getLatestInstalledSdkVersion (DamlPath path) = do
    let dpath = path </> "sdk"
    wrapErr "Determining latest installed sdk version." $ do
        versionMM <- whenMaybeM (doesDirectoryExist path) $ do
            dirlistE <- tryIO $ listDirectory dpath
            dirlist <- requiredE ("Failed to list daml home sdk directory " <> pack dpath) dirlistE
            subdirs <- filterM (doesDirectoryExist . (dpath </>)) dirlist
            -- TODO (FAFM): warn if subdirs /= dirlist
            --  (i.e. $DAML_HOME/sdk is polluted with non-dirs).
            let versions = filter ("nightly-" `isPrefixOf`) subdirs
                -- TODO (FAFM): configurable channels
            pure $ fmap (SdkVersion . pack) (maximumMay versions)
        pure (join versionMM)

-- | Calculate the environment for dispatched commands (i.e. the environment
-- with updated DAML_HOME, DAML_PROJECT, DAML_SDK, etc).
getDispatchEnv :: Env -> IO [(String, String)]
getDispatchEnv Env{..} = do
    originalEnv <- getEnvironment
    pure $ [ (damlPathEnvVar, unwrapDamlPath envDamlPath)
           , (projectPathEnvVar, maybe "" unwrapProjectPath envProjectPath)
           , (sdkPathEnvVar, maybe "" unwrapSdkPath envSdkPath)
           , (sdkVersionEnvVar, maybe "" (unpack . unwrapSdkVersion) envSdkVersion)
           ] ++ filter ((`notElem` damlEnvVars) . fst) originalEnv
