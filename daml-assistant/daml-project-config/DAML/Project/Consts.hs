-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DAML.Project.Consts
    ( damlPathEnvVar
    , projectPathEnvVar
    , sdkPathEnvVar
    , sdkVersionEnvVar
    , sdkVersionLatestEnvVar
    , damlAssistantEnvVar
    , damlAssistantVersionEnvVar
    , damlConfigName
    , projectConfigName
    , sdkConfigName
    , damlEnvVars
    , getDamlPath
    , getProjectPath
    , getSdkPath
    , getSdkVersion
    , getDamlAssistant
    , withProjectRoot
    , makeRelativeToRoot
    ) where

import System.Directory
import System.Environment
import System.FilePath
import Data.Maybe

-- | The DAML_HOME environment variable determines the path of the daml
-- assistant data directory. This defaults to:
--
--     System.Directory.getAppUserDataDirectory "daml"
--
-- On Linux and Mac, that's ~/.daml
-- On Windows, that's %APPDATA%/daml
damlPathEnvVar :: String
damlPathEnvVar = "DAML_HOME"

-- | The DAML_PROJECT environment variable determines the path of
-- the current daml project. By default, this is done by traversing
-- up the directory structure until we find a "daml.yaml" file.
projectPathEnvVar :: String
projectPathEnvVar = "DAML_PROJECT"

-- | The DAML_SDK environment variable determines the path of the
-- sdk folder. By default, this is calculated as
-- $DAML_HOME/sdk/$DAML_SDK_VERSION.
sdkPathEnvVar :: String
sdkPathEnvVar = "DAML_SDK"

-- | The DAML_SDK_VERSION environment variable determines the
-- current or preferred sdk version. By default this is, in order
-- of preference:
--
-- 1. taken from the current $DAML_PROJECT config file, if it exists
-- 2. read from $DAML_SDK/VERSION file, if DAML_SDK is explicitly set
-- 3. the latest stable SDK version available in $DAML_HOME/sdk.
sdkVersionEnvVar :: String
sdkVersionEnvVar = "DAML_SDK_VERSION"

-- | Latest stable version available from GitHub. Note that this is
-- updated based on the update-check property in the user's daml-config.yaml
-- file, which means it will possibly never be available.
sdkVersionLatestEnvVar :: String
sdkVersionLatestEnvVar = "DAML_SDK_VERSION_LATEST"

-- | The absolute path to the daml assistant executable.
damlAssistantEnvVar :: String
damlAssistantEnvVar = "DAML_ASSISTANT"

-- | The SDK version of the daml assistant. This does not necessarily equal
-- the DAML_SDK_VERSION, e.g. when working with a project with an older
-- pinned SDK version.
damlAssistantVersionEnvVar :: String
damlAssistantVersionEnvVar = "DAML_ASSISTANT_VERSION"

-- | File name of config file in DAML_HOME (~/.daml).
damlConfigName :: FilePath
damlConfigName = "daml-config.yaml"

-- | File name of config file in DAML_PROJECT (the project path).
projectConfigName :: FilePath
projectConfigName = "daml.yaml"

-- | File name of config file in DAML_SDK (the sdk path)
sdkConfigName :: FilePath
sdkConfigName = "sdk-config.yaml"

-- | List of all environment variables handled by daml assistant.
damlEnvVars :: [String]
damlEnvVars =
    [ damlPathEnvVar
    , projectPathEnvVar
    , sdkPathEnvVar
    , sdkVersionEnvVar
    , sdkVersionLatestEnvVar
    , damlAssistantEnvVar
    , damlAssistantVersionEnvVar
    ]

-- | Returns the path to the daml assistant data directory.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getDamlPath :: IO FilePath
getDamlPath = getEnv damlPathEnvVar

-- | Returns the path of the current daml project or
--`Nothing` if invoked outside of a project.
getProjectPath :: IO (Maybe FilePath)
getProjectPath = do
    mbProjectPath <- lookupEnv projectPathEnvVar
    pure ((\p -> if null p then Nothing else Just p) =<< mbProjectPath)

-- | Returns the path of the sdk folder.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getSdkPath :: IO FilePath
getSdkPath = getEnv sdkPathEnvVar

-- | Returns the current SDK version.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getSdkVersion  :: IO String
getSdkVersion = getEnv sdkVersionEnvVar

-- | Returns the absolute path to the assistant.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getDamlAssistant :: IO FilePath
getDamlAssistant = getEnv damlAssistantEnvVar

-- | This function changes the working directory to the project root and calls
-- the supplied action with a function to transform filepaths relative to the previous
-- directory into filepaths relative to the project root (absolute file paths will not be modified).
--
-- When called outside of a project or outside of the environment setup by the assistant,
-- this function will make the filepath relative to the current working directory
withProjectRoot :: ((FilePath -> IO FilePath) -> IO a) -> IO a
withProjectRoot act = do
    previousCwd <- getCurrentDirectory
    mbProjectPath <- getProjectPath
    let projectPath = fromMaybe previousCwd mbProjectPath
    projectPath <- makeAbsolute projectPath
    withCurrentDirectory projectPath $ act $ \f -> do
        absF <- makeAbsolute (previousCwd </> f)
        pure $ projectPath `makeRelative` absF

makeRelativeToRoot :: FilePath -> IO FilePath
makeRelativeToRoot fp =
    withProjectRoot ($ fp)
