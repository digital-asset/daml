-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Project.Consts
    ( damlPathEnvVar
    , damlCacheEnvVar
    , projectPathEnvVar
    , sdkPathEnvVar
    , sdkVersionEnvVar
    , sdkVersionLatestEnvVar
    , damlAssistantEnvVar
    , damlAssistantVersionEnvVar
    , damlAssistantIsSet
    , damlConfigName
    , projectConfigName
    , sdkConfigName
    , multiPackageConfigName
    , damlEnvVars
    , getDamlPath
    , getProjectPath
    , getSdkPath
    , getSdkVersion
    , getSdkVersionMaybe
    , getDamlAssistant
    , ProjectCheck(..)
    , withProjectRoot
    , withExpectProjectRoot
    ) where

import Control.Monad
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import System.IO
import qualified Data.Text as T
import Data.Maybe (isJust)

import DA.Daml.Project.Types

-- | The DAML_HOME environment variable determines the path of the daml
-- assistant data directory. This defaults to:
--
--     System.Directory.getAppUserDataDirectory "daml"
--
-- On Linux and Mac, that's ~/.daml
-- On Windows, that's %APPDATA%/daml
damlPathEnvVar :: String
damlPathEnvVar = "DAML_HOME"

-- | The DAML_CACHE environment variable determines the path of the daml
-- assistant cache directory. This defaults to XDG_CACHE_HOME.
damlCacheEnvVar :: String
damlCacheEnvVar = "DAML_CACHE"

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

-- | File name of optional multi-package config file in DAML_PROJECT (the project path).
multiPackageConfigName :: FilePath
multiPackageConfigName = "multi-package.yaml"

-- | List of all environment variables handled by daml assistant.
damlEnvVars :: [String]
damlEnvVars =
    [ damlPathEnvVar
    , damlCacheEnvVar
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
getSdkVersion :: IO String
getSdkVersion = getEnv sdkVersionEnvVar

-- | Returns the current SDK version if set, or Nothing.
getSdkVersionMaybe :: IO (Maybe (Either InvalidVersion UnresolvedReleaseVersion))
getSdkVersionMaybe = (fmap . fmap) (parseVersion . T.pack) $ lookupEnv sdkVersionEnvVar

-- | Returns the absolute path to the assistant.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getDamlAssistant :: IO FilePath
getDamlAssistant = getEnv damlAssistantEnvVar

damlAssistantIsSet :: IO Bool
damlAssistantIsSet = isJust <$> lookupEnv damlAssistantEnvVar

-- | Whether we should check if a command is invoked inside of a project.
-- The string is the command name used in error messages
data ProjectCheck = ProjectCheck String Bool

-- | Execute an action within the project root, if available.
--
-- Determines the project root, if available, using 'getProjectPath' unless it
-- is passed explicitly.
--
-- If no project root is found and 'ProjectCheck' requires a project root, then
-- an error is printed and the program is terminated before executing the given
-- action.
--
-- The provided action is executed on 'Just' the project root, if available,
-- otherwise on 'Nothing'. Additionally, it is passed a function to make
-- filepaths relative to the new working directory.
withProjectRoot
    :: Maybe ProjectPath
    -> ProjectCheck
    -> (Maybe FilePath -> (FilePath -> IO FilePath) -> IO a)
    -> IO a
withProjectRoot mbProjectDir (ProjectCheck cmdName check) act = do
    previousCwd <- getCurrentDirectory
    mbProjectPath <- maybe getProjectPath (pure . Just . unwrapProjectPath) mbProjectDir
    case mbProjectPath of
        Nothing -> do
            when check $ do
                hPutStrLn stderr (cmdName <> ": Not in project.")
                exitFailure
            act Nothing pure
        Just projectPath -> do
            projectPath <- canonicalizePath projectPath
            withCurrentDirectory projectPath $ act (Just projectPath) $ \f -> do
                absF <- canonicalizePath (previousCwd </> f)
                pure (projectPath `makeRelative` absF)

-- | Same as 'withProjectRoot' but always requires project root.
withExpectProjectRoot
    :: Maybe ProjectPath  -- ^ optionally specified project root
    -> String  -- ^ command name for error message
    -> (FilePath -> (FilePath -> IO FilePath) -> IO a)  -- ^ action
    -> IO a
withExpectProjectRoot mbProjectDir cmdName act = do
    withProjectRoot mbProjectDir (ProjectCheck cmdName True) $ \case
        Nothing -> error "withProjectRoot should terminated the program"
        Just projectPath -> act projectPath
