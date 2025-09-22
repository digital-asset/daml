-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Project.Consts
    ( damlPathEnvVar
    , damlCacheEnvVar
    , projectPathEnvVar
    , packagePathEnvVar
    , sdkPathEnvVar
    , sdkVersionEnvVar
    , sdkVersionLatestEnvVar
    , damlAssistantEnvVar
    , damlAssistantVersionEnvVar
    , damlAssistantIsSet
    , damlConfigName
    , packageConfigName
    , sdkConfigName
    , multiPackageConfigName
    , damlEnvVars
    , getDamlPath
    , getPackagePath
    , getSdkPath
    , tryGetSdkPath
    , getSdkVersion
    , getSdkVersionDpm
    , getSdkVersionMaybe
    , getDamlAssistant
    , PackageLocationCheck(..)
    , withPackageRoot
    , withExpectPackageRoot
    ) where

import Control.Applicative ((<|>))
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
-- the current daml package. By default, this is done by traversing
-- up the directory structure until we find a "daml.yaml" file.
-- (deprecated, replaced by packagePathEnvVar, check for both in 3.4)
projectPathEnvVar :: String
projectPathEnvVar = "DAML_PROJECT"

-- | The DAML_PACKAGE environment variable determines the path of
-- the current daml package. By default, this is done by traversing
-- up the directory structure until we find a "daml.yaml" file.
-- This variable replaces the deprecated `DAML_PROJECT` environment variable
-- in DPM
packagePathEnvVar :: String
packagePathEnvVar = "DAML_PACKAGE"

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

sdkVersionDpmEnvVar :: String
sdkVersionDpmEnvVar = "DPM_SDK_VERSION"

-- | Latest stable version available from GitHub. Note that this is
-- updated based on the update-check property in the user's daml-config.yaml
-- file, which means it will possibly never be available.
sdkVersionLatestEnvVar :: String
sdkVersionLatestEnvVar = "DAML_SDK_VERSION_LATEST"

-- | The absolute path to the daml assistant executable.
damlAssistantEnvVar :: String
damlAssistantEnvVar = "DAML_ASSISTANT"

-- | The SDK version of the daml assistant. This does not necessarily equal
-- the DAML_SDK_VERSION, e.g. when working with a package with an older
-- pinned SDK version.
damlAssistantVersionEnvVar :: String
damlAssistantVersionEnvVar = "DAML_ASSISTANT_VERSION"

-- | File name of config file in DAML_HOME (~/.daml).
damlConfigName :: FilePath
damlConfigName = "daml-config.yaml"

-- | File name of config file in DAML_PACKAGE (the package path).
packageConfigName :: FilePath
packageConfigName = "daml.yaml"

-- | File name of config file in DAML_SDK (the sdk path)
sdkConfigName :: FilePath
sdkConfigName = "sdk-config.yaml"

-- | File name of optional multi-package config file
multiPackageConfigName :: FilePath
multiPackageConfigName = "multi-package.yaml"

-- | List of all environment variables handled by daml assistant.
damlEnvVars :: [String]
damlEnvVars =
    [ damlPathEnvVar
    , damlCacheEnvVar
    , projectPathEnvVar
    , packagePathEnvVar
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

-- | Returns the path of the current daml package or
--`Nothing` if invoked outside of a package.
getPackagePath :: IO (Maybe FilePath)
getPackagePath = do
    let nullToNothing p = if null p then Nothing else Just p
    mbPackagePath <- lookupEnv packagePathEnvVar
    mbProjectPath <- lookupEnv projectPathEnvVar
    pure $ (mbPackagePath >>= nullToNothing) <|> (mbProjectPath >>= nullToNothing)

-- | Returns the path of the sdk folder.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getSdkPath :: IO FilePath
getSdkPath = getEnv sdkPathEnvVar

-- | getSdkPath but can fail
tryGetSdkPath :: IO (Maybe FilePath)
tryGetSdkPath = lookupEnv sdkPathEnvVar

-- | Returns the current SDK version.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getSdkVersion :: IO String
getSdkVersion = getEnv sdkVersionEnvVar

-- | Returns the current SDK version via DPM.
--
-- This will throw an `IOException` if the environment has not been setup by
-- the assistant.
getSdkVersionDpm :: IO String
getSdkVersionDpm = getEnv sdkVersionDpmEnvVar

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

-- | Whether we should check if a command is invoked inside of a package.
-- The string is the command name used in error messages
data PackageLocationCheck = PackageLocationCheck String Bool

-- | Execute an action within the package root, if available.
--
-- Determines the package root, if available, using 'getPackagePath' unless it
-- is passed explicitly.
--
-- If no package root is found and 'PackageLocationCheck' requires a package root, then
-- an error is printed and the program is terminated before executing the given
-- action.
--
-- The provided action is executed on 'Just' the package root, if available,
-- otherwise on 'Nothing'. Additionally, it is passed a function to make
-- filepaths relative to the new working directory.
withPackageRoot
    :: Maybe PackagePath
    -> PackageLocationCheck
    -> (Maybe FilePath -> (FilePath -> IO FilePath) -> IO a)
    -> IO a
withPackageRoot mbProjectDir (PackageLocationCheck cmdName check) act = do
    previousCwd <- getCurrentDirectory
    mbProjectPath <- maybe getPackagePath (pure . Just . unwrapPackagePath) mbProjectDir
    case mbProjectPath of
        Nothing -> do
            when check $ do
                hPutStrLn stderr (cmdName <> ": Not in package.")
                exitFailure
            act Nothing pure
        Just packagePath -> do
            packagePath <- canonicalizePath packagePath
            withCurrentDirectory packagePath $ act (Just packagePath) $ \f -> do
                absF <- canonicalizePath (previousCwd </> f)
                pure (packagePath `makeRelative` absF)

-- | Same as 'withPackageRoot' but always requires package root.
withExpectPackageRoot
    :: Maybe PackagePath  -- ^ optionally specified package root
    -> String  -- ^ command name for error message
    -> (FilePath -> (FilePath -> IO FilePath) -> IO a)  -- ^ action
    -> IO a
withExpectPackageRoot mbProjectDir cmdName act = do
    withPackageRoot mbProjectDir (PackageLocationCheck cmdName True) $ \case
        Nothing -> error "withPackageRoot should terminated the program"
        Just packagePath -> act packagePath
