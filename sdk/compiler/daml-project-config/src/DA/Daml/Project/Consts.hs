-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Project.Consts
    ( damlCacheEnvVar
    , projectPathEnvVar
    , packagePathEnvVar
    , sdkVersionDpmEnvVar
    , damlConfigName
    , packageConfigName
    , multiPackageConfigName
    , damlEnvVars
    , getPackagePath
    , getAssemblyVersionMaybe
    , PackageLocationCheck(..)
    , withPackageRoot
    , withExpectPackageRoot
    , getCachePath
    , getVersionInfo
    , getComponentVersionInfo
    , getAssemblyVersionFromComponentVersion
    ) where

import Control.Applicative ((<|>))
import Control.Monad
import Control.Monad.Extra (maybeM)
import Control.Exception
import System.Directory
import System.Environment
import qualified System.Environment.Blank as EnvBlank
import System.Exit
import System.FilePath
import System.IO
import qualified Data.Text as T

import DA.Daml.Project.Types
import ComponentVersion.Class (ComponentVersioned, getComponentVersion)

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

-- | The DPM_SDK_VERSION environment variable determines the
-- current or preferred sdk version. By default this is, in order
-- of preference:
--
-- 1. taken from the current $DAML_PROJECT config file, if it exists
-- 2. read from $DAML_SDK/VERSION file, if DAML_SDK is explicitly set
-- 3. the latest stable SDK version available in $DAML_HOME/sdk.
sdkVersionDpmEnvVar :: String
sdkVersionDpmEnvVar = "DPM_SDK_VERSION"

-- | File name of config file in DAML_HOME (~/.daml).
damlConfigName :: FilePath
damlConfigName = "daml-config.yaml"

-- | File name of config file in DAML_PACKAGE (the package path).
packageConfigName :: FilePath
packageConfigName = "daml.yaml"

-- | File name of optional multi-package config file
multiPackageConfigName :: FilePath
multiPackageConfigName = "multi-package.yaml"

-- | List of all environment variables handled by daml assistant.
damlEnvVars :: [String]
damlEnvVars =
    [ damlCacheEnvVar
    , projectPathEnvVar
    , packagePathEnvVar
    ]

-- | Returns the path of the current daml package or
--`Nothing` if invoked outside of a package.
getPackagePath :: IO (Maybe FilePath)
getPackagePath = do
    let nullToNothing p = if null p then Nothing else Just p
    mbPackagePath <- lookupEnv packagePathEnvVar
    mbProjectPath <- lookupEnv projectPathEnvVar
    pure $ (mbPackagePath >>= nullToNothing) <|> (mbProjectPath >>= nullToNothing)

-- | Returns the DPM sdk version via its environment variable.
-- DPM will not provide a version if the package is using only component overrides
getAssemblyVersionMaybe :: IO (Maybe AssemblyVersion)
getAssemblyVersionMaybe = do
  mVer <- EnvBlank.getEnv sdkVersionDpmEnvVar
  let unblankedVer = mVer >>= \case
        "" -> Nothing
        x -> Just x
  forM unblankedVer $ either throwIO pure . parseAssemblyVersion . T.pack

-- | Returns the assembly and component version by checking the component typeclass, and assembly version env var (via getAssemblyVersionMaybe)
getVersionInfo :: ComponentVersioned => IO VersionInfo
getVersionInfo = flip VersionInfo getComponentVersion <$> getAssemblyVersionMaybe

-- | Returns assembly version of the component version, for cases where the assembly version is unknown
getAssemblyVersionFromComponentVersion :: ComponentVersioned => AssemblyVersion
getAssemblyVersionFromComponentVersion = AssemblyVersion $ unwrapComponentVersion getComponentVersion

-- | Returns a version info where both component and assembly versions are the damlc component version. Use only in tests
getComponentVersionInfo :: ComponentVersioned => VersionInfo
getComponentVersionInfo = VersionInfo (Just getAssemblyVersionFromComponentVersion) getComponentVersion

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

-- | (internal) Override function with environment variable
-- if it is available.
overrideWithEnvVar
    :: String                   -- ^ env var name
    -> (String -> IO String)    -- ^ normalize env var
    -> (String -> t)            -- ^ parser for env var
    -> IO t                     -- ^ calculation to override
    -> IO t
overrideWithEnvVar envVar normalize parse calculate =
    maybeM calculate (normalize >=> pure . parse) (EnvBlank.getEnv envVar)

-- | Get the Daml cache folder. This defaults to $XDG_CACHE_HOME/daml.
getCachePath :: IO CachePath
getCachePath =
    overrideWithEnvVar damlCacheEnvVar makeAbsolute CachePath $
        CachePath <$> getXdgDirectory XdgCache "daml"
