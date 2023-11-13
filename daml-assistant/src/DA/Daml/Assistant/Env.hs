-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Env
    ( EnvF (..)
    , DamlPath (..)
    , ProjectPath (..)
    , SdkPath (..)
    , SdkVersion (..)
    , getDamlEnv
    , testDamlEnv
    , getDamlPath
    , getCachePath
    , getProjectPath
    , getSdk
    , getDispatchEnv
    , envUseCache
    , forceEnv
    ) where

import Control.Exception.Safe
import Control.Monad.Extra
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import DA.Daml.Assistant.Version
import DA.Daml.Project.Consts hiding (getDamlPath, getProjectPath)
import Data.Maybe
import System.Directory
import System.Environment.Blank
import System.FilePath
import System.Info.Extra

-- | Calculate the environment variables in which to run daml commands.
getDamlEnv :: DamlPath -> LookForProjectPath -> IO Env
getDamlEnv envDamlPath lookForProjectPath = do
    envDamlAssistantSdkVersion <- getDamlAssistantSdkVersion
    envDamlAssistantPath <- getDamlAssistantPath envDamlPath
    envProjectPath <- getProjectPath lookForProjectPath
    (envSdkVersion, envSdkPath) <- getSdk envDamlPath envProjectPath
    envCachePath <- getCachePath
    envFreshStableSdkVersionForCheck <- getFreshStableSdkVersionForCheck (mkUseCache envCachePath envDamlPath)
    pure Env {..}

envUseCache :: Env -> UseCache
envUseCache Env {..} =
  mkUseCache envCachePath envDamlPath

mkUseCache :: CachePath -> DamlPath -> UseCache
mkUseCache cachePath damlPath =
  UseCache { cachePath, damlPath, overrideTimeout = Nothing }

-- | (internal) Override function with environment variable
-- if it is available.
overrideWithEnvVar
    :: String                   -- ^ env var name
    -> (String -> IO String)    -- ^ normalize env var
    -> (String -> t)            -- ^ parser for env var
    -> IO t                     -- ^ calculation to override
    -> IO t
overrideWithEnvVar envVar normalize parse calculate =
    maybeM calculate (normalize >=> pure . parse) (getEnv envVar)

-- | (internal) Same as overrideWithEnvVar but accepts "" as
-- Nothing and throws exception on parse failure.
overrideWithEnvVarMaybe
    :: Exception e
    => String                   -- ^ env var name
    -> (String -> IO String)    -- ^ normalize env var
    -> (String -> Either e t)   -- ^ parser for env var
    -> IO (Maybe t)             -- ^ calculation to override
    -> IO (Maybe t)
overrideWithEnvVarMaybe envVar normalize parse calculate = do
    valueM <- getEnv envVar
    case valueM of
        Nothing -> calculate
        Just "" -> pure Nothing
        Just value -> do
            value <- normalize value
            Just <$> requiredE
                ("Invalid value for environment variable " <> pack envVar <> ".")
                (parse value)

-- | Get the latest stable SDK version. Can be overriden with
-- DAML_SDK_LATEST_VERSION environment variable.
getFreshStableSdkVersionForCheck :: UseCache -> IO (IO (Maybe SdkVersion))
getFreshStableSdkVersionForCheck useCache = do
  val <- getEnv sdkVersionLatestEnvVar
  case val of
    Nothing -> pure (freshMaximumOfVersions (getAvailableReleaseVersions useCache))
    Just "" -> pure (pure Nothing)
    Just value -> do
      parsed <- requiredE
        ("Invalid value for environment variable " <> pack sdkVersionLatestEnvVar <> ".")
        (parseVersion (pack value))
      pure (pure (Just parsed))

-- | Determine the viability of running sdk commands in the environment.
-- Returns the first failing test's error message.
testDamlEnv :: Env -> IO (Maybe Text)
testDamlEnv Env{..} = firstJustM (\(test, msg) -> unlessMaybeM test (pure msg))
    [ ( doesDirectoryExist (unwrapDamlPath envDamlPath)
      ,  "The Daml home directory does not exist. Please check if DAML_HOME is incorrectly set, "
      <> "or run \n\n    daml install --initial\n\nto create the daml home directory and "
      <> "install the SDK." )
    , ( pure (isJust envSdkVersion)
      ,  "Could not determine SDK version. Please check if DAML_HOME is incorrectly set, or "
      <> "check the daml project config file, or run \"daml install\" to install the latest "
      <> "version of the SDK." )
    , ( maybe (pure False) (doesDirectoryExist . unwrapSdkPath) envSdkPath
      ,  "The SDK directory does not exist. Please check if DAML_SDK or DAML_SDK_VERSION "
      <> "are incorrectly set, or run \"daml install\" to install the appropriate SDK version.")
    , ( maybe (pure True) (doesDirectoryExist . unwrapProjectPath) envProjectPath
      , "The project directory does not exist. Please check if DAML_PROJECT is incorrectly set.")
    ]

-- | Determine the absolute path to the assistant. Can be overriden with
-- DAML_ASSISTANT env var.
getDamlAssistantPath :: DamlPath -> IO DamlAssistantPath
getDamlAssistantPath damlPath =
    overrideWithEnvVar damlAssistantEnvVar makeAbsolute DamlAssistantPath $
        pure (getDamlAssistantPathDefault damlPath)

-- | Determine the absolute path to the assistant. Note that there is no
-- daml-head on Windows at the moment.
getDamlAssistantPathDefault :: DamlPath -> DamlAssistantPath
getDamlAssistantPathDefault (DamlPath damlPath) =
  let commandNameNoExt = "daml"
      commandName
          | isWindows = commandNameNoExt <.> "cmd"
          | otherwise = commandNameNoExt
      path = damlPath </> "bin" </> commandName
  in DamlAssistantPath path

-- | Determine SDK version of running daml assistant. Can be overriden
-- with DAML_ASSISTANT_VERSION env var.
getDamlAssistantSdkVersion :: IO (Maybe DamlAssistantSdkVersion)
getDamlAssistantSdkVersion =
    overrideWithEnvVarMaybe damlAssistantVersionEnvVar pure
        (fmap DamlAssistantSdkVersion . parseVersion . pack)
        (fmap DamlAssistantSdkVersion <$> tryAssistantM getAssistantSdkVersion)

-- | Determine absolute path of daml home directory.
--
-- On Linux and Mac this is ~/.daml by default.
-- On Windows this is %APPDATA%/daml by default.
--
-- This default can be overriden with the DAML_HOME environment variable,
-- or by running from within an installed daml distribution, as
-- determined by the presence of a "daml-config.yaml" in the ascendants
-- of the executable path.
--
-- Raises an AssistantError if the path is missing.
getDamlPath :: IO DamlPath
getDamlPath = wrapErr "Determining daml home directory." $ do
    overrideWithEnvVar damlPathEnvVar makeAbsolute DamlPath $ do
        maybeM
            (DamlPath <$> getAppUserDataDirectory "daml")
            (pure . DamlPath)
            (findM hasDamlConfig . ascendants =<< getExecutablePath)

    where
        hasDamlConfig :: FilePath -> IO Bool
        hasDamlConfig p = doesFileExist (p </> damlConfigName)

-- | Get the Daml cache folder. This defaults to $XDG_CACHE_HOME/daml.
getCachePath :: IO CachePath
getCachePath =
    wrapErr "Determing daml cache directory." $ do
        overrideWithEnvVar damlCacheEnvVar makeAbsolute CachePath $ do
            errOrcacheDir <- tryIO $ getXdgDirectory XdgCache "daml"
            case errOrcacheDir of
                Left _err -> do
                    -- if getXdgDirectory fails we fall back to the daml path.
                    -- TODO (drsk) This fallback can be removed when we upgrade "directory" to 1.3.6.1.
                    damlPath <- unwrapDamlPath <$> getDamlPath
                    pure $ CachePath damlPath
                Right cacheDir -> pure $ CachePath cacheDir


-- | Calculate the project path. This is done by starting at the current
-- working directory, checking if "daml.yaml" is present. If it is found,
-- that's the project path. Otherwise, go up one level and repeat
-- until you can't go up.
--
-- The project path can be overriden by passing the DAML_PROJECT
-- environment variable.
getProjectPath :: LookForProjectPath -> IO (Maybe ProjectPath)
getProjectPath (LookForProjectPath False) = pure Nothing
getProjectPath (LookForProjectPath True) = wrapErr "Detecting daml project." $ do
        pathM <- overrideWithEnvVarMaybe @SomeException projectPathEnvVar makeAbsolute Right $ do
            cwd <- getCurrentDirectory
            findM hasProjectConfig (ascendants cwd)
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

        sdkVersion <- overrideWithEnvVarMaybe sdkVersionEnvVar pure (parseVersion . pack) $ firstJustM id
            [ maybeM (pure Nothing)
                (tryAssistantM . getSdkVersionFromSdkPath . SdkPath)
                (getEnv sdkPathEnvVar)
            , mapM getSdkVersionFromProjectPath projectPathM
            , tryAssistantM $ getDefaultSdkVersion damlPath
            ]

        sdkPath <- overrideWithEnvVarMaybe @SomeException sdkPathEnvVar makeAbsolute (Right . SdkPath) $
            useInstalledPath damlPath sdkVersion

        return (sdkVersion, sdkPath)

    where
        useInstalledPath :: DamlPath -> Maybe SdkVersion -> IO (Maybe SdkPath)
        useInstalledPath _ Nothing = pure Nothing
        useInstalledPath damlPath (Just sdkVersion) = do
            let sdkPath = defaultSdkPath damlPath sdkVersion
            test <- doesDirectoryExist (unwrapSdkPath sdkPath)
            pure (guard test >> Just sdkPath)

-- | Calculate the environment for dispatched commands (i.e. the environment
-- with updated DAML_HOME, DAML_PROJECT, DAML_SDK, etc).
getDispatchEnv :: Env -> IO [(String, String)]
getDispatchEnv Env{..} = do
    originalEnv <- getEnvironment
    envFreshStableSdkVersionForCheck <- envFreshStableSdkVersionForCheck
    pure $ filter ((`notElem` damlEnvVars) . fst) originalEnv
        ++ [ (damlPathEnvVar, unwrapDamlPath envDamlPath)
           , (damlCacheEnvVar, unwrapCachePath envCachePath)
           , (projectPathEnvVar, maybe "" unwrapProjectPath envProjectPath)
           , (sdkPathEnvVar, maybe "" unwrapSdkPath envSdkPath)
           , (sdkVersionEnvVar, maybe "" versionToString envSdkVersion)
           , (sdkVersionLatestEnvVar, maybe "" versionToString envFreshStableSdkVersionForCheck)
           , (damlAssistantEnvVar, unwrapDamlAssistantPath envDamlAssistantPath)
           , (damlAssistantVersionEnvVar, maybe ""
               (versionToString . unwrapDamlAssistantSdkVersion)
               envDamlAssistantSdkVersion)
           ]
