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
    , getDamlAssistantPath
    , getProjectPath
    , getSdk
    , getDispatchEnv
    ) where

import DAML.Assistant.Types
import DAML.Assistant.Util
import DAML.Assistant.Install
import DAML.Project.Config
import DAML.Project.Consts hiding (getDamlPath, getProjectPath)
import System.Directory
import System.FilePath
import System.Environment
import System.IO
import Control.Monad.Extra
import Control.Exception.Safe
import Data.Maybe
import Data.Either.Extra
import Safe

-- | Calculate the environment variables in which to run daml-something.
getDamlEnv :: IO Env
getDamlEnv = do
    envDamlPath <- getDamlPath
    envDamlAssistantPath <- getDamlAssistantPath envDamlPath
    envDamlAssistantSdkVersion <- getDamlAssistantSdkVersion
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

-- | Determine the absolute path to the assistant.
getDamlAssistantPath :: Applicative f => DamlPath -> f DamlAssistantPath
getDamlAssistantPath (DamlPath damlPath)
    -- Our packaging logic for Haskell results in getExecutablePath
    -- pointing to the dynamic linker and getProgramName returning "daml" in
    -- both cases so we use this hack to figure out the executable name.
    | takeFileName damlPath == ".daml-head" = pure $ DamlAssistantPath $ damlPath </> "bin" </> "daml-head"
    | otherwise = pure $ DamlAssistantPath $ damlPath </> "bin" </> "daml"

-- | Determine SDK version of running daml assistant.
getDamlAssistantSdkVersion :: IO (Maybe SdkVersion)
getDamlAssistantSdkVersion = do
    exePath <- getExecutablePath
    sdkPathM <- fmap SdkPath <$> findM hasSdkConfig (ascendants exePath)
    case sdkPathM of
        Nothing -> pure Nothing
        Just sdkPath -> do
            sdkConfigE <- try $ readSdkConfig sdkPath
            pure $ eitherToMaybe (sdkVersionFromSdkConfig =<< sdkConfigE)
    where
        hasSdkConfig :: FilePath -> IO Bool
        hasSdkConfig p = doesFileExist (p </> sdkConfigName)


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
    path <- required "Failed to determine daml path." =<< firstJustM id
        [ lookupEnv damlPathEnvVar
        , findM hasDamlConfig . ascendants =<< getExecutablePath
        , Just <$> getAppUserDataDirectory "daml"
        ]
    pure (DamlPath path)

    where
        hasDamlConfig :: FilePath -> IO Bool
        hasDamlConfig p = doesFileExist (p </> damlConfigName)

-- | Calculate the project path. This is done by starting at the current
-- working directory, checking if "daml.yaml" is present. If it is found,
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
            [ lookupEnv sdkVersionEnvVar >>= \ vstrM -> pure $ do
                vstr <- vstrM
                eitherToMaybe (parseVersion (pack vstr))

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
            , useInstalledPath damlPath sdkVersion
            , autoInstall damlPath sdkVersion
            ]

        return (sdkVersion, sdkPath)

    where
        fromConfig :: Text
                   -> IO (Maybe path)
                   -> (path -> IO config)
                   -> (config -> Either ConfigError (Maybe SdkVersion))
                   -> IO (Maybe SdkVersion)
        fromConfig name lookupPath readConfig parseVersion =
            wrapErr ("Determining SDK version from " <> name <> " config.") $ do
                pathM <- lookupPath
                fmap join . forM pathM $ \path -> do
                    config <- readConfig path
                    fromRightM throwIO (parseVersion config)

        useInstalledPath :: DamlPath -> Maybe SdkVersion -> IO (Maybe SdkPath)
        useInstalledPath _ Nothing = pure Nothing
        useInstalledPath damlPath (Just sdkVersion) = do
            let sdkPath = defaultSdkPath damlPath sdkVersion
            test <- doesDirectoryExist (unwrapSdkPath sdkPath)
            pure (guard test >> Just sdkPath)

-- | Determine the latest installed version of the SDK.
getLatestInstalledSdkVersion :: DamlPath -> IO (Maybe SdkVersion)
getLatestInstalledSdkVersion (DamlPath path) = do
    let dpath = path </> "sdk"
    wrapErr "Determining latest installed sdk version." $ do
        dirlistE <- tryIO $ listDirectory dpath
        case dirlistE of
            Left _ -> pure Nothing
            Right dirlist -> do
                subdirs <- filterM (doesDirectoryExist . (dpath </>)) dirlist
                let versions = mapMaybe (eitherToMaybe . parseVersion . pack) subdirs
                pure $ maximumMay (filter isStableVersion versions)

-- | Calculate the environment for dispatched commands (i.e. the environment
-- with updated DAML_HOME, DAML_PROJECT, DAML_SDK, etc).
getDispatchEnv :: Env -> IO [(String, String)]
getDispatchEnv Env{..} = do
    originalEnv <- getEnvironment
    pure $ [ (damlPathEnvVar, unwrapDamlPath envDamlPath)
           , (projectPathEnvVar, maybe "" unwrapProjectPath envProjectPath)
           , (sdkPathEnvVar, maybe "" unwrapSdkPath envSdkPath)
           , (sdkVersionEnvVar, maybe "" versionToString envSdkVersion)
           , (damlAssistantEnvVar, unwrapDamlAssistantPath envDamlAssistantPath)
           , (damlAssistantVersionEnvVar, maybe "" versionToString envDamlAssistantSdkVersion)
           ] ++ filter ((`notElem` damlEnvVars) . fst) originalEnv


-- | Auto-installs requested version if it is missing and updates daml-assistant
-- if it is the latest stable version.
autoInstall :: DamlPath -> Maybe SdkVersion -> IO (Maybe SdkPath)
autoInstall damlPath sdkVersionM = do
    damlConfigE <- try $ readDamlConfig damlPath
    let doAutoInstallE = queryDamlConfigRequired ["auto-install"] =<< damlConfigE
        doAutoInstall = fromRight True doAutoInstallE
    whenMaybe (doAutoInstall && isJust sdkVersionM) $ do
        let sdkVersion = fromJust sdkVersionM
        -- sdk is missing, so let's install it!
        -- first, determine if it is the latest stable version
        latestVersionE :: Either AssistantError SdkVersion
            <- try getLatestVersion
        let isLatest = latestVersionE == Right sdkVersion
            options = InstallOptions
                { iTargetM = Nothing
                , iQuiet = QuietInstall False
                , iActivate = ActivateInstall isLatest
                , iForce = ForceInstall False
                }
            env = InstallEnv
                { options = options
                , damlPath = damlPath
                , targetVersionM = Just sdkVersion
                , projectPathM = Nothing
                , output = hPutStrLn stderr
                    -- Print install messages to stderr since the install
                    -- is only happening because of some other command,
                    -- and we don't want to mess up the other command's
                    -- output / have the install messages be gobbled
                    -- up by a pipe.
                }
        versionInstall env sdkVersion
        pure (defaultSdkPath damlPath sdkVersion)

