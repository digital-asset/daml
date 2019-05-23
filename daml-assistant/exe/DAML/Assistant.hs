-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant
    ( main
    ) where

import DAML.Project.Config
import DAML.Assistant.Types
import DAML.Assistant.Env
import DAML.Assistant.Command
import DAML.Assistant.Version
import DAML.Assistant.Install
import DAML.Assistant.Util
import System.FilePath
import System.Directory
import System.Process
import System.Exit
import System.IO
import Control.Exception.Safe
import Data.Maybe
import Data.List.Extra
import Data.Either.Extra
import qualified Data.Text as T
import Control.Monad.Extra

-- | Run the assistant and exit.
main :: IO ()
main = displayErrors $ do
    builtinCommandM <- tryBuiltinCommand
    case builtinCommandM of
        Just builtinCommand -> do
            env <- getDamlEnv
            handleCommand env builtinCommand
        Nothing -> do
            env@Env{..} <- autoInstall =<< getDamlEnv

            -- We already know we can't parse the command without an installed SDK.
            -- So if we can't find it, let the user know. This will happen whenever
            -- auto-install is disabled and the project or environment specify a
            -- missing SDK version.
            when (isNothing envSdkPath) $ do
                let installTarget
                        | Just v <- envSdkVersion = versionToString v
                        | otherwise = "latest"
                hPutStr stderr . unlines $
                    [ "DAML SDK not installed. Cannot run command without SDK."
                    , "To proceed, please install the SDK by running:"
                    , ""
                    , "    daml install " <> installTarget
                    , ""
                    ]
                exitFailure

            versionChecks env
            sdkConfig <- readSdkConfig (fromJust envSdkPath)
            sdkCommands <- fromRightM throwIO (listSdkCommands sdkConfig)
            userCommand <- getCommand sdkCommands
            handleCommand env userCommand

-- | Perform version checks, i.e. warn user if project SDK version or assistant SDK
-- versions are out of date with the latest known release.
versionChecks :: Env -> IO ()
versionChecks Env{..} =
    whenJust envLatestStableSdkVersion $ \latestVersion -> do
        let isHead = maybe False isHeadVersion envSdkVersion
            projectSdkVersionIsOld = isJust envProjectPath && envSdkVersion < Just latestVersion
            assistantVersionIsOld = isJust envDamlAssistantSdkVersion &&
                fmap unwrapDamlAssistantSdkVersion envDamlAssistantSdkVersion <
                Just latestVersion

        -- Project SDK version is outdated.
        when (not isHead && projectSdkVersionIsOld) $ do
            hPutStr stderr . unlines $
                [ "WARNING: Using an outdated version of the DAML SDK in project."
                , "Please set the sdk-version in the project config daml.yaml"
                , "to the latest stable version " <> versionToString latestVersion
                    <> " like this:"
                , ""
                , "    sdk-version: " <> versionToString latestVersion
                , ""
                ]

        -- DAML assistant is outdated.
        when (not isHead && not projectSdkVersionIsOld && assistantVersionIsOld) $ do
            hPutStr stderr . unlines $
                [ "WARNING: Using an outdated version of the DAML assistant."
                , "Please upgrade to the latest stable version by running:"
                , ""
                , "    daml install latest --activate"
                , ""
                ]

-- | Perform auto-install if SDK version is given but SDK path is missing,
-- and auto-installs are not disabled in the $DAML_HOME/daml-config.yaml.
-- Returns the Env updated with the installed SdkPath.
autoInstall :: Env -> IO Env
autoInstall env@Env{..} = do
    damlConfigE <- tryConfig $ readDamlConfig envDamlPath
    let doAutoInstallE = queryDamlConfigRequired ["auto-install"] =<< damlConfigE
        doAutoInstall = fromRight True doAutoInstallE

    if doAutoInstall && isJust envSdkVersion && isNothing envSdkPath then do
        -- sdk is missing, so let's install it!
        let sdkVersion = fromJust envSdkVersion
            isLatest
                | Just (DamlAssistantSdkVersion v) <- envDamlAssistantSdkVersion =
                    sdkVersion > v
                | otherwise =
                    True
            options = InstallOptions
                { iTargetM = Nothing
                , iQuiet = QuietInstall False
                , iActivate = ActivateInstall isLatest
                , iForce = ForceInstall False
                , iSetPath = SetPath True
                }
            installEnv = InstallEnv
                { options = options
                , damlPath = envDamlPath
                , targetVersionM = Just sdkVersion
                , projectPathM = Nothing
                , output = hPutStrLn stderr
                    -- Print install messages to stderr since the install
                    -- is only happening because of some other command,
                    -- and we don't want to mess up the other command's
                    -- output / have the install messages be gobbled
                    -- up by a pipe.
                }
        versionInstall installEnv sdkVersion
        pure env { envSdkPath = Just (defaultSdkPath envDamlPath sdkVersion) }

    else
        pure env

handleCommand :: Env -> Command -> IO ()
handleCommand env@Env{..} = \case

    Builtin Version -> do
        installedVersionsE <- tryAssistant $ getInstalledSdkVersions envDamlPath
        availableVersionsE <- tryAssistant $ getAvailableSdkVersions
        defaultVersionM <- tryAssistantM $ getDefaultSdkVersion envDamlPath

        let asstVersion = unwrapDamlAssistantSdkVersion <$> envDamlAssistantSdkVersion
            envVersions = catMaybes
                [ envSdkVersion
                , envLatestStableSdkVersion
                , asstVersion
                ]

            isInstalled =
                case installedVersionsE of
                    Left _ -> const True
                    Right vs -> (`elem` vs)

            versionAttrs v = catMaybes
                [ "active"
                    <$ guard (Just v == envSdkVersion)
                , "default"
                    <$ guard (Just v == defaultVersionM)
                , "assistant"
                    <$ guard (Just v == asstVersion)
                , "latest release"
                    <$ guard (Just v == envLatestStableSdkVersion)
                , "not installed"
                    <$ guard (not (isInstalled v))
                ]

            versions = nubSort (envVersions ++ fromRight [] installedVersionsE ++ fromRight [] availableVersionsE)
            versionTable = [ (versionToText v, versionAttrs v) | v <- versions ]
            versionWidth = maximum (1 : map (T.length . fst) versionTable)
            versionLines =
                [ T.concat
                    [ "  "
                    , v
                    , T.replicate (versionWidth - T.length v) " "
                    , if null attrs
                        then ""
                        else "  (" <> T.intercalate ", " attrs <> ")"
                    ]
                | (v,attrs) <- versionTable ]

        putStr . unpack $ T.unlines ("DAML SDK versions:" : versionLines)

    Builtin (Install options) -> wrapErr "Installing the SDK." $ do
        install options envDamlPath envProjectPath

    Builtin (Exec cmd args) -> do
        wrapErr "Running executable in daml environment." $ do
            path <- fromMaybe cmd <$> findExecutable cmd
            exitWith =<< dispatch env path args

    Dispatch SdkCommandInfo{..} cmdArgs -> do
        wrapErr ("Running " <> unwrapSdkCommandName sdkCommandName <> " command.") $ do
            sdkPath <- required "Could not determine SDK path." envSdkPath
            let path = unwrapSdkPath sdkPath </> unwrapSdkCommandPath sdkCommandPath
                args = unwrapSdkCommandArgs sdkCommandArgs ++ unwrapUserCommandArgs cmdArgs
            exitWith =<< dispatch env path args

dispatch :: Env -> FilePath -> [String] -> IO ExitCode
dispatch env path args = do
    dispatchEnv <- getDispatchEnv env
    requiredIO "Failed to spawn command subprocess." $
        withCreateProcess (proc path args) { env = Just dispatchEnv }
            (\ _ _ _ -> waitForProcess)

displayErrors :: IO () -> IO ()
displayErrors m = m `catches`
    [ Handler $ \ (e :: AssistantError) -> do
        hPutStrLn stderr (displayException e)
        exitFailure
    , Handler $ \ (e :: ConfigError) -> do
        hPutStrLn stderr (displayException e)
        exitFailure
    ]
