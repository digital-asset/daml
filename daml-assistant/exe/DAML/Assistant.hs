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
import System.Environment
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
    rawArgs <- getArgs
    let isInstall = listToMaybe rawArgs == Just "install"
    env@Env{..} <- if isInstall then getMinimalDamlEnv else getDamlEnv
    sdkConfigM <- mapM readSdkConfig envSdkPath
    sdkCommandsM <- mapM (fromRightM throwIO . listSdkCommands) sdkConfigM
    userCommand <- getCommand (fromMaybe [] sdkCommandsM)

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
                , "   sdk-version: " <> versionToString latestVersion
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

    case userCommand of

        Builtin Version -> do
            installedVersionsE <- tryAssistant $ getInstalledSdkVersions envDamlPath
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

                -- | Workaround for Data.SemVer old unfixed bug (see https://github.com/brendanhay/semver/pull/6)
                -- TODO: move away from Data.SemVer...
                versionCompare v1 v2 =
                    if v1 == v2
                        then EQ
                        else compare v1 v2

                versions = nubSortBy versionCompare (envVersions ++ fromRight [] installedVersionsE)
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
