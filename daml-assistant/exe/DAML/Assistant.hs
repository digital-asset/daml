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
import DAML.Assistant.Install
import DAML.Assistant.Util
import System.FilePath
import System.Directory
import System.Process
import System.Exit
import System.IO
import Control.Exception.Safe
import Data.Maybe
import Control.Monad.Extra

-- | Run the assistant and exit.
main :: IO ()
main = displayErrors $ do
    env@Env{..} <- getDamlEnv
    sdkConfigM <- mapM readSdkConfig envSdkPath
    sdkCommandsM <- mapM (fromRightM throwIO . listSdkCommands) sdkConfigM
    userCommand <- getCommand (fromMaybe [] sdkCommandsM)

    whenJust envLatestStableSdkVersion $ \latestVersion -> do
        let isHead = maybe False isHeadVersion envSdkVersion
            test1 = not isHead && isJust envProjectPath && envSdkVersion < Just latestVersion
            test2 = not isHead && not test1 && isJust envDamlAssistantSdkVersion &&
                fmap unwrapDamlAssistantSdkVersion envDamlAssistantSdkVersion <
                Just latestVersion

        -- Project SDK version is outdated.
        when test1 $ do
            hPutStrLn stderr . unlines $
                [ "WARNING: Using an outdated version of the DAML SDK in project."
                , ""
                , "Please set the sdk-version in the project config daml.yaml"
                , "to the latest stable version " <> versionToString latestVersion
                    <> " like this:"
                , ""
                , "sdk-version: " <> versionToString latestVersion
                , ""
                ]

        -- DAML assistant is outdated.
        when test2 $ do
            hPutStrLn stderr . unlines $
                [ "WARNING: Using an outdated version of the DAML assistant."
                , ""
                , "Please upgrade to the latest stable version by running:"
                , ""
                , "daml install latest --activate --force"
                , ""
                ]



    case userCommand of

        Builtin Version -> do
            putStr . unlines $
              [ "SDK version: "
              <> maybe "unknown" versionToString envSdkVersion
              , "Latest stable release: "
              <> maybe "unknown" versionToString envLatestStableSdkVersion
              , "Assistant version: "
              <> maybe "unknown" (versionToString . unwrapDamlAssistantSdkVersion)
                 envDamlAssistantSdkVersion
              ]

        Builtin (Install options) -> wrapErr "Installing the SDK." $ do
            install options envDamlPath envProjectPath

        Builtin (Exec cmd args) ->
            wrapErr "Running executable in daml environment." $ do
                path <- fromMaybe cmd <$> findExecutable cmd
                exitWith =<< dispatch env path args

        Dispatch SdkCommandInfo{..} cmdArgs ->
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
