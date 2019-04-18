-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant
    ( main
    , runTests
    ) where

import DAML.Project.Config
import DAML.Project.Types
import DAML.Assistant.Env
import DAML.Assistant.Tests
import DAML.Assistant.Command
import DAML.Assistant.Install
import DAML.Assistant.Util
import System.FilePath
import System.Directory
import System.Process
import System.Exit
import Control.Exception.Safe
import Data.Maybe

-- | Run the assistant and exit.
main :: IO ()
main = do
    env@Env{..} <- getDamlEnv
    sdkConfigM <- mapM readSdkConfig envSdkPath
    sdkCommandsM <- mapM (fromRightM throwIO . listSdkCommands) sdkConfigM
    userCommand <- getCommand (fromMaybe [] sdkCommandsM)
    case userCommand of

        Builtin Version -> do
            version <- required "Could not determine SDK version." envSdkVersion
            putStrLn (versionToString version)

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
