-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Process
    ( callProcessSilent
    , callProcessForStdout
    , callProcessSilentWithEnv
    ) where

import Control.Monad
import System.Environment
import System.Exit
import System.IO
import System.Process

callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args =
  void $ run (proc cmd args)

callProcessSilentWithEnv :: [(String, String)] -> FilePath -> [String] -> IO ()
callProcessSilentWithEnv additionalEnv cmd args = do
  currentEnv <- getEnvironment
  void $ run (proc cmd args) {env = Just $ additionalEnv <> currentEnv}

callProcessForStdout :: FilePath -> [String] -> IO String
callProcessForStdout cmd args =
  run (proc cmd args)

run :: CreateProcess -> IO String
run cp = do
  (exitCode, out, err) <- readCreateProcessWithExitCode cp ""
  unless (exitCode == ExitSuccess) $ do
    hPutStrLn stderr $ "Failure: Command \"" <> show cp <> "\" exited with " <> show exitCode
    hPutStrLn stderr $ unlines ["stdout: ", out]
    hPutStrLn stderr $ unlines ["stderr: ", err]
    exitFailure
  return out

