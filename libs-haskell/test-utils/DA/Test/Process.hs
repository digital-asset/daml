-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Process
  ( ShouldSucceed(..)
  , callProcessSilent
  , callProcessSilentError
  , callProcessForStdout
  , callCommandSilent
  ) where

import Control.Monad (unless,void)
import System.IO.Extra (hPutStrLn,stderr)
import System.Exit (ExitCode(ExitSuccess),exitFailure)
import System.Process (CreateProcess,proc,shell,readCreateProcessWithExitCode)

newtype ShouldSucceed = ShouldSucceed Bool

callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args =
  void $ run (ShouldSucceed True) (proc cmd args)

callProcessSilentError :: FilePath -> [String] -> IO ()
callProcessSilentError cmd args =
  void $ run (ShouldSucceed False) (proc cmd args)

callProcessForStdout :: FilePath -> [String] -> IO String
callProcessForStdout cmd args =
  run (ShouldSucceed True) (proc cmd args)

callCommandSilent :: String -> IO ()
callCommandSilent cmd =
  void $ run (ShouldSucceed True) (shell cmd)

run :: ShouldSucceed -> CreateProcess -> IO String
run (ShouldSucceed shouldSucceed) cp = do
    (exitCode, out, err) <- readCreateProcessWithExitCode cp ""
    unless (shouldSucceed == (exitCode == ExitSuccess)) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> show cp <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout: ", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure
    return out
