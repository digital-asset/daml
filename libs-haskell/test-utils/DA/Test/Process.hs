-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Process
  ( ShouldSucceed(..)
  , callProcessSilent
  , callProcessSilentError
  , callProcessForStdout
  , callProcessForStderr
  , callProcessForSuccessfulStderr
  , callCommandSilent
  , callCommandSilentIn
  , callCommandSilentWithEnvIn
  , subprocessEnv
  ) where

import Control.Monad (unless,void)
import System.IO.Extra (hPutStrLn,stderr)
import System.Exit (ExitCode(ExitSuccess),exitFailure)
import System.Process (CreateProcess,proc,shell,readCreateProcessWithExitCode,cwd,env)
import System.Environment.Blank (getEnvironment)
import qualified Data.Set as S

newtype ShouldSucceed = ShouldSucceed Bool

callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args =
  void $ run (ShouldSucceed True) (proc cmd args)

callProcessSilentError :: FilePath -> [String] -> IO ()
callProcessSilentError cmd args =
  void $ run (ShouldSucceed False) (proc cmd args)

callProcessForStdout :: FilePath -> [String] -> IO String
callProcessForStdout cmd args =
  fst <$> run (ShouldSucceed True) (proc cmd args)

callProcessForStderr :: FilePath -> [String] -> IO String
callProcessForStderr cmd args =
  snd <$> run (ShouldSucceed False) (proc cmd args)

callProcessForSuccessfulStderr :: FilePath -> [String] -> IO String
callProcessForSuccessfulStderr cmd args =
  snd <$> run (ShouldSucceed True) (proc cmd args)

callCommandSilent :: String -> IO ()
callCommandSilent cmd =
  void $ run (ShouldSucceed True) (shell cmd)

callCommandSilentIn :: FilePath -> String -> IO ()
callCommandSilentIn path cmd =
  void $ run (ShouldSucceed True) (shell cmd) { cwd = Just path }

callCommandSilentWithEnvIn :: FilePath -> [(String, String)] -> String -> IO ()
callCommandSilentWithEnvIn path envChanges cmd = do
  newEnv <- subprocessEnv envChanges
  void $ run (ShouldSucceed True) (shell cmd) { cwd = Just path, env = Just newEnv }

subprocessEnv :: [(String, String)] -> IO [(String, String)]
subprocessEnv envChanges = do
  oldEnv <- getEnvironment
  let changedVars = S.fromList (map fst envChanges)
  let newEnv = filter (\(v,_) -> v `S.notMember` changedVars) oldEnv ++ envChanges
  pure newEnv

run :: ShouldSucceed -> CreateProcess -> IO (String, String)
run (ShouldSucceed shouldSucceed) cp = do
    (exitCode, out, err) <- readCreateProcessWithExitCode cp ""
    unless (shouldSucceed == (exitCode == ExitSuccess)) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> show cp <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout: ", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure
    return (out, err)
