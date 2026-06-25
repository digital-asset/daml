-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

module Main (main) where

import Control.Monad (unless, when)
import Data.List.Extra (replace, splitOn, stripInfix)
import Data.Maybe (isJust)
import System.Environment (getArgs)
import System.FilePath ((</>))
import System.Process.Typed (proc, runProcess_, withProcessTerm, unsafeProcessHandle)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)
import System.IO.Extra (withTempDir)
import System.Info.Extra
import System.Process (terminateProcess)

#ifndef mingw32_HOST_OS
import Foreign.C.String (CString, withCString)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (peek, poke)
#endif

import DA.PortFile

#ifndef mingw32_HOST_OS
foreign import ccall unsafe "&environ" p_environ :: Ptr (Ptr CString)
foreign import ccall unsafe "dlsym" c_dlsym :: Ptr () -> CString -> IO (Ptr (Ptr CString))

-- The hermetic glibc build split the `environ`/`__environ` weak alias into two
-- distinct objects, so the executable's copy-relocated `environ` stays NULL
-- while libc keeps the real environment in `__environ`. That breaks env
-- inheritance to spawned children (and getEnvironment). Repoint `environ` at
-- libc's real `__environ`. No-op on a correct glibc (same address) and where
-- the symbol is absent (dlsym -> NULL), e.g. macOS.
fixEnviron :: IO ()
fixEnviron = do
  real <- withCString "__environ" (c_dlsym nullPtr)
  when (real /= nullPtr) $ peek real >>= poke p_environ
#else
fixEnviron :: IO ()
fixEnviron = pure ()
#endif

main :: IO ()
main = do
  fixEnviron
  [clientExe, clientArgs, serverExe, serverArgs, _] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let splitServerArgs = splitArgs serverArgs
  let splitClientArgs = splitArgs clientArgs
  unless (any (isJust . stripInfix "%PORT_FILE%") splitServerArgs) $ do
    hPutStrLn stderr "No server parameters accept a port file."
    exitFailure
  withTempDir $ \tempDir -> do
    let portFile = tempDir </> "portfile"
    let interpolatedServerArgs = map (replace "%PORT_FILE%" portFile) splitServerArgs
    let serverProc = proc serverExe interpolatedServerArgs
    withProcessTerm serverProc $ \ph -> do
      port <- readPortFile (unsafeProcessHandle ph) maxRetries portFile
      let interpolatedClientArgs = map (replace "%PORT%" (show port)) splitClientArgs
      runProcess_ (proc clientExe interpolatedClientArgs)
      -- withProcessTerm kills processes using async exceptions, which doesn't work on windows.
      -- Note use of terminateProcess over stopProcess, as the latter also uses async exceptions
      when isWindows (terminateProcess $ unsafeProcessHandle ph)
