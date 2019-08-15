-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Ledger.Sandbox ( -- Run a sandbox for testing on a dynamically selected port
    SandboxSpec(..),
    Sandbox(..),
    startSandbox,
    shutdownSandbox,
    withSandbox
    ) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import DA.Bazel.Runfiles(exe,locateRunfiles,mainWorkspace)
import DA.Ledger (Port (..))
import GHC.IO.Handle (Handle)
import Safe (readMay)
import System.IO.Extra (withTempFile, withFile, IOMode( WriteMode ))
import System.FilePath((</>))
import System.Process

import DA.Ledger.Trace

data SandboxSpec = SandboxSpec {dar :: String}

data Sandbox = Sandbox { port :: Port, proh :: ProcessHandle }


sandboxProcess :: SandboxSpec -> FilePath -> IO CreateProcess
sandboxProcess SandboxSpec{dar} portFile = do
    binary <- locateRunfiles (mainWorkspace </> exe "ledger/sandbox/sandbox-binary")
    pure $ proc binary [ dar, "--port-file", portFile, "-p", "0"]

startSandboxProcess :: SandboxSpec -> FilePath -> IO (ProcessHandle,Maybe Handle)
startSandboxProcess spec portFile = withDevNull $ \devNull -> do
    processRecord <- sandboxProcess spec portFile
    (_,hOutOpt,_,proh) <-
        createProcess processRecord {
            std_out = UseHandle devNull,
            create_group = True  -- To avoid sending INT to ourself
        }
    pid <- getPid proh
    trace $ "Sandbox process started, pid = " <> show pid
    return (proh,hOutOpt)

shutdownSandboxProcess :: ProcessHandle -> IO ()
shutdownSandboxProcess proh = do
    pidOpt <- getPid proh
    trace $ "Sending INT to sandbox process: " <> show pidOpt
    interruptProcessGroupOf proh
    -- Sandbox takes ages to shutdown. Not going to wait!
    let _ = waitForProcess
    --x <- timeoutError 30 "Sandbox process didn't exit" (waitForProcess proh)
    --trace $ "Sandbox process exited with: " <> show x
    return ()

startSandbox :: SandboxSpec -> IO Sandbox
startSandbox spec = withTempFile $ \portFile -> do
    (proh,_hOpt) <- startSandboxProcess spec portFile
    portNum <- readPortFile maxRetries portFile
    let port = Port portNum
    return Sandbox { port, proh }

shutdownSandbox :: Sandbox -> IO ()
shutdownSandbox Sandbox{proh} = shutdownSandboxProcess proh

withSandbox :: SandboxSpec -> (Sandbox -> IO a) -> IO a
withSandbox spec f =
    bracket (startSandbox spec) -- TODO: too long to backet over? (masks C-c ?)
    shutdownSandbox
    f

retryDelayMillis :: Int
retryDelayMillis = 100

-- Wait up to 60s for the port file to be written to.
maxRetries :: Int
maxRetries = 60 * (1000 `div` retryDelayMillis)

readPortFile :: Int -> FilePath -> IO Int
readPortFile 0 _file = fail "Sandbox port file was not written to in time."

readPortFile n file =
  readMay <$> readFile file >>= \case
    Nothing -> do
      threadDelay (1000 * retryDelayMillis)
      readPortFile (n-1) file
    Just p -> pure p

-- | Getting a dev-null handle in a cross-platform way seems to be somewhat tricky so we instead
-- use a temporary file.
withDevNull :: (Handle -> IO a) -> IO a
withDevNull a = withTempFile $ \f -> withFile f WriteMode a
