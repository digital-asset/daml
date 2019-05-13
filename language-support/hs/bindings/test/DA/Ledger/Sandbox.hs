-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Sandbox ( -- Run a sandbox for testing on a dynamically selected port
    SandboxSpec(..),
    Sandbox(port),
    startSandbox,
    shutdownSandbox,
    withSandbox
    ) where

import Control.Monad (when)
import Control.Exception (Exception, bracket, evaluate, onException, throw)
import DA.Ledger (Port (..), unPort)
import Data.List (isInfixOf)
import Data.List.Split (splitOn)
import GHC.IO.Handle (Handle, hGetLine)
import Prelude hiding (log)
import System.IO (hFlush, stdout)
import System.Process (CreateProcess (..), ProcessHandle, StdStream (CreatePipe), createProcess, getPid, interruptProcessGroupOf, proc, waitForProcess)
import System.Time.Extra (Seconds, timeout)

data SandboxSpec = SandboxSpec {dar :: String}

data Sandbox = Sandbox { port :: Port, proh :: ProcessHandle }

selectedPort :: Int
selectedPort = 0 --dynamic port selection

sandboxProcess :: SandboxSpec -> CreateProcess
sandboxProcess SandboxSpec{dar} =
    proc binary [ dar, "--port", show selectedPort]
    where
        binary = "ledger/sandbox/sandbox-binary"

startSandboxProcess :: SandboxSpec -> IO (ProcessHandle,Maybe Handle)
startSandboxProcess spec = do
    (_,hOutOpt,_,proh) <-
        createProcess (sandboxProcess spec) {
        std_out = CreatePipe,
        std_err = CreatePipe, -- Question: ought the pipe to be drained?
        create_group = True  -- To avoid sending INT to ourself
        }
    pid <- getPid proh
    log $ "Sandbox process started, pid = " <> show pid
    return (proh,hOutOpt)

shutdownSandboxProcess :: ProcessHandle -> IO ()
shutdownSandboxProcess proh = do
    pidOpt <- getPid proh
    log $ "Sending INT to sandbox process: " <> show pidOpt
    interruptProcessGroupOf proh
    x <- timeoutError 10 "Sandbox process didn't exit" (waitForProcess proh)
    log $ "Sandbox process exited with: " <> show x
    return ()

parsePortFromListeningLine :: String -> IO Port
parsePortFromListeningLine line = do
    [_,portNumStr] <- return (splitOn ":" line)
    num <- evaluate (read portNumStr)
    return (Port num)

interestingLineFromSandbox :: String -> Bool
interestingLineFromSandbox line =
    any (`isInfixOf` line)
    [--"listening",
     "error", "Address already in use", "java.net.BindException"]

getListeningLine :: Handle -> IO String
getListeningLine h = loop where
    loop = do
        line <- hGetLine h
        when (interestingLineFromSandbox line) $ log $ "SANDBOX: " <> line
        if "listening" `isInfixOf` line
            then return line
            else if "initialization error" `isInfixOf` line
                 then error line
                 else loop

discoverListeningPort :: Maybe Handle -> IO Port
discoverListeningPort hOpt = do
    Just h <- return hOpt
    log "Looking for sandbox listening port..."
    line <- getListeningLine h
    port <- parsePortFromListeningLine line
        `onException` log ("Failed to parse listening port from: " <> show line)
    log $ "Sandbox listening on port: " <> show (unPort port)
    return port

startSandbox :: SandboxSpec-> IO Sandbox
startSandbox spec = do
    (proh,hOpt) <-startSandboxProcess spec
    port <-
        timeoutError 10 "Didn't discover sandbox port" (discoverListeningPort hOpt)
        `onException` shutdownSandboxProcess proh
    return Sandbox { port, proh }

shutdownSandbox :: Sandbox -> IO ()
shutdownSandbox Sandbox{proh} = do shutdownSandboxProcess proh

withSandbox :: SandboxSpec -> (Sandbox -> IO a) -> IO a
withSandbox spec f =
    bracket (startSandbox spec)
    shutdownSandbox
    f

data Timeout = Timeout String deriving Show
instance Exception Timeout

timeoutError :: Seconds -> String -> IO a -> IO a
timeoutError n tag io =
    timeout n io >>= \case
        Just x -> return x
        Nothing -> do
            log $ "Timeout: " <> tag <> ", after " <> show n <> " seconds."
            throw (Timeout tag)

_log,log :: String -> IO ()
_log s = do putStr ("\n["<>s<>"]"); hFlush stdout -- debugging
log _ = return ()
