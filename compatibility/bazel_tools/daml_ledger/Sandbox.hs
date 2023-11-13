-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Sandbox
  ( SandboxConfig (..)
  , SandboxResource (..)
  , ClientAuth (..)
  , TimeMode (..)
  , defaultSandboxConf
  , withSandbox
  , createSandbox
  , destroySandbox
  , readCantonPortFile
  , readPortFile
  , maxRetries
  , nullDevice
  ) where

import Control.Concurrent (threadDelay)
import Control.Exception.Safe (IOException, catchJust, mask, onException)
import Control.Monad
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy.Char8 qualified as BSL8
import Data.Map qualified as Map
import Data.Text (pack)
import Data.Text.IO qualified as T
import Safe (readMay)
import System.Environment (getEnvironment)
import System.Exit (exitFailure)
import System.FilePath ((</>))
import System.IO.Error (isDoesNotExistError, isPermissionError)
import System.IO.Extra (Handle, IOMode (..), hClose, newTempDir, openBinaryFile, stderr)
import System.Info.Extra (isWindows)
import System.Process
import Test.Tasty (TestTree, withResource)

data ClientAuth
    = None
    | Optional
    | Require

data TimeMode
    = WallClock
    | Static

data SandboxConfig = SandboxConfig
    { sandboxBinary :: FilePath
      -- ^ Path to the sandbox executable.
    , sandboxArgs :: [String]
      -- ^ Extra arguments required to run the sandbox.
    , sandboxCertificates :: FilePath
      -- ^ Path to the directory holding the certificates.
      --
      -- Should contain @ca.crt@, @server.pem@, and @server.crt@.
    , enableTls :: Bool
    , dars :: [FilePath]
    , timeMode :: TimeMode
    , mbClientAuth :: Maybe ClientAuth
    , mbSharedSecret :: Maybe String
    , mbLedgerId :: Maybe String
    }

defaultSandboxConf :: SandboxConfig
defaultSandboxConf = SandboxConfig
    { sandboxBinary = "sandbox"
    , sandboxArgs = []
    , sandboxCertificates = ""
    , enableTls = False
    , dars = []
    , timeMode = WallClock
    , mbClientAuth = Nothing
    , mbSharedSecret = Nothing
    , mbLedgerId = Just "MyLedger"
    }

getSandboxProc :: SandboxConfig -> FilePath -> IO CreateProcess
getSandboxProc SandboxConfig{..} portFile = do
    tlsArgs <- if enableTls
        then do
            pure
                [ "--cacrt", sandboxCertificates </> "ca.crt"
                , "--pem", sandboxCertificates </> "server.pem"
                , "--crt", sandboxCertificates </> "server.crt"
                ]
        else pure []
    let args = concat
          [ sandboxArgs
          , [ "--port=0", "--port-file", portFile ]
          , tlsArgs
          , [ timeArg ]
          , [ "--client-auth=" <> clientAuthArg auth | Just auth <- [mbClientAuth] ]
          , [ "--auth-jwt-hs256-unsafe=" <> secret | Just secret <- [mbSharedSecret] ]
          , [ "--ledgerid=" <> ledgerId | Just ledgerId <- [mbLedgerId] ]
          , dars
          ]
    env <- getEnvironment
    pure $ (proc sandboxBinary args)
      -- Reducing memory consumption to allow multiple parallel test executions.
      { env = Just $ ("_JAVA_OPTIONS", "-Xss4m -Xms128m -Xmx1g") : env }
  where timeArg = case timeMode of
            WallClock -> "--wall-clock-time"
            Static -> "--static-time"
        clientAuthArg auth = case auth of
            None ->  "none"
            Optional -> "optional"
            Require -> "require"

createSandbox :: FilePath -> Handle -> SandboxConfig -> IO SandboxResource
createSandbox portFile sandboxOutput conf = do
    sandboxProc <- getSandboxProc conf portFile
    mask $ \unmask -> do
        ph@(_, _, _, handle) <- createProcess sandboxProc { std_out = UseHandle sandboxOutput, create_group = True }
        let waitForStart = do
                port <- readPortFile handle maxRetries portFile
                pure (SandboxResource ph port)
        unmask (waitForStart `onException` cleanupProcess ph)

withSandbox :: IO SandboxConfig -> (IO Int -> TestTree) -> TestTree
withSandbox getConf f =
    withResource (openBinaryFile nullDevice ReadWriteMode) hClose $ \getDevNull ->
    withResource newTempDir snd $ \getTempDir ->
        let createSandbox' = do
                (tempDir, _) <- getTempDir
                let portFile = tempDir </> "sandbox-portfile"
                devNull <- getDevNull
                conf <- getConf
                createSandbox portFile devNull conf
        in withResource createSandbox' destroySandbox (f . fmap sandboxPort)


data SandboxResource = SandboxResource
    { sandboxProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
    , sandboxPort :: Int
    }

destroySandbox :: SandboxResource -> IO ()
destroySandbox SandboxResource{..} = do
    let (_, _, _, ph) = sandboxProcess
    -- This is a shell script so we kill the whole process group.
    interruptProcessGroupOf ph
    cleanupProcess sandboxProcess
    void $ waitForProcess ph

nullDevice :: FilePath
nullDevice
    -- taken from typed-process
    | isWindows = "\\\\.\\NUL"
    | otherwise =  "/dev/null"

readOnce :: (String -> Maybe t) -> FilePath -> IO (Maybe t)
readOnce parseFn file = catchJust
    (guard . shouldCatch)
    (parseFn <$> readFile file)
    (const $ pure Nothing)

readPortFileWith :: (String -> Maybe t) -> ProcessHandle -> Int -> FilePath -> IO t
readPortFileWith _ _ 0 file = do
  T.hPutStrLn stderr ("Port file was not written to '" <> pack file <> "' in time.")
  exitFailure
readPortFileWith parseFn ph n file = do
  result <- readOnce parseFn file
  case result of
    Just p -> pure p
    Nothing -> do
      status <- getProcessExitCode ph
      case status of
        Nothing -> do -- Process still active. Try again.
          threadDelay (1000 * retryDelayMillis)
          readPortFileWith parseFn ph (n-1) file
        Just exitCode -> do -- Process exited already. Try reading one last time, then give up.
          threadDelay (1000 * retryDelayMillis)
          result <- readOnce parseFn file
          case result of
            Just p -> pure p
            Nothing -> do
              T.hPutStrLn stderr ("Port file was not written to '" <> pack file <> "' before process exit with " <> pack (show exitCode))
              exitFailure

decodeCantonPort :: String -> String -> Maybe Int
decodeCantonPort participantName json = do
    participants :: Map.Map String (Map.Map String Int) <- Aeson.decode (BSL8.pack json)
    ports <- Map.lookup participantName participants
    Map.lookup "ledgerApi" ports

decodeCantonSandboxPort :: String -> Maybe Int
decodeCantonSandboxPort = decodeCantonPort "sandbox"

readCantonPortFile :: ProcessHandle -> Int -> FilePath -> IO Int
readCantonPortFile = readPortFileWith decodeCantonSandboxPort

readPortFile :: ProcessHandle -> Int -> String -> IO Int
readPortFile = readPortFileWith readMay

-- On Windows we sometimes get permission errors. It looks like
-- this might come from a race where sandbox is writing the file at the same
-- time we try to open it so catching the exception is the right thing to do.
shouldCatch :: IOException -> Bool
shouldCatch e = isDoesNotExistError e || isPermissionError e

retryDelayMillis :: Int
retryDelayMillis = 50

maxRetries :: Int
maxRetries = 120 * (1000 `div` retryDelayMillis)
