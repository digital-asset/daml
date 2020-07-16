-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Tasty resource for starting sandbox
module DA.Test.Sandbox
    ( SandboxConfig(..)
    , SandboxResource(..)
    , ClientAuth(..)
    , TimeMode(..)
    , defaultSandboxConf
    , withSandbox
    , createSandbox
    , destroySandbox
    ) where

import Control.Exception
import DA.Bazel.Runfiles
import DA.PortFile
import DA.Test.Util
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty

data ClientAuth
    = None
    | Optional
    | Require

data TimeMode
    = WallClock
    | Static

data SandboxConfig = SandboxConfig
    { enableTls :: Bool
    , dars :: [FilePath]
    , timeMode :: TimeMode
    , mbClientAuth :: Maybe ClientAuth
    , mbSharedSecret :: Maybe String
    , mbLedgerId :: Maybe String
    }

defaultSandboxConf :: SandboxConfig
defaultSandboxConf = SandboxConfig
    { enableTls = False
    , dars = []
    , timeMode = WallClock
    , mbClientAuth = Nothing
    , mbSharedSecret = Nothing
    , mbLedgerId = Nothing
    }

getSandboxProc :: SandboxConfig -> FilePath -> IO CreateProcess
getSandboxProc SandboxConfig{..} portFile = do
    sandbox <- locateRunfiles (mainWorkspace </> "ledger" </> "sandbox" </> exe "sandbox-binary")
    tlsArgs <- if enableTls
        then do
            certDir <- locateRunfiles (mainWorkspace </> "ledger" </> "test-common" </> "test-certificates")
            pure
                [ "--cacrt", certDir </> "ca.crt"
                , "--pem", certDir </> "server.pem"
                , "--crt", certDir </> "server.crt"
                ]
        else pure []
    pure $ proc sandbox $ concat
        [ [ "--port=0", "--port-file", portFile ]
        , tlsArgs
        , [ timeArg ]
        , [ "--client-auth=" <> clientAuthArg auth | Just auth <- [mbClientAuth] ]
        , [ "--auth-jwt-hs256-unsafe=" <> secret | Just secret <- [mbSharedSecret] ]
        , [ "--ledgerid=" <> ledgerId | Just ledgerId <- [mbLedgerId] ]
        , dars
        ]
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
        ph <- createProcess sandboxProc { std_out = UseHandle sandboxOutput }
        let waitForStart = do
                port <- readPortFile maxRetries portFile
                pure (SandboxResource ph port)
        unmask (waitForStart `onException` cleanupProcess ph)

withSandbox :: SandboxConfig -> (IO Int -> TestTree) -> TestTree
withSandbox conf f =
    withResource (openBinaryFile nullDevice ReadWriteMode) hClose $ \getDevNull ->
    withResource newTempFile snd $ \getPortFile ->
        let createSandbox' = do
                (portFile, _) <- getPortFile
                devNull <- getDevNull
                createSandbox portFile devNull conf
        in withResource createSandbox' destroySandbox (f . fmap sandboxPort)


data SandboxResource = SandboxResource
    { sandboxProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
    , sandboxPort :: Int
    }

destroySandbox :: SandboxResource -> IO ()
destroySandbox = cleanupProcess . sandboxProcess
