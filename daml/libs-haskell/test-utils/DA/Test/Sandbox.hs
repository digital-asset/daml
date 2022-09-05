-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    , makeSignedJwt
    ) where

import Control.Exception
import DA.Bazel.Runfiles
import DA.Daml.Helper.Ledger
import Data.Foldable
import qualified DA.Ledger as L
import DA.PortFile
import qualified Data.Aeson as Aeson
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Vector as Vector
import qualified Data.Maybe as Maybe
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import qualified Web.JWT as JWT

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
    , mbLedgerId = Just "MyLedger"
    }

getSandboxProc :: SandboxConfig -> FilePath -> IO CreateProcess
getSandboxProc SandboxConfig{..} portFile = do
    sandbox <- locateRunfiles (mainWorkspace </> "ledger" </> "sandbox-on-x" </> exe "app")
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
        [ ["run-legacy-cli-config"]
        , [ "--participant=participant-id=sandbox-participant,port=0,port-file=" <> portFile ]
        , tlsArgs
        , Maybe.maybeToList timeArg
        , [ "--client-auth=" <> clientAuthArg auth | Just auth <- [mbClientAuth] ]
        , [ "--auth-jwt-hs256-unsafe=" <> secret | Just secret <- [mbSharedSecret] ]
        , [ "--ledger-id=" <> ledgerId | Just ledgerId <- [mbLedgerId] ]
        , [ "--implicit-party-allocation=true" ]
        ]
  where timeArg = case timeMode of
            WallClock -> Nothing
            Static ->  Just "--static-time"
        clientAuthArg auth = case auth of
            None ->  "none"
            Optional -> "optional"
            Require -> "require"

createSandbox :: FilePath -> Handle -> SandboxConfig -> IO SandboxResource
createSandbox portFile sandboxOutput conf@SandboxConfig{..} = do
    sandboxProc <- getSandboxProc conf portFile
    mask $ \unmask -> do
        ph@(_,_,_,ph') <- createProcess sandboxProc { std_out = UseHandle sandboxOutput }
        let waitForStart = do
                port <- readPortFile ph' maxRetries portFile
                forM_ dars $ \darPath -> do
                    let args = (defaultLedgerArgs Grpc) { port = port, tokM = fmap (\s -> L.Token $ makeSignedJwt s []) mbSharedSecret }
                    runLedgerUploadDar' args (Just darPath)

                pure (SandboxResource ph port)
        unmask (waitForStart `onException` cleanupProcess ph)

withSandbox :: SandboxConfig -> (IO Int -> TestTree) -> TestTree
withSandbox conf f =
    withResource newTempDir snd $ \getTmpDir ->
        let createSandbox' = do
                (tempDir, _) <- getTmpDir
                let portFile = tempDir </> "sandbox-portfile"
                createSandbox portFile stdout conf
        in withResource createSandbox' destroySandbox (f . fmap sandboxPort)

data SandboxResource = SandboxResource
    { sandboxProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
    , sandboxPort :: Int
    }

destroySandbox :: SandboxResource -> IO ()
destroySandbox = cleanupProcess . sandboxProcess

makeSignedJwt :: String -> [String] -> String
makeSignedJwt sharedSecret actAs = do
    let urc =
            JWT.ClaimsMap $
            Map.fromList
                [ ("admin", Aeson.Bool True)
                , ("actAs", Aeson.Array $ Vector.fromList $ map (Aeson.String . T.pack) actAs)
                ]
    let cs = mempty {JWT.unregisteredClaims = urc}
    let key = JWT.hmacSecret $ T.pack sharedSecret
    let text = JWT.encodeSigned key mempty cs
    T.unpack text
