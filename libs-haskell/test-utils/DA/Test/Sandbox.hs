-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Tasty resource for starting sandbox
module DA.Test.Sandbox
    ( SandboxConfig(..)
    , SandboxResource(..)
    , ClientAuth(..)
    , TimeMode(..)
    , defaultSandboxConf
    , withCantonSandbox
    , createCantonSandbox
    , destroySandbox
    , makeSignedJwt
    ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Exception
import Control.Monad (replicateM)
import Control.Monad.Extra (whenMaybe)
import DA.Bazel.Runfiles
import DA.Daml.Helper.Util (decodeCantonPort)
import Data.Foldable
import DA.PortFile
import qualified DA.Test.FreePort as FreePort
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Key as AesonKey
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Vector as Vector
import qualified Data.Maybe as Maybe
import System.Environment (getEnv)
import System.FilePath
import System.IO.Extra
import System.Info.Extra (isWindows)
import System.Process
import Test.Tasty
import qualified Web.JWT as JWT

data ClientAuth
    = None
    | Optional
    | Require

instance Show ClientAuth where
    show None = "none"
    show Optional = "optional"
    show Require = "require"

data TimeMode
    = WallClock
    | Static

data Certs = Certs
    { trustedRootCrt :: FilePath
    , serverCrt :: FilePath
    , serverPem :: FilePath
    , clientCrt :: FilePath
    , clientPem :: FilePath
    }

data SandboxConfig = SandboxConfig
    { enableTls :: Bool
    , dars :: [FilePath]
    , timeMode :: TimeMode
    , mbClientAuth :: Maybe ClientAuth
    , mbSharedSecret :: Maybe String
    , mbLedgerId :: Maybe String
    , devVersionSupport :: Bool
    }

defaultSandboxConf :: SandboxConfig
defaultSandboxConf = SandboxConfig
    { enableTls = False
    , dars = []
    , timeMode = WallClock
    , mbClientAuth = Nothing
    , mbSharedSecret = Nothing
    , mbLedgerId = Just "MyLedger"
    , devVersionSupport = False
    }

getCerts :: IO Certs
getCerts = do
    certDir <- locateRunfiles (mainWorkspace </> "test-common" </> "test-certificates")
    pure Certs
        { trustedRootCrt = certDir </> "ca.crt"
        , serverCrt = certDir </> "server.crt"
        , serverPem = certDir </> "server.pem"
        , clientCrt = certDir </> "client.crt"
        , clientPem = certDir </> "client.pem"
        }

withGeneralSandbox :: (FilePath -> Handle -> SandboxConfig -> IO SandboxResource) -> SandboxConfig -> (IO Int -> TestTree) -> TestTree
withGeneralSandbox create conf f =
    withResource newTempDir snd $ \getTmpDir ->
        let createSandbox' = do
                (tempDir, _) <- getTmpDir
                create tempDir stdout conf
        in withResource createSandbox' destroySandbox (f . fmap sandboxPort)

data SandboxResource = SandboxResource
    { sandboxProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
    , sandboxPort :: Int
    , sandboxPortLocks :: [FreePort.LockedPort]
    }

destroySandbox :: SandboxResource -> IO ()
destroySandbox SandboxResource {..} = do
  cleanupProcess sandboxProcess
  traverse_ FreePort.unlock sandboxPortLocks

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

withCantonSandbox :: SandboxConfig -> (IO Int -> TestTree) -> TestTree
withCantonSandbox = withGeneralSandbox createCantonSandbox

getParticipantName :: SandboxConfig -> String
getParticipantName = Maybe.fromMaybe "participant" . mbLedgerId

createCantonSandbox :: FilePath -> Handle -> SandboxConfig -> IO SandboxResource
createCantonSandbox dir sandboxOutput conf = do
    lockedPorts <- replicateM 5 FreePort.getAvailablePort
    [ledgerPort, adminPort, sequencerPublicPort, sequencerAdminPort, mediatorAdminPort] <- pure $ FreePort.port <$> lockedPorts
    mCerts <- whenMaybe (enableTls conf) getCerts
    let portFile = dir </> "sandbox-portfile"
        configStr = getCantonConfig conf portFile mCerts (ledgerPort, adminPort, sequencerPublicPort, sequencerAdminPort, mediatorAdminPort)
        configPath = dir </> "canton-config.conf"
        bootstrapStr = getCantonBootstrap conf portFile
        bootstrapPath = dir </> "canton-bootstrap.canton"
    BSL.writeFile configPath configStr
    writeFile bootstrapPath bootstrapStr
    cantonSandboxProc <- getCantonSandboxProc configPath bootstrapPath

    mask $ \unmask -> do
        ph@(_,_,_,ph') <- createProcess cantonSandboxProc { std_out = UseHandle sandboxOutput }
        let waitForStart = do
                port <- readPortFileWith (decodeCantonPort $ getParticipantName conf) ph' maxRetries (portFile <> "-bootstrapped")
                pure (SandboxResource ph port lockedPorts)
        unmask (waitForStart `onException` cleanupProcess ph)

-- TODO: replace with https://github.com/digital-asset/docs.daml.com/pull/582/files#diff-26dd6f57464579b580645656eeccdcb20eb953f481003726a013d49a7aa7b3c9R184-R185
getCantonBootstrap :: SandboxConfig -> FilePath -> String
getCantonBootstrap conf portFile = unlines $ domainBootstrap <> (upload <$> dars conf) <> [cpPortFile]
  where
    domainBootstrap =
        [ "val staticDomainParameters = StaticDomainParameters.defaults(sequencer1.config.crypto)"
        , "val domainOwners = Seq(sequencer1, mediator1)"
        , "bootstrap.domain(\"mydomain\", Seq(sequencer1), Seq(mediator1), domainOwners, staticDomainParameters)"
        , getParticipantName conf <> ".domains.connect_local(sequencer1)"
        ]
    upload dar = "participantsX.all.dars.upload(" <> show dar <> ")"
    -- We copy out the port file after bootstrap is finished to get a true setup marker
    -- As the normal portfile is created before the bootstrap command is run
    cpPortFile = "os.copy(os.Path(" <> show portFile <> "), os.Path(" <> show (portFile <> "-bootstrapped") <> "))"

getCantonConfig :: SandboxConfig -> FilePath -> Maybe Certs -> (Int, Int, Int, Int, Int) -> BSL.ByteString
getCantonConfig conf@SandboxConfig{..} portFile mCerts (ledgerPort, adminPort, sequencerPublicPort, sequencerAdminPort, mediatorAdminPort) =
    Aeson.encode $ Aeson.object
        [ "canton" Aeson..= Aeson.object
            [ "parameters" Aeson..= Aeson.object ( concat
                [ [ "ports-file" Aeson..= portFile ]
                , [ "clock" Aeson..= Aeson.object
                        [ "type" Aeson..= ("sim-clock" :: T.Text) ]
                  | Static <- [timeMode] ]
                ] )
            , "participants-x" Aeson..= Aeson.object
                [ (AesonKey.fromString $ getParticipantName conf) Aeson..= Aeson.object
                    (
                     [ storage
                     , "admin-api" Aeson..= port adminPort
                     , "ledger-api" Aeson..= Aeson.object (
                          [ "port" Aeson..= ledgerPort
                          ] <>
                          [ tlsOpts certs
                          | Just certs <- [mCerts]
                          ] <>
                          [ "auth-services" Aeson..= aesonArray [ Aeson.object
                                [ "type" Aeson..= ("unsafe-jwt-hmac-256" :: T.Text)
                                , "secret" Aeson..= secret
                                ] ]
                          | Just secret <- [mbSharedSecret] ]
                          )
                     ] <>
                     [ "testing-time" Aeson..= Aeson.object [ "type" Aeson..= ("monotonic-time" :: T.Text) ]
                     | Static <- [timeMode]
                     ]
                    )
                 ]
            , "sequencers-x" Aeson..= Aeson.object
                [ "sequencer1" Aeson..= Aeson.object
                    [ "sequencer" Aeson..= Aeson.object
                        [ "config" Aeson..= Aeson.object [ storage ]
                        , "type" Aeson..= ("community-reference" :: T.Text)
                        ]
                    , storage
                    , "public-api" Aeson..= port sequencerPublicPort
                    , "admin-api" Aeson..= port sequencerAdminPort
                    ]
                ]
            , "mediators-x" Aeson..= Aeson.object
                [ "mediator1" Aeson..= Aeson.object
                     [ "admin-api" Aeson..= port mediatorAdminPort
                     ]
                ]
            ]
        ]
  where
    storage = "storage" Aeson..= Aeson.object [ "type" Aeson..= ("memory" :: T.Text) ]
    port p = Aeson.object [ "port" Aeson..= p ]
    tlsOpts certs =
        "tls" Aeson..= Aeson.object (
            [ "cert-chain-file" Aeson..= serverCrt certs
            , "private-key-file" Aeson..= serverPem certs
            , "trust-collection-file" Aeson..= trustedRootCrt certs
            ] <>
            [ "client-auth" Aeson..= Aeson.object (["type" Aeson..= show auth] <> adminClient auth certs)
            | Just auth <- [mbClientAuth]
            ] )
    adminClient Require certs =
      [ "admin-client" Aeson..= Aeson.object
        [ "cert-chain-file" Aeson..= clientCrt certs
        , "private-key-file" Aeson..= clientPem certs
        ]
      ]
    adminClient _ _ = []
    aesonArray = Aeson.Array . Vector.fromList

getCantonSandboxProc :: FilePath -> FilePath -> IO CreateProcess
getCantonSandboxProc configPath bootstrapPath = do
    canton <- locateRunfiles $ mainWorkspace </> "canton" </> "community_app_deploy.jar"
    java <- getJava
    pure $ proc java $ concat
      [ ["-jar", canton]
      , ["daemon"]
      , ["-c", configPath]
      , ["--bootstrap", bootstrapPath]
      ]

getJava :: IO FilePath
getJava = do
    javaHome <- getEnv "JAVA_HOME"
    let exe = if isWindows then ".exe" else ""
    pure $ javaHome </> "bin" </> "java" <> exe
