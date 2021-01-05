-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
module DA.Daml.LF.ReplClient
  ( Options(..)
  , ApplicationId(..)
  , MaxInboundMessageSize(..)
  , ReplTimeMode(..)
  , Handle
  , ReplResponseType(..)
  , withReplClient
  , loadPackage
  , runScript
  , clearResults
  , BackendError
  , ClientSSLConfig(..)
  , ClientSSLKeyCertPair(..)
  ) where

import Control.Concurrent
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1
import DA.PortFile
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Network.GRPC.HighLevel.Client (ClientError, ClientRequest(..), ClientResult(..), GRPCMethodType(..))
import Network.GRPC.HighLevel.Generated (withGRPCClient)
import Network.GRPC.LowLevel (ClientConfig(..), ClientSSLConfig(..), ClientSSLKeyCertPair(..), Host(..), Port(..), StatusCode(..))
import qualified Proto3.Suite as Proto
import qualified ReplService as Grpc
import System.Environment
import System.FilePath
import qualified System.IO as IO
import System.IO.Extra (withTempFile)
import System.Process

newtype MaxInboundMessageSize = MaxInboundMessageSize Int
  deriving newtype Read

data ReplTimeMode = ReplWallClock | ReplStatic

data ReplResponseType = ReplText | ReplJson

newtype ApplicationId = ApplicationId String

data Options = Options
  { optServerJar :: FilePath
  , optLedgerConfig :: Maybe (String, String)
  , optMbAuthTokenFile :: Maybe FilePath
  , optMbApplicationId :: Maybe ApplicationId
  , optMbSslConfig :: Maybe ClientSSLConfig
  , optMaxInboundMessageSize :: Maybe MaxInboundMessageSize
  , optTimeMode :: ReplTimeMode
  , optStdout :: StdStream
  -- ^ This is intended for testing so we can redirect stdout there.
  }

data Handle = Handle
  { hClient :: Grpc.ReplService ClientRequest ClientResult
  , hOptions :: Options
  }
data BackendError
  = BErrorClient ClientError
  | BErrorFail StatusCode
  deriving Show

-- | Return the 'CreateProcess' for running java.
-- Uses 'java' from JAVA_HOME if set, otherwise calls java via
-- /usr/bin/env. This is needed when running under "bazel run" where
-- JAVA_HOME is correctly set, but 'java' is not in PATH.
javaProc :: [String] -> IO CreateProcess
javaProc args =
  lookupEnv "JAVA_HOME" >>= return . \case
    Nothing ->
      proc "java" args
    Just javaHome ->
      let javaExe = javaHome </> "bin" </> "java"
      in proc javaExe args

withReplClient :: Options -> (Handle -> Maybe IO.Handle -> ProcessHandle -> IO a) -> IO a
withReplClient opts@Options{..} f = withTempFile $ \portFile -> do
    replServer <- javaProc $ concat
        [ [ "-jar", optServerJar
          , "--port-file", portFile
          ]
        , concat
          [ [ "--ledger-host", host
            , "--ledger-port", port
            ]
          | Just (host, port) <- [optLedgerConfig]
          ]
        , [ "--access-token-file=" <> tokenFile | Just tokenFile <- [optMbAuthTokenFile] ]
        , [ "--application-id=" <> appId | Just (ApplicationId appId)  <- [ optMbApplicationId] ]
        , do Just tlsConf <- [ optMbSslConfig ]
             "--tls" :
                 concat
                     [ [ "--cacrt=" <> rootCert | Just rootCert <- [ serverRootCert tlsConf ] ]
                     , concat
                           [ ["--crt=" <> clientCert, "--pem=" <> clientPrivateKey]
                           | Just ClientSSLKeyCertPair{..} <- [ clientSSLKeyCertPair tlsConf ]
                           ]
                     ]
        , [ case optTimeMode of
                ReplStatic -> "--static-time"
                ReplWallClock -> "--wall-clock-time"
          ]
        ]
    withCreateProcess replServer { std_out = optStdout } $ \_ stdout _ ph -> do
      port <- readPortFile maxRetries portFile
      let grpcConfig = ClientConfig (Host "127.0.0.1") (Port port) [] Nothing Nothing
      threadDelay 1000000
      withGRPCClient grpcConfig $ \client -> do
          replClient <- Grpc.replServiceClient client
          f Handle
              { hClient = replClient
              , hOptions = opts
              } stdout ph

loadPackage :: Handle -> BS.ByteString -> IO (Either BackendError ())
loadPackage Handle{..} package = do
    r <- performRequest
        (Grpc.replServiceLoadPackage hClient)
        (Grpc.LoadPackageRequest package)
    pure (() <$ r)

runScript :: Handle -> LF.Version -> LF.Module -> ReplResponseType -> IO (Either BackendError (Maybe T.Text))
runScript Handle{..} version m rspType = do
    r <- performRequest
        (Grpc.replServiceRunScript hClient)
        (Grpc.RunScriptRequest bytes (TL.pack $ LF.renderMinorVersion (LF.versionMinor version)) grpcRspType)
    pure $ fmap handleResult r
  where
    bytes = BSL.toStrict (Proto.toLazyByteString (EncodeV1.encodeScenarioModule version m))
    handleResult r =
        let t = TL.toStrict (Grpc.runScriptResponseResult r)
        in if T.null t then Nothing else Just t
    grpcRspType = case rspType of
        ReplText -> Proto.Enumerated (Right Grpc.RunScriptRequest_FormatTEXT_ONLY)
        ReplJson -> Proto.Enumerated (Right Grpc.RunScriptRequest_FormatJSON)


clearResults :: Handle -> IO (Either BackendError ())
clearResults Handle{..} = do
    r <- performRequest (Grpc.replServiceClearResults hClient) Grpc.ClearResultsRequest
    pure (() <$ r)

performRequest
  :: (ClientRequest 'Normal payload response -> IO (ClientResult 'Normal response))
  -> payload
  -> IO (Either BackendError response)
performRequest method payload = do
  method (ClientNormalRequest payload 30 mempty) >>= \case
    ClientNormalResponse resp _ _ StatusOk _ -> return (Right resp)
    ClientNormalResponse _ _ _ status _ -> return (Left $ BErrorFail status)
    ClientErrorResponse err -> return (Left $ BErrorClient err)
