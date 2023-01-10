-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  , hTerminate
  , hStdout
  , ReplResponseType(..)
  , withReplClient
  , loadPackages
  , runScript
  , clearResults
  , BackendError
  , ClientSSLConfig(..)
  , ClientSSLKeyCertPair(..)
  , ScriptResult(..)
  ) where

import Control.Concurrent.Async
import Control.Concurrent.Extra
import Control.Exception
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1
import DA.PortFile
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Functor
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V
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

newtype MaxInboundMessageSize = MaxInboundMessageSize { getMaxInboundMessageSize :: Int }
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
  { hClient :: IO (Grpc.ReplService ClientRequest ClientResult)
  , hOptions :: Options
  , hStdout :: Maybe IO.Handle
  , hTerminate :: IO ()
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
  lookupEnv "JAVA_HOME" <&> \case
    Nothing ->
      proc "java" args
    Just javaHome ->
      let javaExe = javaHome </> "bin" </> "java"
      in proc javaExe args

withReplClient :: Options -> (Handle -> IO a) -> IO a
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
        , concat [ ["--max-inbound-message-size", show (getMaxInboundMessageSize size)] | Just size <- [optMaxInboundMessageSize] ]
        ]
    withCreateProcess replServer { std_out = optStdout } $ \_ stdout _ ph -> do
      clientBarrier <- newBarrier
      -- Barrier for when we exit the scope of this `withCreateProcess`.
      -- We need that to make the client process stay open until then.
      exitBarrier <- newBarrier
      let handle = Handle
            { hClient = waitBarrier clientBarrier
            , hStdout = stdout
            , hTerminate = terminateProcess ph
            , hOptions = opts
            }
          clientAct = do
            port <- readPortFile ph maxRetries portFile
            let grpcConfig = ClientConfig (Host "127.0.0.1") (Port port) [] Nothing Nothing
            withGRPCClient grpcConfig $ \client -> do
                replClient <- Grpc.replServiceClient client
                signalBarrier clientBarrier replClient
                waitBarrier exitBarrier
      withAsync clientAct $ const $ f handle `finally` signalBarrier exitBarrier ()

loadPackages :: Handle -> [BS.ByteString] -> IO (Either BackendError ())
loadPackages Handle{..} packages = do
    client <- hClient
    r <- performRequest
        (Grpc.replServiceLoadPackages client)
        (Grpc.LoadPackagesRequest (V.fromList packages))
    pure (void r)

data ScriptResult
    = ScriptSuccess (Maybe T.Text) -- ^ Script succeeded, if it was of type Script Text include the result.
    | ScriptError T.Text -- ^ Script failed
    | InternalError T.Text -- ^ The impossible happened

runScript :: Handle -> LF.Version -> LF.Module -> ReplResponseType -> IO (Either BackendError ScriptResult)
runScript Handle{..} version m rspType = do
    client <- hClient
    r <- performRequest
        (Grpc.replServiceRunScript client)
        (Grpc.RunScriptRequest bytes (TL.pack $ LF.renderMinorVersion (LF.versionMinor version)) grpcRspType)
    pure $ fmap handleResult r
  where
    bytes = BSL.toStrict (Proto.toLazyByteString (EncodeV1.encodeScenarioModule version m))
    handleResult r = case Grpc.runScriptResponseResult r of
        Nothing -> InternalError "Script produced neither a result nor an error"
        Just (Grpc.RunScriptResponseResultSuccess (Grpc.ScriptSuccess result)) ->
            ScriptSuccess (if TL.null result then Nothing else Just (TL.toStrict result))
        Just (Grpc.RunScriptResponseResultError (Grpc.ScriptError err)) ->
            ScriptError (TL.toStrict err)
    grpcRspType = case rspType of
        ReplText -> Proto.Enumerated (Right Grpc.RunScriptRequest_FormatTEXT_ONLY)
        ReplJson -> Proto.Enumerated (Right Grpc.RunScriptRequest_FormatJSON)


clearResults :: Handle -> IO (Either BackendError ())
clearResults Handle{..} = do
    client <- hClient
    r <- performRequest (Grpc.replServiceClearResults client) Grpc.ClearResultsRequest
    pure (void r)

performRequest
  :: (ClientRequest 'Normal payload response -> IO (ClientResult 'Normal response))
  -> payload
  -> IO (Either BackendError response)
performRequest method payload = do
  method (ClientNormalRequest payload timeoutSeconds mempty) >>= \case
    ClientNormalResponse resp _ _ StatusOk _ -> return (Right resp)
    ClientNormalResponse _ _ _ status _ -> return (Left $ BErrorFail status)
    ClientErrorResponse err -> return (Left $ BErrorClient err)


timeoutSeconds :: Int
timeoutSeconds = 120
