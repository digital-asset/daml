-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module JsonTestRunner (main) where

import Control.Concurrent
import DA.Bazel.Runfiles
import DA.Daml.Helper.Run
import DA.PortFile
import Data.Aeson
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import qualified Data.Text.Extended as T
import System.FilePath
import System.IO.Extra
import System.Process
import qualified Web.JWT as JWT

main :: IO ()
main = do
    sandbox <- locateRunfiles (mainWorkspace </> "ledger" </> "sandbox" </> exe "sandbox-binary")
    jsonApi <- locateRunfiles (mainWorkspace </> "ledger-service" </> "http-json" </> exe "http-json-binary")
    logback <- locateRunfiles (mainWorkspace </> "ledger-service" </> "http-json" </> "release" </> "json-api-logback.xml")
    testClient <- locateRunfiles (mainWorkspace </> "daml-script" </> "test" </> exe "test_client_json")
    dar <- locateRunfiles (mainWorkspace </> "daml-script" </> "test" </> "script-test.dar")
    withTempFile $ \tempFile -> do
        withCreateProcess (proc sandbox [dar, "--port=0", "--port-file", tempFile, "--wall-clock-time", "--ledgerid=myledger"]) $ \_ _ _ _ -> do
        port <- readPortFile maxRetries tempFile
        withCreateProcess
            (proc jsonApi
                  [ "--ledger-host=localhost"
                  , "--ledger-port=" <> show port
                  , "--http-port=7500"
                  , "--wrapper_script_flag=--jvm_flag=-Dlogback.configurationFile=" <> logback
                  ]) $ \_ _ _ _ -> do
            waitForConnectionOnPort (threadDelay 500000) 7500
            withTempFile $ \tokenFile -> do
                T.writeFileUtf8 tokenFile token
                callProcess testClient [dar, "--access-token-file=" <> tokenFile]

token :: T.Text
token =
  JWT.encodeSigned
    (JWT.HMACSecret "secret")
    mempty
    mempty
      { JWT.unregisteredClaims =
          JWT.ClaimsMap $
          Map.fromList
            [ ( "https://daml.com/ledger-api"
              , Object $
                HashMap.fromList
                  [ ("actAs", toJSON ["Alice" :: String])
                  , ("ledgerId", "myledger")
                  , ("applicationId", "script tests")
                  ])
            ]
      }

