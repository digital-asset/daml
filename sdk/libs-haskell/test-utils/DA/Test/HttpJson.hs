-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Tasty resource for starting the HTTP JSON API
module DA.Test.HttpJson
  ( withHttpJson
  , HttpJson(..)
  , HttpJsonConfig(..)
  , defaultHttpJsonConf
  ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Exception
import DA.Bazel.Runfiles
import DA.PortFile
import Data.Aeson
import qualified Data.Aeson.KeyMap as Aeson
import qualified Data.ByteString as BS
import qualified Data.Map as Map
import Data.Maybe
import qualified Data.Text as T
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import qualified Web.JWT as JWT

data HttpJsonConfig = HttpJsonConfig
  { mbSharedSecret :: Maybe BS.ByteString
  , actor :: T.Text
  }

defaultHttpJsonConf :: T.Text -> HttpJsonConfig
defaultHttpJsonConf actor = HttpJsonConfig {mbSharedSecret = Nothing, actor = actor}

getHttpJsonProc :: IO Int -> FilePath -> IO CreateProcess
getHttpJsonProc getLedgerPort portFile = do
  ledgerPort <- getLedgerPort
  jsonApi <-
    locateRunfiles (mainWorkspace </> "ledger-service" </> "http-json" </> exe "http-json-binary")
  pure $
    proc
      jsonApi
      [ "--http-port=0"
      , "--ledger-host"
      , "localhost"
      , "--ledger-port"
      , show ledgerPort
      , "--port-file"
      , portFile
      , "--allow-insecure-tokens"
      ]

createHttpJson :: Handle -> IO Int -> HttpJsonConfig -> IO HttpJsonResource
createHttpJson httpJsonOutput getLedgerPort HttpJsonConfig {actor, mbSharedSecret} = do
  (tmpDir, rmTmpDir) <- newTempDir
  let portFile = tmpDir </> "http-json.portfile"
  let tokenFile = tmpDir </> "http-json.token"
  writeFileUTF8 tokenFile $ T.unpack token
  httpJsonProc <- getHttpJsonProc getLedgerPort portFile
  mask $ \unmask -> do
    ph@(_,_,_,ph') <- createProcess httpJsonProc {std_out = UseHandle httpJsonOutput}
    let cleanup = cleanupProcess ph >> rmTmpDir
    let waitForStart = do
          port <- readPortFile ph' maxRetries portFile
          pure
            (HttpJsonResource
               { httpJsonProcess = ph
               , httpJsonPort = port
               , httpJsonTokenFile = tokenFile
               , httpJsonDestroy = cleanup
               })
    unmask (waitForStart `onException` cleanup)
  where
    token =
      JWT.encodeSigned
        (JWT.EncodeHMACSecret $ fromMaybe "secret" mbSharedSecret)
        mempty
        mempty
          { JWT.unregisteredClaims =
              JWT.ClaimsMap $
              Map.fromList
                [ ( "https://daml.com/ledger-api"
                  , Object $
                    Aeson.fromList
                      [ ("actAs", toJSON [actor])
                      , ("ledgerId", "MyLedger")
                      , ("applicationId", "foobar")
                      ])
                ]
          }

withHttpJson :: IO Int -> HttpJsonConfig -> (IO HttpJson -> TestTree) -> TestTree
withHttpJson getLedgerPort conf f =
  withResource
    (createHttpJson stdout getLedgerPort conf)
    httpJsonDestroy
    (f . fmap fromHttpJsonResource)

data HttpJsonResource = HttpJsonResource
  { httpJsonProcess :: (Maybe Handle, Maybe Handle, Maybe Handle, ProcessHandle)
  , httpJsonPort :: Int
  , httpJsonTokenFile :: FilePath
  , httpJsonDestroy :: IO ()
  }

data HttpJson = HttpJson
  { hjPort :: Int
  , hjTokenFile :: FilePath
  }

fromHttpJsonResource :: HttpJsonResource -> HttpJson
fromHttpJsonResource HttpJsonResource {httpJsonPort, httpJsonTokenFile} =
  HttpJson {hjPort = httpJsonPort, hjTokenFile = httpJsonTokenFile}
