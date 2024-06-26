-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Test.Ledger
  ( main
  ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import qualified "zip-archive" Codec.Archive.Zip as Zip
import DA.Bazel.Runfiles
import DA.Daml.Dar.Reader
import qualified DA.Daml.LF.Ast.Base as LF
import DA.Ledger.Services.PartyManagementService (PartyDetails(..))
import DA.Ledger.Types (Party(..))
import DA.Test.HttpJson
import DA.Test.Sandbox
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

readDarMainPackageId :: FilePath -> IO String
readDarMainPackageId dar = do
  archive <- Zip.toArchive <$> BSL.readFile dar
  InspectInfo {mainPackageId} <- either fail pure $ collectInfo archive
  pure $ T.unpack $ LF.unPackageId mainPackageId

-- | Check if the list-parties output contains a party like the one given
-- Note the special behaviour needed to handle the `::identifier` on party names
outputContainsParty :: String -> PartyDetails -> Assertion
outputContainsParty out self = any (=~ pat) (lines out) @?
    (TL.unpack (displayName self) <> " is not contained in list-parties output.")
  where
    pat = mconcat
      [ "^PartyDetails {party = '"
      , unParty (party self)
      , "::[a-f0-9]+', displayName = \""
      , displayName self
      , "\", isLocal = "
      , if isLocal self then "True" else "False"
      , "}$"
      ]

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  damlHelper <-
    locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
  testDar <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> "test.dar")
  testDar2 <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> "test2.dar")
  testDarUpgradeV1 <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> "upgrade-test-v1.dar")
  testDarUpgradeV2 <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> "upgrade-test-v2.dar")
  defaultMain $
    withCantonSandbox defaultSandboxConf{previewVersionSupport=True} $ \getSandboxPort ->
    withHttpJson getSandboxPort (defaultHttpJsonConf "Alice") $ \getHttpJson ->
    testGroup
      "daml ledger"
      [ testGroup "list-parties"
          [ testCase "succeeds against HTTP JSON API" $ do
              HttpJson {hjPort, hjTokenFile} <- getHttpJson
              sandboxPort <- getSandboxPort
              -- allocate parties via gRPC
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "allocate-party"
                  , "--host"
                  , "localhost"
                  , "--port"
                  , show sandboxPort
                  , "--timeout=120"
                  , "Bob"
                  ]
              -- check for parties via json api
              let ledgerOpts =
                    [ "--host=localhost"
                    , "--json-api"
                    , "--port"
                    , show hjPort
                    , "--access-token-file"
                    , hjTokenFile
                    ]
              out <- readProcess damlHelper ("ledger" : "list-parties" : ledgerOpts) ""
              out `outputContainsParty` PartyDetails (Party "Bob") "Bob" True
          ]
      , testGroup "allocate-parties"
          [ testCase "succeeds against HTTP JSON API" $ do
              HttpJson {hjPort, hjTokenFile} <- getHttpJson
              sandboxPort <- getSandboxPort
              -- allocate parties via json api
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "allocate-parties"
                  , "--host=localhost"
                  , "--json-api"
                  , "--port"
                  , show hjPort
                  , "--access-token-file"
                  , hjTokenFile
                  , "Bob"
                  , "Charlie"
                  ]
              -- check for parties via gRPC
              let ledgerOpts = ["--host=localhost", "--port", show sandboxPort]
              out <- readProcess damlHelper ("ledger" : "list-parties" : ledgerOpts) ""
              out `outputContainsParty` PartyDetails (Party "Bob") "Bob" True
              out `outputContainsParty` PartyDetails (Party "Charlie") "Charlie" True
          ]
      , testGroup "upload-dar"
          [ testCase "succeeds against HTTP JSON API" $ do
              HttpJson {hjPort, hjTokenFile} <- getHttpJson
              sandboxPort <- getSandboxPort
              testDarPkgId <- readDarMainPackageId testDar
              -- upload-dar via json-api
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "upload-dar"
                  , "--host=localhost"
                  , "--json-api"
                  , "--port"
                  , show hjPort
                  , "--access-token-file"
                  , hjTokenFile
                  , testDar
                  ]
              -- fetch dar via gRPC
              withTempFile $ \tmp -> do
                callCommand $
                  unwords
                    [ damlHelper
                    , "ledger"
                    , "fetch-dar"
                    , "--host=localhost"
                    , "--port"
                    , show sandboxPort
                    , "--main-package-id"
                    , testDarPkgId
                    , "-o"
                    , tmp
                    ]
                fetchedPkgId <- readDarMainPackageId tmp
                fetchedPkgId == testDarPkgId @? "Fechted dar differs from uploaded dar.",
            testCase "dry-run succeeds without uploading" $ do
              sandboxPort <- getSandboxPort
              -- We use a different dar file to avoid conflicts with the 
              -- previous test cases.
              testDar2PkgId <- readDarMainPackageId testDar2
              -- upload-dar via gRPC with --dry-run=true
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "upload-dar"
                  , "--dry-run"
                  , "--host=localhost"
                  , "--port"
                  , show sandboxPort
                  , testDar2
                  ]
              -- fetch dar via gRPC, but too small max-inbound-message-size
              withTempFile $ \tmp -> do
                (exitCode, _, _) <-
                  readCreateProcessWithExitCode
                    (shell $
                     unwords
                       [ damlHelper
                       , "ledger"
                       , "fetch-dar"
                       , "--host=localhost"
                       , "--port"
                       , show sandboxPort
                       , "--main-package-id"
                       , testDar2PkgId
                       , "-o"
                       , tmp
                       ])
                    ""
                exitCode == ExitFailure 1 @? "Fetch after dry-run upload succeeded",
            testCase "dry-run reports validation errors" $ do
              sandboxPort <- getSandboxPort
              -- upload-dar via gRPC
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "upload-dar"
                  , "--host=localhost"
                  , "--port"
                  , show sandboxPort
                  , testDarUpgradeV1
                  ]
              -- upload-dar via gRPC with --dry-run=true
              (exitCode, out, _) <- 
                readCreateProcessWithExitCode
                  (shell $
                    unwords
                      [ damlHelper
                      , "ledger"
                      , "upload-dar"
                      , "--dry-run"
                      , "--host=localhost"
                      , "--port"
                      , show sandboxPort
                      -- This dar contains a package that is a wrong upgrade of the
                      -- package in testDarUpgradeV1. This should fail validation.
                      , testDarUpgradeV2
                      ])
                ""
              exitCode == ExitFailure 1 @? "Dry-run of a wrong update unexpectedely succeeded"
              assertBool "Error message did not contain expected DAR_NOT_VALID_UPGRADE" ("DAR_NOT_VALID_UPGRADE" `L.isInfixOf` out)
              assertBool "Error message did not contain expected reason" $
                "Reason: The upgraded data type T has added new fields, but those fields are not Optional." `L.isInfixOf` out
          ]
      , testGroup "fetch-dar"
          [ testCase "succeeds against HTTP JSON API" $ do
              HttpJson {hjPort, hjTokenFile} <- getHttpJson
              sandboxPort <- getSandboxPort
              testDarPkgId <- readDarMainPackageId testDar
              -- upload-dar via gRPC
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "upload-dar"
                  , "--host=localhost"
                  , "--port"
                  , show sandboxPort
                  , testDar
                  ]
              -- fetch dar via http json
              withTempFile $ \tmp -> do
                callCommand $
                  unwords
                    [ damlHelper
                    , "ledger"
                    , "fetch-dar"
                    , "--json-api"
                    , "--host=localhost"
                    , "--port"
                    , show hjPort
                    , "--access-token-file"
                    , hjTokenFile
                    , "--main-package-id"
                    , testDarPkgId
                    , "-o"
                    , tmp
                    ]
                fetchedPkgId <- readDarMainPackageId tmp
                testDarPkgId == fetchedPkgId @? "Fechted dar differs from uploaded dar."
          ]
      , testGroup "fetch-dar limited gRPC message size"
          [ testCase "fails if the message size is too low" $ do
              sandboxPort <- getSandboxPort
              testDarPkgId <- readDarMainPackageId testDar
              -- upload-dar via gRPC
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "upload-dar"
                  , "--host=localhost"
                  , "--port"
                  , show sandboxPort
                  , testDar
                  ]
              -- fetch dar via gRPC, but too small max-inbound-message-size
              withTempFile $ \tmp -> do
                (exitCode, _, _) <-
                  readCreateProcessWithExitCode
                    (shell $
                     unwords
                       [ damlHelper
                       , "ledger"
                       , "fetch-dar"
                       , "--host=localhost"
                       , "--port"
                       , show sandboxPort
                       , "--main-package-id"
                       , testDarPkgId
                       , "-o"
                       , tmp
                       , "--max-inbound-message-size"
                       , "20"
                       ])
                    ""
                exitCode ==
                  ExitFailure 1 @?
                  "fetch-dar did not fail with too small max-inbound-message-size flag"
                (exitCode2, _, _) <-
                  readCreateProcessWithExitCode
                    (shell $
                     unwords
                       [ damlHelper
                       , "ledger"
                       , "fetch-dar"
                       , "--host=localhost"
                       , "--port"
                       , show sandboxPort
                       , "--main-package-id"
                       , testDarPkgId
                       , "-o"
                       , tmp
                       , "--max-inbound-message-size"
                       , "2000000"
                       ])
                    ""
                exitCode2 ==
                  ExitSuccess @?
                  "fetch-dar did fail with big enough max-inbound-message-size flag"
          ]

      , testGroup "metering-report"
          [ testCase "succeeds against gRPC" $ do
              sandboxPort <- getSandboxPort
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "metering-report"
                  , "--host=localhost"
                  , "--port"
                  , show sandboxPort
                  , "--from=2022-02-10"
                  ]
            , testCase "succeeds against HTTP JSON API" $ do
              HttpJson {hjPort, hjTokenFile} <- getHttpJson
              callCommand $
                unwords
                  [ damlHelper
                  , "ledger"
                  , "metering-report"
                  , "--host=localhost"
                  , "--json-api"
                  , "--port"
                  , show hjPort
                  , "--access-token-file"
                  , hjTokenFile
                  , "--from=2022-02-10"
                  ]
          ]

      ]
