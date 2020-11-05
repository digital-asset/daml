-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test.Ledger (main) where

import DA.Bazel.Runfiles
import DA.Ledger.Services.PartyManagementService (PartyDetails(..))
import DA.Ledger.Types (Party(..))
import DA.Test.HttpJson
import DA.Test.Sandbox
import System.Environment.Blank
import System.FilePath
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  damlHelper <-
    locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
  defaultMain $
    testGroup
      "daml ledger"
      [ withSandbox defaultSandboxConf $ \getSandboxPort -> do
          withHttpJson getSandboxPort (defaultHttpJsonConf "Alice") $ \getHttpJson ->
            testGroup
              "list-parties"
              [ testCase "succeeds against HTTP JSON API" $ do
                  HttpJson {hjPort, hjTokenFile} <- getHttpJson
                  sandboxPort <- getSandboxPort
                  callCommand $
                    unwords
                      [ damlHelper
                      , "ledger"
                      , "allocate-party"
                      , "--host"
                      , "localhost"
                      , "--port"
                      , show sandboxPort
                      , "Bob"
                      ]
                  let ledgerOpts =
                        [ "--host=localhost"
                        , "--json-api"
                        , "--port"
                        , show hjPort
                        , "--access-token-file"
                        , hjTokenFile
                        ]
                  out <- readProcess damlHelper ("ledger" : "list-parties" : ledgerOpts) ""
                  ((show $ PartyDetails (Party "Bob") "Bob" True) `elem` lines out) @?
                    "Bob is not contained in list-parties output"
              ]
      ]
