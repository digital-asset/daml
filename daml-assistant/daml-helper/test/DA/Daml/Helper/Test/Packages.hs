-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Test.Packages
  ( main
  ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import DA.Bazel.Runfiles
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
    testDar <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> "test.dar")
    defaultMain $
        testGroup
            "daml packages"
            [ withSandbox defaultSandboxConf {dars = [testDar]} $ \getSandboxPort ->
                  testGroup
                      "list"
                      [ testCase "succeeds listing packages" $ do
                            sandboxPort <- getSandboxPort
                            let ledgerOpts = ["--host=localhost", "--port", show sandboxPort]
                            out <- readProcess damlHelper ("packages" : "list" : ledgerOpts) ""
                            ("(test-1.0.0)" `elem` words out) @?
                                "Missing `test-1.0.0` package in packages list."
                      ]
            , withSandbox defaultSandboxConf {dars = [testDar]} $ \getSandboxPort ->
                  withHttpJson getSandboxPort (defaultHttpJsonConf "Alice") $ \getHttpJson ->
                      testGroup
                          "list"
                          [ testCase "succeeds listing packages against HTTP JSON API" $ do
                                HttpJson {hjPort, hjTokenFile} <- getHttpJson
                                let ledgerOpts =
                                        [ "--host=localhost"
                                        , "--json-api"
                                        , "--port"
                                        , show hjPort
                                        , "--access-token-file"
                                        , hjTokenFile
                                        ]
                                out <- readProcess damlHelper ("packages" : "list" : ledgerOpts) ""
                                ("(test-1.0.0)" `elem` words out) @?
                                    "Missing `test-1.0.0` package in packages list."
                          ]
            ]
