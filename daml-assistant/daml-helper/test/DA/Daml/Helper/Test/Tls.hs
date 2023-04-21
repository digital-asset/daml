-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Helper.Test.Tls (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import DA.Bazel.Runfiles
import DA.Test.Sandbox
import DA.Test.Util
import Data.List (isInfixOf)
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

-- | Asserts we have no _extra_ parties. Canton always creates a party with your participant name
-- (which `withCantonSandbox`'s default config sets to `MyLedger`)
assertOnlyMyLedger :: String -> Assertion
assertOnlyMyLedger out = do
  assertBool ("Expected 1 party but got " <> show (length ls - 1)) $ length ls == 2
  assertBool "Expected only party to be MyLedger" $ "MyLedger" `isInfixOf` (ls !! 1)
  where
    ls = lines out

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlHelper <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "daml-helper" </> exe "daml-helper")
    certDir <- locateRunfiles (mainWorkspace </> "test-common" </> "test-certificates")
    defaultMain $
        testGroup "TLS"
           [ withCantonSandbox defaultSandboxConf { enableTls = True, mbClientAuth = Just None } $ \getSandboxPort ->
                 testGroup "client-auth: none"
                     [ testCase "succeeds without client cert" $ do
                           p <- getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   ]
                           out <- readProcess damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertOnlyMyLedger out
                     ]
           , withCantonSandbox defaultSandboxConf { enableTls = True, mbClientAuth = Just Optional } $ \getSandboxPort ->
                 testGroup "client-auth: optional"
                     [ testCase "succeeds without client cert" $ do
                           p <- getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   ]
                           out <- readProcess damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertOnlyMyLedger out
                     ]
           , withCantonSandbox defaultSandboxConf { enableTls = True, mbClientAuth = Just Require } $ \getSandboxPort ->
                 testGroup "client-auth: require"
                     [ testCase "fails without client cert" $ do
                           p <- getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   ]
                           (exit, stderr, stdout) <- readProcessWithExitCode damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertInfixOf "Listing parties" stderr
                           -- Sadly we do not seem to get a better error for this.
                           assertInfixOf "GRPCIOTimeout" stdout
                           exit @?= ExitFailure 1
                     , testCase "succeeds with client cert" $ do
                           p <- getSandboxPort
                           let ledgerOpts =
                                   [ "--host=localhost" , "--port", show p
                                   , "--cacrt", certDir </> "ca.crt"
                                   , "--pem", certDir </> "client.pem"
                                   , "--crt", certDir </> "client.crt"
                                   ]
                           out <- readProcess damlHelper
                               ("ledger" : "list-parties" : ledgerOpts)
                               ""
                           assertOnlyMyLedger out
                     ]
           ]
