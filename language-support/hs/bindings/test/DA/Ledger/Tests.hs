-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger.Tests (main) where

import Control.Exception (SomeException, try)
import qualified DA.Ledger as Ledger
import Data.List (isPrefixOf)
import qualified Data.Text.Lazy as Text (unpack)
import Prelude hiding (log)
import DA.Ledger.Sandbox as Sandbox(SandboxSpec (..), port, shutdownSandbox, withSandbox)
import Test.Tasty as Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit as Tasty (assertBool, assertFailure, testCase)

main :: IO ()
main = Tasty.defaultMain tests

spec1 :: SandboxSpec
spec1 = SandboxSpec {dar}
    where dar = "language-support/hs/bindings/quickstart.dar"

tests :: TestTree
tests = testGroup "Haskell Ledger Bindings" [t1,t2]

t1 :: Tasty.TestTree
t1 = testCase "connect, ledgerid" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        let lid = Ledger.identity h
        let got = Text.unpack $ Ledger.unLedgerId lid
        assertBool "bad ledgerId" (looksLikeSandBoxLedgerId got)

looksLikeSandBoxLedgerId :: String -> Bool
looksLikeSandBoxLedgerId s = "sandbox-" `isPrefixOf` s && length s == 44

t2 :: Tasty.TestTree
t2 = testCase "connect, sandbox dead -> exception" $ do
    withSandbox spec1 $ \sandbox -> do
        shutdownSandbox sandbox -- kill it here
        try (Ledger.connect (Sandbox.port sandbox)) >>= \case
            Left (_::SomeException) -> return ()
            Right _ -> assertFailure "an Exception was expected"
