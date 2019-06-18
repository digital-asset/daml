-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module VisualTest
   ( main
   ) where

import Test.Tasty
import Test.Tasty.HUnit
import System.Directory
import System.IO.Extra
import DA.Cli.Visual


main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [
    testCase "file exists" (doesFileExist "daml-foundations/daml-tools/da-hs-daml-cli/visual-test-daml.dar" @? "missing dar file to process") ,
    testCase "file exists" (doesFileExist "daml-foundations/daml-tools/da-hs-daml-cli/tests/res/out.dot" @? "missing the expected dot file to check") ,
    testCase "dot file check" $ do
        withTempFile $ \path -> do
            let darPath = "daml-foundations/daml-tools/da-hs-daml-cli/visual-test-daml.dar"
            _ <- execVisual darPath (Just path)
            shouldThrow path "daml-foundations/daml-tools/da-hs-daml-cli/tests/res/out.dot"
  ]


shouldThrow :: FilePath -> FilePath -> IO ()
shouldThrow a b = do
    ac <-  readFile a
    bc <- readFile b
    case ac == bc of
        True -> pure ()
        False -> assertFailure "Expected program to throw an IOException."
