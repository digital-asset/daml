-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlDesugar (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import DA.Bazel.Runfiles (locateRunfiles, mainWorkspace)
import DA.Daml.Desugar.Tests (mkTestTree)
import System.Environment.Blank (setEnv)
import System.FilePath ((</>))
import Test.Tasty.Extended qualified as Tasty

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  testDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/daml-test-files"
  Tasty.deterministicMain =<< allTests testDir

allTests :: FilePath -> IO Tasty.TestTree
allTests testDir = Tasty.testGroup "All Daml GHC tests using Tasty" <$> sequence
  [ mkTestTree testDir
  ]
