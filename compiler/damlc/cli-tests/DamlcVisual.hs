-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module VisualTest
   ( main
   ) where

import Test.Tasty
import System.IO.Extra
import DA.Cli.Visual
import Test.Tasty.Golden
import DA.Bazel.Runfiles
import System.FilePath

main :: IO ()
main = defaultMain  =<< unitTests

unitTests :: IO TestTree
unitTests = do
    withTempFile $ \path -> do
        darPath <- locateRunfiles (mainWorkspace </> "compiler/damlc/visual-test-daml.dar")
        dotFile <- locateRunfiles (mainWorkspace </> "compiler/damlc/cli-tests/visual/Basic.dot")
        return $ testGroup "making sure we do not add extra edges" [
            goldenVsFile
                "dot file test"
                dotFile
                path
                (execVisual darPath (Just path))
            ]
