-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module VisualTest
   ( main
   ) where

import Test.Tasty
import System.IO.Extra
import DA.Cli.Visual
import DA.Daml.LF.Reader
import Test.Tasty.Golden
import DA.Bazel.Runfiles
import System.FilePath
import Test.Tasty.HUnit


main :: IO ()
main = defaultMain  =<< unitTests

unitTests :: IO TestTree
unitTests = do
    withTempFile $ \path -> do
        darPath <- locateRunfiles (mainWorkspace </> "compiler/damlc/tests/visual-test-daml.dar")
        dotFile <- locateRunfiles (mainWorkspace </> "compiler/damlc/tests/visual/Basic.dot")
        return $ testGroup "making sure we do not add extra edges" [
            goldenVsFile
                "dot file test"
                dotFile
                path
                (execVisual darPath (Just path))
            , testCase "multiline manifest file test" $
                assertEqual "content over multiple lines"
                    ["Dalfs: stdlib.dalf"]
                    (multiLineContent ["Dalfs: stdlib.da", " lf"])
            , testCase "multiline manifest file test" $
                assertEqual "all content in the same line"
                    ["Main-Dalf:solution.dalf" ,"Dalfs: stdlib.dalf"]
                    (multiLineContent ["Dalfs: stdlib.dalf" , "Main-Dalf:solution.dalf"])
            ]
