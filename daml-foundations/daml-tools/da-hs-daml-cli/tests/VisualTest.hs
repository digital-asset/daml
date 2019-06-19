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

main :: IO ()
main = defaultMain  =<< unitTests

unitTests :: IO TestTree
unitTests = do
    withTempFile $ \path -> do
        let darPath = "daml-foundations/daml-tools/da-hs-daml-cli/visual-test-daml.dar"
        return $ testGroup "golden tests" [
            goldenVsFile
                ("dot file test")
                ("daml-foundations/daml-tools/da-hs-daml-cli/tests/res/out.dot")
                (path)
                (execVisual darPath (Just path))
            ]
