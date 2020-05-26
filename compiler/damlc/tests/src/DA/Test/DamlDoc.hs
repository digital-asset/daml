-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Test.DamlDoc (main) where

import qualified DA.Daml.Doc.Tests as Damldoc
import qualified DA.Daml.Doc.Render.Tests as Render

import qualified Test.Tasty.Extended as Tasty
import System.Environment.Blank

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    Tasty.deterministicMain =<< allTests

allTests :: IO Tasty.TestTree
allTests = Tasty.testGroup "All DAML GHC tests using Tasty" <$> sequence
  [ Damldoc.mkTestTree
  , Render.mkTestTree
  ]
