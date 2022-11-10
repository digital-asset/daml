-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Test.DamlDoc (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import DA.Daml.Doc.Driver (loadExternalAnchors)
import DA.Daml.Doc.Types
import qualified DA.Daml.Doc.Tests as Damldoc
import qualified DA.Daml.Doc.Render.Tests as Render
import qualified Test.Tasty.Extended as Tasty
import System.Environment.Blank
import System.FilePath
import DA.Bazel.Runfiles

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    externalAnchorsPath <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "daml-base-anchors.json")
    anchors <- loadExternalAnchors (Just externalAnchorsPath)
    Tasty.deterministicMain =<< allTests anchors

allTests :: AnchorMap -> IO Tasty.TestTree
allTests externalAnchors = Tasty.testGroup "All Daml GHC tests using Tasty" <$> sequence
  [ Damldoc.mkTestTree externalAnchors
  , Render.mkTestTree externalAnchors
  ]
