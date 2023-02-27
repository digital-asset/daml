-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Test.DamlDoc (main) where

import DA.Daml.Doc.Driver (ExternalAnchorPath (DefaultExternalAnchorPath), loadExternalAnchors)
import DA.Daml.Doc.Types
import qualified DA.Daml.Doc.Tests as Damldoc
import qualified DA.Daml.Doc.Render.Tests as Render
import qualified Test.Tasty.Extended as Tasty
import System.Environment.Blank

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    anchors <- loadExternalAnchors DefaultExternalAnchorPath
    Tasty.deterministicMain =<< allTests anchors

allTests :: AnchorMap -> IO Tasty.TestTree
allTests externalAnchors = Tasty.testGroup "All Daml GHC tests using Tasty" <$> sequence
  [ Damldoc.mkTestTree externalAnchors
  , Render.mkTestTree externalAnchors
  ]
