-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Test.DamlDoc (main) where

import Control.Monad ((<=<))
import DA.Daml.Doc.Driver (ExternalAnchorPath (DefaultExternalAnchorPath), loadExternalAnchors)
import DA.Daml.Doc.Types
import qualified DA.Daml.Doc.Tests as Damldoc
import qualified DA.Daml.Doc.Render.Tests as Render
import qualified Test.Tasty.Extended as Tasty
import DA.Test.DamlcIntegration (ScriptPackageData, withDamlScriptDep)
import System.Environment.Blank

import SdkVersion (SdkVersioned, withSdkVersions)

main :: IO ()
main = withSdkVersions $ do
  setEnv "TASTY_NUM_THREADS" "1" True
  anchors <- loadExternalAnchors DefaultExternalAnchorPath
  withDamlScriptDep Nothing $ Tasty.deterministicMain <=< allTests anchors

allTests :: SdkVersioned => AnchorMap -> ScriptPackageData -> IO Tasty.TestTree
allTests externalAnchors scriptPackageData = Tasty.testGroup "All Daml GHC tests using Tasty" <$> sequence
  [ Damldoc.mkTestTree externalAnchors scriptPackageData
  , Render.mkTestTree externalAnchors
  ]
