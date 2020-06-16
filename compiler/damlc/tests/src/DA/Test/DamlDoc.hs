-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Test.DamlDoc (main) where

import DA.Daml.Doc.Driver (loadExternalAnchors)
import qualified DA.Daml.Doc.Types as Damldoc
import qualified DA.Daml.Doc.Tests as Damldoc
import qualified DA.Daml.Doc.Render.Tests as Render

import qualified Data.Map.Strict as Map
import qualified Test.Tasty.Extended as Tasty
import System.Environment.Blank
import System.FilePath
import System.IO
import System.Exit

import DA.Bazel.Runfiles

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    externalAnchorsPath <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "daml-base-anchors.json")
    externalAnchors <- loadExternalAnchors (Just externalAnchorsPath)
    case externalAnchors of
      Right anchors -> 
        Tasty.deterministicMain =<< allTests anchors
      Left err -> do
        hPutStr stderr err
        exitFailure

allTests :: Map.Map Damldoc.Anchor FilePath -> IO Tasty.TestTree
allTests externalAnchors = Tasty.testGroup "All DAML GHC tests using Tasty" <$> sequence
  [ Damldoc.mkTestTree externalAnchors
  , Render.mkTestTree externalAnchors
  ]
