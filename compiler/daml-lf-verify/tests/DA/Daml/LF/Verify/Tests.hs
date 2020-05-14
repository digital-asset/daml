-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Verify.Tests
    ( mainTest
    ) where

import System.FilePath
import Test.Tasty
import Test.Tasty.HUnit

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify
import DA.Daml.LF.Verify.Solve
import DA.Bazel.Runfiles

mainTest :: IO ()
mainTest = defaultMain $ testGroup "DA.Daml.LF.Verify"
  [ quickstartTests
  , conditionalTests
  ]

quickstartTests :: TestTree
quickstartTests = testGroup "Quickstart"
  [ testCase "Iou_Split" $ do
      quickstartDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/quickstart.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "Iou_Split"
          field = FieldName "amount"
      result <- verify quickstartDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_Split - amount"
        Success result
  , testCase "Iou_Merge" $ do
      quickstartDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/quickstart.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "Iou_Merge"
          field = FieldName "amount"
      result <- verify quickstartDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_Merge - amount"
        Success result
  ]

conditionalTests :: TestTree
conditionalTests = testGroup "Conditionals"
  [ testCase "Success A" $ do
      condDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/conditionals.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "SuccA"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccA - content"
        Success result
  , testCase "Success B" $ do
      condDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/conditionals.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "SuccB"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccB - content"
        Success result
  , testCase "Success C" $ do
      condDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/conditionals.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "SuccC"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccC - content"
        Success result
  , testCase "Success D" $ do
      condDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/conditionals.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "SuccD"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccD - content"
        Success result
  , testCase "Fail A" $ do
      condDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/conditionals.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "FailA"
          field = FieldName "content"
      verify condDar debug tmpl choice tmpl field >>= \case
        Success -> assertFailure "Verification wrongfully passed for FailA - content"
        Unknown -> assertFailure "Verification inconclusive for FailA - content"
        Fail _ -> return ()
  , testCase "Fail B" $ do
      condDar <- locateRunfiles (mainWorkspace </> "compiler/daml-lf-verify/conditionals.dar")
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "FailB"
          field = FieldName "content"
      verify condDar debug tmpl choice tmpl field >>= \case
        Success -> assertFailure "Verification wrongfully passed for FailB - content"
        Unknown -> assertFailure "Verification inconclusive for FailB - content"
        Fail _ -> return ()
  ]

debug :: String -> IO ()
debug _ = return ()
