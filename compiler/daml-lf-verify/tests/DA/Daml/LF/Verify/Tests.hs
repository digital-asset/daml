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
  , generalTests
  , conditionalTests
  , recursionTests
  ]

quickstartPath :: String
quickstartPath = "compiler/daml-lf-verify/quickstart.dar"
generalPath :: String
generalPath = "compiler/daml-lf-verify/general.dar"
conditionalsPath :: String
conditionalsPath = "compiler/daml-lf-verify/conditionals.dar"
recursionPath :: String
recursionPath = "compiler/daml-lf-verify/recursion.dar"

quickstartTests :: TestTree
quickstartTests = testGroup "Quickstart"
  [ testCase "Iou_Split" $ do
      quickstartDar <- locateRunfiles (mainWorkspace </> quickstartPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "Iou_Split"
          field = FieldName "amount"
      result <- verify quickstartDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_Split - amount"
        [Success] result
  , testCase "Iou_Merge" $ do
      quickstartDar <- locateRunfiles (mainWorkspace </> quickstartPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "Iou_Merge"
          field = FieldName "amount"
      result <- verify quickstartDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_Merge - amount"
        [Success] result
  ]

generalTests :: TestTree
generalTests = testGroup "General"
  [ testCase "Success A" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "SuccA"
          field = FieldName "content"
      result <- verify genDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccA - content"
        [Success] result
  , testCase "Success B" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "SuccB"
          field = FieldName "content"
      result <- verify genDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccB - content"
        [Success] result
  , testCase "Success C" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "SuccC"
          field = FieldName "content"
      result <- verify genDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccC - content"
        [Success] result
  , testCase "Success D" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "SuccD"
          field = FieldName "content"
      result <- verify genDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccD - content"
        [Success] result
  , testCase "Success E" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "SuccE"
          field = FieldName "content"
      result <- verify genDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccE - content"
        [Success] result
  , testCase "Success F" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "SuccF"
          field = FieldName "content"
      result <- verify genDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccF - content"
        [Success] result
  , testCase "Fail A" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "FailA"
          field = FieldName "content"
      verify genDar debug tmpl choice tmpl field >>= \case
        [Success] -> assertFailure "Verification wrongfully passed for FailA - content"
        [Unknown] -> assertFailure "Verification inconclusive for FailA - content"
        [Fail _] -> return ()
        _ -> assertFailure "Verification produced an incorrect number of outcomes for FailA - content"
  , testCase "Fail B" $ do
      genDar <- locateRunfiles (mainWorkspace </> generalPath)
      let tmpl = TypeConName ["Gen"]
          choice = ChoiceName "FailB"
          field = FieldName "content"
      verify genDar debug tmpl choice tmpl field >>= \case
        [Success] -> assertFailure "Verification wrongfully passed for FailB - content"
        [Unknown] -> assertFailure "Verification inconclusive for FailB - content"
        [Fail _] -> return ()
        _ -> assertFailure "Verification produced an incorrect number of outcomes for FailB - content"
  ]

conditionalTests :: TestTree
conditionalTests = testGroup "Conditionals"
  [ testCase "Success A" $ do
      condDar <- locateRunfiles (mainWorkspace </> conditionalsPath)
      let tmpl = TypeConName ["Cond"]
          choice = ChoiceName "SuccA"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccA - content"
        [Success] result
  , testCase "Success B" $ do
      condDar <- locateRunfiles (mainWorkspace </> conditionalsPath)
      let tmpl = TypeConName ["Cond"]
          choice = ChoiceName "SuccB"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccB - content"
        [Success] result
  , testCase "Success C" $ do
      condDar <- locateRunfiles (mainWorkspace </> conditionalsPath)
      let tmpl = TypeConName ["Cond"]
          choice = ChoiceName "SuccC"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccC - content"
        [Success] result
  , testCase "Success D" $ do
      condDar <- locateRunfiles (mainWorkspace </> conditionalsPath)
      let tmpl = TypeConName ["Cond"]
          choice = ChoiceName "SuccD"
          field = FieldName "content"
      result <- verify condDar debug tmpl choice tmpl field
      assertEqual "Verification failed for SuccD - content"
        [Success] result
  , testCase "Fail A" $ do
      condDar <- locateRunfiles (mainWorkspace </> conditionalsPath)
      let tmpl = TypeConName ["Cond"]
          choice = ChoiceName "FailA"
          field = FieldName "content"
      verify condDar debug tmpl choice tmpl field >>= \case
        [Success] -> assertFailure "Verification wrongfully passed for FailA - content"
        [Unknown] -> assertFailure "Verification inconclusive for FailA - content"
        [Fail _] -> return ()
        _ -> assertFailure "Verification produced an incorrect number of outcomes for FailA - content"
  , testCase "Fail B" $ do
      condDar <- locateRunfiles (mainWorkspace </> conditionalsPath)
      let tmpl = TypeConName ["Cond"]
          choice = ChoiceName "FailB"
          field = FieldName "content"
      verify condDar debug tmpl choice tmpl field >>= \case
        [Success] -> assertFailure "Verification wrongfully passed for FailB - content"
        [Unknown] -> assertFailure "Verification inconclusive for FailB - content"
        [Fail _] -> return ()
        _ -> assertFailure "Verification produced an incorrect number of outcomes for FailB - content"
  ]

recursionTests :: TestTree
recursionTests = testGroup "Recursion"
  [ testCase "Iou_TestRecA" $ do
      recDar <- locateRunfiles (mainWorkspace </> recursionPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "TestRecA"
          field = FieldName "amount"
      result <- verify recDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_TestRecA - amount"
        [Success, Success] result
  , testCase "Iou_TestRecB" $ do
      recDar <- locateRunfiles (mainWorkspace </> recursionPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "TestRecB"
          field = FieldName "amount"
      result <- verify recDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_TestRecB - amount"
        [Success, Success] result
  , testCase "Iou_Divide" $ do
      recDar <- locateRunfiles (mainWorkspace </> recursionPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "Iou_Divide"
          field = FieldName "amount"
      result <- verify recDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_Divide - amount"
        [Success, Success] result
  -- TODO Bug:
  -- , testCase "Iou_TestMutA1" $ do
  --     recDar <- locateRunfiles (mainWorkspace </> recursionPath)
  --     let tmpl = TypeConName ["Iou"]
  --         choice = ChoiceName "TestMutA1"
  --         field = FieldName "amount"
  --     verify recDar debug tmpl choice tmpl field >>= \case
  --       [Fail _, Success] -> return ()
  --       _ -> assertFailure "Verification failed for Iou_TestMutA1 - amount"
  , testCase "Iou_TestMutB1" $ do
      recDar <- locateRunfiles (mainWorkspace </> recursionPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "TestMutB1"
          field = FieldName "amount"
      verify recDar debug tmpl choice tmpl field >>= \case
        [Fail _, Fail _] -> return ()
        _ -> assertFailure "Verification failed for Iou_TestMutB1 - amount"
  , testCase "Iou_Divide_Mut" $ do
      recDar <- locateRunfiles (mainWorkspace </> recursionPath)
      let tmpl = TypeConName ["Iou"]
          choice = ChoiceName "Iou_Divide_Mut"
          field = FieldName "amount"
      result <- verify recDar debug tmpl choice tmpl field
      assertEqual "Verification failed for Iou_Divide_Mut - amount"
        [Success, Success] result
  ]

debug :: String -> IO ()
debug _ = return ()
