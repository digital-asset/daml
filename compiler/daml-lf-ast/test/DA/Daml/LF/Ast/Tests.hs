-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Ast.Tests
    ( main
    ) where

import Data.Foldable
import qualified Data.Map.Strict as Map
import Text.Read
import Test.Tasty
import Test.Tasty.HUnit

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Numeric
import DA.Daml.LF.Ast.Type

main :: IO ()
main = defaultMain $ testGroup "DA.Daml.LF.Ast"
    [ numericTests,
      substitutionTests
    ]

numericExamples :: [(String, Numeric)]
numericExamples =
    [ ("0.", numeric 0 0)
    , ("42.", numeric 0 42)
    , ("-100.", numeric 0 (-100))
    , ("100.00", numeric 2 10000)
    , ("123.45", numeric 2 12345)
    , ("1.0000", numeric 4 10000)
    , ("1.2345", numeric 4 12345)
    , ("0." ++ replicate 32 '0' ++ "54321", numeric 37 54321)
    , ("-9." ++ replicate 37 '9', numeric 37 (1 - 10^(38::Int)))
    ]

numericTests :: TestTree
numericTests = testGroup "Numeric"
    [ testCase "show" $ do
        for_ numericExamples $ \(str,num) -> do
            assertEqual "show produced wrong string" str (show num)
    , testCase "read" $ do
        for_ numericExamples $ \(str,num) -> do
            assertEqual "read produced wrong numeric or failed" (Just num) (readMaybe str)
    ]


substitutionTests :: TestTree
substitutionTests = testGroup "substitution"
   [ testCase "forall" $ do
        assertBool "wrong substitution" $
          (alphaEquiv y substitutionExample)
   ]
   where
     beta1 = TypeVarName "beta11"
     beta2 = TypeVarName "beta1"
     vBeta1 = TVar beta1
     vBeta2 = TVar beta2
     subst1 = Map.insert beta1 (TVar beta2) Map.empty
     substitutionExample =
         TForall (beta1, KStar) $ TForall (beta2, KStar) $  TBuiltin BTArrow `TApp` vBeta1 `TApp` vBeta2
     y = substitute subst1 substitutionExample
