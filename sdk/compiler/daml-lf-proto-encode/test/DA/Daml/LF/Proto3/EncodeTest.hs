-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeTest (
        module DA.Daml.LF.Proto3.EncodeTest
) where


import qualified Data.Text.Lazy                           as TL
import           Data.Vector                              (Vector, (!), singleton, empty)
import           Text.Printf

import           Data.Data

import           DA.Daml.LF.Ast
import           DA.Daml.LF.Proto3.EncodeV2
import           DA.Daml.LF.Proto3.Util

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit                               (Assertion, testCase, assertBool, (@?=))
import           Test.Tasty

import           DA.Daml.LF.Proto3.WellInterned

entry :: IO ()
entry = defaultMain $ testGroup "All tests"
  [ propertyTests
  , unitTests
  ]

-- countConcreteConstrs' :: Data a => a -> ConcreteConstrCount
-- countConcreteConstrs' =
--   let
--     countETE (_ :: EncodeTestEnv) =
--       error "you probably meant to call assertInternedEnv with an EncodeTestEnv (no sensible implementation for EncodeTestEnv exists)"

--   in countETE `extQ` countConcreteConstrs

assertInterned :: Data a => a -> Assertion
assertInterned (countConcreteConstrs -> count) = do
    assertBool (printf "invalid counts: %s" (show count)) $ checkWellInterned count

assertInternedEnv :: EncodeTestEnv -> Assertion
assertInternedEnv EncodeTestEnv{..} = do
  assertBool "Env kinds" $ all wellInterned iKinds
  assertBool "Env types" $ all wellInterned iTypes
  assertBool "Env exprs" $ all wellInterned iExprs

------------------------------------------------------------------------
-- Property tests
------------------------------------------------------------------------

propertyTests :: TestTree
propertyTests = testGroup "Property tests"
  [ propertyCorrectTests
  , deepTests
  ]

{-
Assert the correctness of the property by testing a few examples that should
fail (negative tests). The positive tests are done within the unittests.
-}
propertyCorrectTests :: TestTree
propertyCorrectTests = testGroup "Correctness of property (negative tests)"
  [ propertyCorrectKindArrow
  , propertyCorrectTypeArrow
  , propertyCorrectExprArrow
  , propertyCorrectKindInType
  , propertyCorrectTypeInExpr
  , propertyCorrectKindInExpr
  ]

propertyCorrectKindArrow :: TestTree
propertyCorrectKindArrow =
  testCase "(* -> interned 0)" $
    assertBool "kind (* -> *) should be rejected as not well interned, but is accepted" $
    not (wellInterned $ pkarr pkstar (pkinterned 0))

propertyCorrectTypeArrow :: TestTree
propertyCorrectTypeArrow =
  testCase "(() -> interned 0)" $
    assertBool "type (() -> interned 0) should be rejected as not well interned, but is accepted" $
    not (wellInterned $ ptapp (ptapp ptarr ptunit) (ptinterned 0))

propertyCorrectKindInType :: TestTree
propertyCorrectKindInType =
  testCase "forall 0::*. ()" $
    assertBool "forall 0::*. () should be rejected as not well interned, but is accepted" $
    not (wellInterned $ ptforall 0 pkstar ptunit)

propertyCorrectExprArrow :: TestTree
propertyCorrectExprArrow =
  testCase "true (interned 0)" $
    assertBool "true (interned 0) should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peApp peTrue (peInterned 0))

propertyCorrectTypeInExpr :: TestTree
propertyCorrectTypeInExpr =
  testCase "(interned 0) TUnit" $
    assertBool "(interned 0) TUnit should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peTyApp (peInterned 0) ptunit)

propertyCorrectKindInExpr :: TestTree
propertyCorrectKindInExpr =
  testCase "/\0::*. ()" $
    assertBool "/\0::*. () should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peTyAbs 0 pkstar peUnit)

{-
Assert that interning effectively happens by testing the encoding of "deep"
input, i.e. input with a nesting that _has_ to be interned in order to be flat
enough to be considered well-interned
-}
deepTests :: TestTree
deepTests = testGroup "Deep AST tests"
  [ deepKindArrTest
  , deepTypeArrTest
  , deepLetExprTest
  ]

kindArrOfDepth :: Int -> Kind -> Kind
kindArrOfDepth 0 k = k
kindArrOfDepth i k = KArrow (kindArrOfDepth (i - 1) k) k

deepKindArrTest :: TestTree
deepKindArrTest =
  let level = 100
      (pk, e) = runEncodeKindTest $ kindArrOfDepth level KStar
  in  testCase (printf "deep kind test (%d levels)" level) $ do
      assertInterned pk
      assertInternedEnv e

typeArrOfDepth :: Int -> Type -> Type
typeArrOfDepth 0 t = t
typeArrOfDepth i t = typeArrOfDepth (i - 1) t :-> t

deepTypeArrTest :: TestTree
deepTypeArrTest =
  let level = 100
      (pt, e) = runEncodeTypeTest $ typeArrOfDepth level TUnit
  in  testCase (printf "deep type test (%d levels)" level) $ do
      assertInterned pt
      assertInternedEnv e

letOfDepth :: Int -> Expr
letOfDepth 0 = EUnit
letOfDepth i = elet (letOfDepth $ i -1)
  where
    elet :: Expr -> Expr
    elet = ELet unitBinding

    unitBinding :: Binding
    unitBinding = Binding (ExprVarName "x", TUnit) EUnit

deepLetExprTest :: TestTree
deepLetExprTest =
  let level = 100
      (pe, e) = runEncodeExprTest $ letOfDepth level
  in  testCase (printf "deep let test (%d levels)" level) $ do
      assertInterned pe
      assertInternedEnv e

------------------------------------------------------------------------
-- Unit tests
------------------------------------------------------------------------
unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [ kindTests
  , typeInterningTests
  , exprInterningTests
  ]

-- EncodeTestEnv
data EncodeTestEnv = EncodeTestEnv
    { iStrings :: Vector TL.Text
    , iKinds   :: Vector P.Kind
    , iTypes   :: Vector P.Type
    , iExprs   :: Vector P.Expr
    }

deriving instance Show EncodeTestEnv
deriving instance Data EncodeTestEnv

envToTestEnv :: EncodeState -> EncodeTestEnv
envToTestEnv EncodeState{..} =
  EncodeTestEnv (packInternedStrings internedStrings)
                (packInternedKinds   internedKindsMap)
                (packInternedTypes   internedTypesMap)
                (packInternedExprs   internedExprsMap)

-- Params
testVersion :: Version
testVersion = Version V2 PointDev

runEnc :: Encode a -> (a, EncodeState)
runEnc = runEncode (initTestEncodeConfig testVersion) initEncodeState

-- Kinds
encodeKindAssert :: Kind -> P.Kind -> Assertion
encodeKindAssert k pk =
  let (pk', _) = runEnc (encodeKind k)
  in  pk' @?= pk

encodeKindTest :: String -> Kind -> P.Kind -> TestTree
encodeKindTest str k pk = testCase str $ encodeKindAssert k pk

kindTests :: TestTree
kindTests = testGroup "Kind tests"
  [ kindInterningStarToStar
  , kindInterningStarToNatToStar
  , kindInterningAssertSharing
  ]

runEncodeKindTest :: Kind -> (P.Kind, EncodeTestEnv)
runEncodeKindTest k = envToTestEnv <$> runEnc (encodeKind k)

kindInterningStarToStar :: TestTree
kindInterningStarToStar =
  let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow KStar KStar)
  in  testCase "star to star" $ do
      assertInterned pk
      assertInternedEnv e
      pk @?= pkinterned 1
      iKinds ! 0 @?= pkstar
      iKinds ! 1 @?= pkarr (pkinterned 0) (pkinterned 0)

kindInterningStarToNatToStar :: TestTree
kindInterningStarToNatToStar =
  let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KNat) KStar)
  in  testCase "(star to nat) to star" $ do
      assertInterned pk
      assertInternedEnv e
      pk @?= pkinterned 3
      iKinds ! 0 @?= pkstar
      iKinds ! 1 @?= pknat
      iKinds ! 2 @?= pkarr (pkinterned 0) (pkinterned 1)
      iKinds ! 3 @?= pkarr (pkinterned 2) (pkinterned 0)

kindInterningAssertSharing :: TestTree
kindInterningAssertSharing =
  let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KStar) (KArrow KStar KStar))
  in  testCase "Sharing: (* -> *) -> (* -> *)" $ do
      assertInterned pk
      assertInternedEnv e
      pk @?= pkinterned 2
      iKinds ! 0 @?= pkstar
      iKinds ! 1 @?= pkarr (pkinterned 0) (pkinterned 0)
      iKinds ! 2 @?= pkarr (pkinterned 1) (pkinterned 1)

-- Types
typeInterningTests :: TestTree
typeInterningTests = testGroup "Type tests (interning)"
  [ typeInterningVar
  , typeInterningMyFuncUnit
  , typeInterningMaybeSyn
  , typeInterningUnit
  , typeInterningIntToBool
  , typeInterningForall
  , typeInterningTStruct
  , typeInterningTNat
  , typeInterningAssertSharing
  ]


runEncodeTypeTest :: Type -> (P.Type, EncodeTestEnv)
runEncodeTypeTest k = envToTestEnv <$> runEnc (encodeType' k)

typeInterningVar :: TestTree
typeInterningVar =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ tvar "a"
  in  testCase "tvar a" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 0
      iTypes ! 0 @?= (pliftT $ P.TypeSumVar $ P.Type_Var 0 empty)
      iStrings ! 0 @?= "a"

typeInterningMyFuncUnit :: TestTree
typeInterningMyFuncUnit =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ tmyFuncTest TUnit
  in  testCase "MyFunc ()" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 2
      iTypes ! 0 @?= ptcon 1 empty
      iTypes ! 1 @?= ptunit
      iTypes ! 2 @?= ptapp (ptinterned 0) (ptinterned 1)
      iStrings ! 0 @?= "Main"
      iStrings ! 1 @?= "MyFunc"

typeInterningMaybeSyn :: TestTree
typeInterningMaybeSyn =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ tsynTest "MaybeSyn" [TUnit]
  in  testCase "MaybeSyn ()" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 1
      iTypes ! 0 @?= ptunit
      iTypes ! 1 @?= ptsyn 1 (singleton $ ptinterned 0)
      iStrings ! 0 @?= "Main"
      iStrings ! 1 @?= "MaybeSyn"

typeInterningUnit :: TestTree
typeInterningUnit =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest TUnit
  in  testCase "unit" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 0
      (iTypes ! 0) @?= ptunit

typeInterningIntToBool :: TestTree
typeInterningIntToBool =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TInt64 :-> TBool
  in testCase "Int -> Bool" $ do
    assertInterned pt
    assertInternedEnv e
    pt @?= ptinterned 4
    (iTypes ! 0) @?= ptarr
    (iTypes ! 1) @?= ptint
    (iTypes ! 2) @?= ptapp (ptinterned 0) (ptinterned 1)
    (iTypes ! 3) @?= ptbool
    (iTypes ! 4) @?= ptapp (ptinterned 2) (ptinterned 3)

typeInterningForall :: TestTree
typeInterningForall =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest tyLamTyp
  in  testCase "forall (a : * -> *). a -> a" $ do
    assertInterned pt
    assertInternedEnv e
    pt @?= ptinterned 4
    iStrings ! 0 @?= "a"
    (iKinds ! 0) @?= pkstar
    (iKinds ! 1) @?= pkarr (pkinterned 0) (pkinterned 0)
    (iTypes ! 0) @?= ptarr
    (iTypes ! 1) @?= ptvar 0
    (iTypes ! 2) @?= ptapp (ptinterned 0) (ptinterned 1)
    (iTypes ! 3) @?= ptapp (ptinterned 2) (ptinterned 1)
    (iTypes ! 4) @?= ptforall 0 (pkinterned 1) (ptinterned 3)

typeInterningTStruct :: TestTree
typeInterningTStruct =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TStruct [(FieldName "foo", TUnit)]
  in  testCase "struct {foo :: ()}" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 1
      (iTypes ! 0) @?= ptunit
      (iTypes ! 1) @?= ptstructSingleton 0 (ptinterned 0)
      iStrings ! 0 @?= "foo"

typeInterningTNat :: TestTree
typeInterningTNat =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TNat $ typeLevelNat (16 :: Int)
  in  testCase "tnat 16" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 0
      (iTypes ! 0) @?= (pliftT $ P.TypeSumNat 16)

typeInterningAssertSharing :: TestTree
typeInterningAssertSharing =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TUnit :-> TUnit
  in  testCase "Sharing: () -> ()" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 3
      (iTypes ! 0) @?= ptarr
      (iTypes ! 1) @?= ptunit
      (iTypes ! 2) @?= ptapp (ptinterned 0) (ptinterned 1)
      (iTypes ! 3) @?= ptapp (ptinterned 2) (ptinterned 1)

-- Exprs
exprInterningTests :: TestTree
exprInterningTests = testGroup "Expr tests (interning)"
  [ exprInterningVar
  , exprInterningVal
  , exprInterningBool
  , exprInterningLocLam
  ]

runEncodeExprTest :: Expr -> (P.Expr, EncodeTestEnv)
runEncodeExprTest k = envToTestEnv <$> runEnc (encodeExpr' k)

exprInterningVar :: TestTree
exprInterningVar =
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest $ eVar "x"
  in  testCase "eVar x" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs ! 0 @?= peVar 0
      iStrings ! 0 @?= "x"

exprInterningVal :: TestTree
exprInterningVal =
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest $ eValTest "x"
  in  testCase "eVar x" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs ! 0 @?= peVal 0 1
      iStrings ! 0 @?= "Main"
      iStrings ! 1 @?= "x"

exprInterningBool :: TestTree
exprInterningBool =
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest ETrue
  in  testCase "True" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs ! 0 @?= peTrue

mkIdLocLam :: Expr
mkIdLocLam = ELocation loc1 $ mkETmLams [(x, TUnit)] (ELocation loc2 $ EVar x)
  where
    x = ExprVarName "x"

    loc1, loc2 :: SourceLoc
    loc1 = SourceLoc{..}
      where
        slocModuleRef = Just (SelfPackageId, ModuleName ["loc1"])
        slocStartLine = 1
        slocStartCol = 1
        slocEndLine = 11
        slocEndCol = 11
    loc2 = SourceLoc{..}
      where
        slocModuleRef = Just (SelfPackageId, ModuleName ["loc2"])
        slocStartLine = 2
        slocStartCol = 2
        slocEndLine = 22
        slocEndCol = 22

exprInterningLocLam :: TestTree
exprInterningLocLam =
  let (pe, e) = runEncodeExprTest mkIdLocLam
  in  testCase "IdLocLam" $ do
      assertInterned pe
      assertInternedEnv e
