-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeTest (
        module DA.Daml.LF.Proto3.EncodeTest
) where


import           Control.Monad.State.Strict
import qualified Data.Text.Lazy                           as TL
import qualified Data.Vector                              as V
import           Text.Printf

import           Data.Data
import           Data.Generics.Aliases

import           DA.Daml.LF.Ast
import           DA.Daml.LF.Proto3.EncodeV2
import           DA.Daml.LF.Proto3.Util
import           DA.Daml.LF.Proto3.DerivingData           ()

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit                               (Assertion, testCase, assertBool, (@?=))
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests"
  [ propertyTests
  , unitTests
  ]

------------------------------------------------------------------------
-- Property definition
------------------------------------------------------------------------


{-
Assumptions:

- The AST the Com.Digitalasset.Daml.Lf.Archive.DamlLf2 has _strictly more_ nodes
  than the actual protobuff spec, so therefore, the height of the
  Com.Digitalasset.Daml.Lf.Archive.DamlLf2 AST is an underestimation of the
  protobuff spec's height. Therefore, if the
  Com.Digitalasset.Daml.Lf.Archive.DamlLf2 AST is flat enough, it for sure is in
  the protobuff spec.
-}

data ConcreteConstrCount = ConcreteConstrCount
    { kinds :: Int
    , types :: Int
    , exprs :: Int
    }
    deriving Show

instance Semigroup ConcreteConstrCount where
    (ConcreteConstrCount k1 t1 e1) <> (ConcreteConstrCount k2 t2 e2) =
        ConcreteConstrCount (k1 + k2) (t1 + t2) (e1 + e2)

instance Monoid ConcreteConstrCount where
    mempty = ConcreteConstrCount 0 0 0

singleKind, singleType, singleExpr :: ConcreteConstrCount
singleKind = mempty{kinds = 1}
singleType = mempty{types = 1}
singleExpr = mempty{exprs = 1}

maxCount :: ConcreteConstrCount -> ConcreteConstrCount -> ConcreteConstrCount
maxCount (ConcreteConstrCount k1 t1 e1) (ConcreteConstrCount k2 t2 e2) =
    ConcreteConstrCount (max k1 k2) (max t1 t2) (max e1 e2)

{-
Count the number of concrete constructors for kinds, types, and expressions.
This code would be a lot simpler if we would simply return a bool or another
pass/failiure (such as Either), simply asserting at the point of a concrete
constructor that children do not contain concrete constructors. However, giving
the counts is a bit more inforamative for debugging, and this code will change
anyways if and when we allow for trees of size n
-}
countConcreteConstrs :: Data a => a -> ConcreteConstrCount
countConcreteConstrs =
  let
    countKinds (k :: P.KindSum) = case k of
      (P.KindSumInternedKind _) -> mempty
      -- kind-specific ignoring of leafs
      (P.KindSumStar     _) -> mempty
      (P.KindSumNat      _) -> mempty
      _                         -> singleKind <> mconcat (gmapQ countConcreteConstrs k)

    countTypes (t :: P.TypeSum) = case t of
      (P.TypeSumInterned _) -> mempty
      _                     -> singleType <> mconcat (gmapQ countConcreteConstrs t)

    countExprs (e :: P.ExprSum) = case e of
      (P.ExprSumInternedExpr _) -> mempty
      _                         -> singleExpr <> mconcat (gmapQ countConcreteConstrs e)

    countETE (_ :: EncodeTestEnv) =
      error "you probably meant to call assertInternedEnv with an EnodeTestEnv (no sensible implementation for EncodeTestEnv exists)"

    genericCase x = mconcat (gmapQ countConcreteConstrs x)

  in genericCase `extQ` countETE `extQ` countKinds `extQ` countTypes `extQ` countExprs

checkWellInterned :: ConcreteConstrCount -> Bool
checkWellInterned ConcreteConstrCount{..} =
    kinds + types + exprs <= 1

wellInterned :: Data a => a -> Bool
wellInterned = checkWellInterned . countConcreteConstrs

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
  [ propertyCorrectNestedKindArrow
  , propertyCorrectNestedTypeArrow
  , propertyCorrectExprArrow
  , propertyCorrectKindInType
  , propertyCorrectTypeInExpr
  , propertyCorrectKindInExpr
  ]

propertyCorrectNestedKindArrow :: TestTree
propertyCorrectNestedKindArrow =
  testCase "((* -> *) -> *)" $
    assertBool "kind ((* -> *) -> *) should be rejected as not well interned, but is accepted" $
    not (wellInterned $ pkarr (pkarr pkstar pkstar) pkstar)

propertyCorrectNestedTypeArrow :: TestTree
propertyCorrectNestedTypeArrow =
  testCase "(() -> interned 0)" $
    assertBool "type (() -> ()) should be rejected as not well interned, but is accepted" $
    not (wellInterned $ ptarr ptunit (ptinterned 0))

propertyCorrectKindInType :: TestTree
propertyCorrectKindInType =
  testCase "forall 0::(* -> *). ()" $
    assertBool "forall 0::(* -> *). () should be rejected as not well interned, but is accepted" $
    not (wellInterned $ ptforall 0 (pkarr pkstar pkstar) ptunit)

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
  testCase "/\0::(* -> *). ()" $
    assertBool "/\0::(* -> *). () should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peTyAbs 0 (pkarr pkstar pkstar) peUnit)

{-
Assert that interning effectively happens by testing the encoding of "deep"
input, i.e. input with a nesting that _has_ to be interned in order to be flat
enough to be considered well-interned
-}
deepTests :: TestTree
deepTests = testGroup "Deep AST tests"
  [
    -- TODO[RB]: uncomment in PR that reworks kind interning
    -- deepKindArrTest
  -- ,
    deepTypeArrTest
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
    { iStrings :: V.Vector TL.Text
    , iKinds   :: V.Vector P.Kind
    , iTypes   :: V.Vector P.Type
    , iExprs   :: V.Vector P.Expr
    }

deriving instance Show EncodeTestEnv
deriving instance Data EncodeTestEnv

envToTestEnv :: EncodeEnv -> EncodeTestEnv
envToTestEnv EncodeEnv{..} =
  EncodeTestEnv (packInternedStrings internedStrings)
                (packInternedKinds   internedKindsMap)
                (packInternedTypes   internedTypesMap)
                (packInternedExprs   internedExprsMap)

-- Params
testVersion :: Version
testVersion = Version V2 PointDev

-- Kinds
encodeKindAssert :: Kind -> P.Kind -> Assertion
encodeKindAssert k pk =
  let (pk', _) = runState (encodeKind k) env
  in  pk' @?= pk
    where
      env = initEncodeEnv testVersion

encodeKindTest :: String -> Kind -> P.Kind -> TestTree
encodeKindTest str k pk = testCase str $ encodeKindAssert k pk

kindTests :: TestTree
kindTests = testGroup "Kind tests"
  [ kindPureTests
  , kindInterningTests
  ]

kindPureTests :: TestTree
kindPureTests = testGroup "Kind tests (non-interning)" $
  map (uncurry3 encodeKindTest)
    [ ("Kind star", KStar, pkstar)
    , ("Kind Nat", KNat, pknat)
    ]
  where
    uncurry3 :: (a -> b -> c -> d) -> ((a, b, c) -> d)
    uncurry3 f (a, b, c) = f a b c


kindInterningTests :: TestTree
kindInterningTests = testGroup "Kind tests (interning)"
-- TODO[RB]: uncomment in PR that reworks kind interning
  -- [ kindInterningStarToStar
  -- , kindInterningStarToNatToStar
  -- , kindInterningAssertSharing
  -- ]
  []

runEncodeKindTest :: Kind -> (P.Kind, EncodeTestEnv)
runEncodeKindTest k = envToTestEnv <$> runState (encodeKind k) (initEncodeEnv testVersion)

-- TODO[RB]: uncomment in PR that reworks kind interning
-- kindInterningStarToStar :: TestTree
-- kindInterningStarToStar =
--   let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow KStar KStar)
--   in  testCase "star to star" $ do
--       assertInterned pk
--       assertInternedEnv e
--       pk @?= pkinterned 1
--       iKinds V.! 0 @?= pkstar
--       iKinds V.! 1 @?= pkarr (pkinterned 0) (pkinterned 0)

-- TODO[RB]: uncomment in PR that reworks kind interning
-- kindInterningStarToNatToStar :: TestTree
-- kindInterningStarToNatToStar =
--   let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KNat) KStar)
--   in  testCase "(star to nat) to star" $ do
--       assertInterned pk
--       assertInternedEnv e
--       pk @?= pkinterned 3
--       iKinds V.! 0 @?= pkstar
--       iKinds V.! 1 @?= pknat
--       iKinds V.! 2 @?= pkarr (pkinterned 0) (pkinterned 1)
--       iKinds V.! 3 @?= pkarr (pkinterned 2) (pkinterned 0)

-- TODO[RB]: uncomment in PR that reworks kind interning
-- kindInterningAssertSharing :: TestTree
-- kindInterningAssertSharing =
--   let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KStar) (KArrow KStar KStar))
--   in  testCase "Sharing: (* -> *) -> (* -> *)" $ do
--       assertInterned pk
--       assertInternedEnv e
--       pk @?= pkinterned 2
--       iKinds V.! 0 @?= pkstar
--       iKinds V.! 1 @?= pkarr (pkinterned 0) (pkinterned 0)
--       iKinds V.! 2 @?= pkarr (pkinterned 1) (pkinterned 1)

-- Types
typeInterningTests :: TestTree
typeInterningTests = testGroup "Type tests (interning)"
  [ typeInterningVar
  , typeInterningMyFuncUnit
  , typeInterningMaybeSyn
  , typeInterningUnit
  , typeInterningIntToBool
  -- TODO[RB]: uncomment in PR that reworks kind interning
  -- , typeInterningForall
  , typeInterningTStruct
  , typeInterningTNat
  , typeInterningAssertSharing
  ]


runEncodeTypeTest :: Type -> (P.Type, EncodeTestEnv)
runEncodeTypeTest k = envToTestEnv <$> runState (encodeType' k) (initEncodeEnv testVersion)

typeInterningVar :: TestTree
typeInterningVar =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ tvar "a"
  in  testCase "tvar a" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 0
      iTypes V.! 0 @?= (pliftT $ P.TypeSumVar $ P.Type_Var 0 V.empty)
      iStrings V.! 0 @?= "a"

typeInterningMyFuncUnit :: TestTree
typeInterningMyFuncUnit =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ tmyFuncTest TUnit
  in  testCase "MyFunc ()" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 1
      iTypes V.! 0 @?= ptunit
      iTypes V.! 1 @?= ptcon 1 (V.singleton $ ptinterned 0)
      iStrings V.! 0 @?= "Main"
      iStrings V.! 1 @?= "MyFunc"

typeInterningMaybeSyn :: TestTree
typeInterningMaybeSyn =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ tsynTest "MaybeSyn" [TUnit]
  in  testCase "MaybeSyn ()" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 1
      iTypes V.! 0 @?= ptunit
      iTypes V.! 1 @?= ptsyn 1 (V.singleton $ ptinterned 0)
      iStrings V.! 0 @?= "Main"
      iStrings V.! 1 @?= "MaybeSyn"

typeInterningUnit :: TestTree
typeInterningUnit =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest TUnit
  in  testCase "unit" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 0
      (iTypes V.! 0) @?= ptunit

typeInterningIntToBool :: TestTree
typeInterningIntToBool =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TInt64 :-> TBool
  in  testCase "Int -> Bool" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 2
      (iTypes V.! 0) @?= ptint
      (iTypes V.! 1) @?= ptbool
      (iTypes V.! 2) @?= ptarr (ptinterned 0) (ptinterned 1)

-- TODO[RB]: uncomment in PR that reworks kind interning
-- typeInterningForall :: TestTree
-- typeInterningForall =
--   let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest tyLamTyp
--   in  testCase "forall (a : * -> *). a -> a" $ do
--       assertInterned pt
--       assertInternedEnv e
--       pt @?= ptinterned 2
--       (iKinds V.! 0) @?= pkstar
--       (iKinds V.! 1) @?= pkarr (pkinterned 0) (pkinterned 0)
--       (iTypes V.! 1) @?= ptarr (ptinterned 0) (ptinterned 0)
--       (iTypes V.! 2) @?= ptforall 0 (pkinterned 1) (ptinterned 1)

typeInterningTStruct :: TestTree
typeInterningTStruct =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TStruct [(FieldName "foo", TUnit)]
  in  testCase "struct {foo :: ()}" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 1
      (iTypes V.! 0) @?= ptunit
      (iTypes V.! 1) @?= ptstructSingleton 0 (ptinterned 0)
      iStrings V.! 0 @?= "foo"

typeInterningTNat :: TestTree
typeInterningTNat =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TNat $ typeLevelNat (16 :: Int)
  in  testCase "tnat 16" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 0
      (iTypes V.! 0) @?= (pliftT $ P.TypeSumNat 16)

typeInterningAssertSharing :: TestTree
typeInterningAssertSharing =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest $ TUnit :-> TUnit
  in  testCase "Sharing: () -> ()" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 1
      (iTypes V.! 0) @?= ptunit
      (iTypes V.! 1) @?= ptarr (ptinterned 0) (ptinterned 0)

-- Exprs
exprInterningTests :: TestTree
exprInterningTests = testGroup "Expr tests (interning)"
  [ exprInterningVar
  , exprInterningVal
  , exprInterningBool
  , exprInterningLocLam
  ]

runEncodeExprTest :: Expr -> (P.Expr, EncodeTestEnv)
runEncodeExprTest k = envToTestEnv <$> runState (encodeExpr' k) (initEncodeEnv testVersion)

exprInterningVar :: TestTree
exprInterningVar =
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest $ eVar "x"
  in  testCase "eVar x" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs V.! 0 @?= peVar 0
      iStrings V.! 0 @?= "x"

exprInterningVal :: TestTree
exprInterningVal =
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest $ eVal "x"
  in  testCase "eVar x" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs V.! 0 @?= peVal 0 1
      iStrings V.! 0 @?= "Main"
      iStrings V.! 1 @?= "x"

exprInterningBool :: TestTree
exprInterningBool =
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest ETrue
  in  testCase "True" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs V.! 0 @?= peTrue

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
