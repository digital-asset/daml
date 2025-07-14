-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
-- {-# LANGUAGE DeriveDataTypeable #-}

module DA.Daml.LF.Proto3.EncodeTest (
        module DA.Daml.LF.Proto3.EncodeTest
) where


import           Control.Monad.State.Strict
import qualified Data.Text.Lazy                           as TL
import qualified Data.Vector                              as V
import           Text.Printf

-- generic depth test
import Data.Data
import Data.Generics.Aliases

--tmp, move to utils
import           Data.Int
import           Data.Text                                (Text)
import qualified Proto3.Suite                             as P



import           DA.Daml.LF.Proto3.EncodeV2


import           DA.Daml.LF.Ast
import           DA.Daml.LF.Proto3.Util

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P


import           Test.Tasty.HUnit                               (Assertion, testCase, assertBool, (@?=))
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests"
  [ unitTests
  , propertyTests
  ]

------------------------------------------------------------------------
-- Property tests
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

peApp :: P.Expr -> P.Expr -> P.Expr
peApp e1 e2 = liftE $ P.ExprSumApp $ P.Expr_App (Just e1) (V.singleton e2)

propertyCorrectExprArrow :: TestTree
propertyCorrectExprArrow =
  testCase "true (interned 0)" $
    assertBool "true (interned 0) should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peApp peTrue (peInterned 0))

peTyApp :: P.Expr -> P.Type -> P.Expr
peTyApp e t = liftE $ P.ExprSumTyApp $ P.Expr_TyApp (Just e) (V.singleton t)

propertyCorrectTypeInExpr :: TestTree
propertyCorrectTypeInExpr =
  testCase "(interned 0) TUnit" $
    assertBool "(interned 0) TUnit should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peTyApp (peInterned 0) ptunit)

peTyAbs :: Int32 -> P.Kind -> P.Expr -> P.Expr
peTyAbs i k e = liftE $ P.ExprSumTyAbs $ P.Expr_TyAbs (V.singleton var) (Just e)
  where
    var :: P.TypeVarWithKind
    var = P.TypeVarWithKind i (Just k)

propertyCorrectKindInExpr :: TestTree
propertyCorrectKindInExpr =
  testCase "/\0::(* -> *). ()" $
    assertBool "/\0::(* -> *). () should be rejected as not well interned, but is accepted" $
    not (wellInterned $ peTyAbs 0 (pkarr pkstar pkstar) peUnit)

deepTests :: TestTree
deepTests = testGroup "Deep AST tests"
  [ deepKindArrTest
  , deepTypeArrTest
  , deepLetExprTest
  ]


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
      (P.KindSumInterned _) -> mempty
      -- kind-specific ignoring of leafs
      (P.KindSumStar     _) -> mempty
      (P.KindSumNat      _) -> mempty
      _                     -> singleKind <> mconcat (gmapQ countConcreteConstrs k)

    countTypes (t :: P.TypeSum) = case t of
      (P.TypeSumInterned _) -> mempty
      _                     -> singleType <> mconcat (gmapQ countConcreteConstrs t)

    countExprs (e :: P.ExprSum) = case e of
      (P.ExprSumInterned _) -> mempty
      _                     -> singleExpr <> mconcat (gmapQ countConcreteConstrs e)

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

deriving instance Data P.Unit
deriving instance Data P.SelfOrImportedPackageId
deriving instance Data P.SelfOrImportedPackageIdSum
deriving instance Data P.ModuleId
deriving instance Data P.TypeConId
deriving instance Data P.TypeSynId
deriving instance Data P.ValueId
deriving instance Data P.FieldWithType
deriving instance Data P.VarWithType
deriving instance Data P.TypeVarWithKind
deriving instance Data P.FieldWithExpr
deriving instance Data P.Binding
deriving instance Data P.Kind
deriving instance Data P.Kind_Arrow
deriving instance Data P.KindSum
deriving instance Data P.BuiltinType
deriving instance Data P.Type
deriving instance Data P.Type_Var
deriving instance Data P.Type_Con
deriving instance Data P.Type_Syn
deriving instance Data P.Type_Builtin
deriving instance Data P.Type_Forall
deriving instance Data P.Type_Struct
deriving instance Data P.TypeSum
deriving instance Data P.BuiltinCon
deriving instance Data P.BuiltinFunction
deriving instance Data P.BuiltinLit
deriving instance Data P.BuiltinLit_RoundingMode
deriving instance Data P.BuiltinLit_FailureCategory
deriving instance Data P.BuiltinLitSum
deriving instance Data P.Location
deriving instance Data P.Location_Range
deriving instance Data P.Expr
deriving instance Data P.Expr_RecCon
deriving instance Data P.Expr_RecProj
deriving instance Data P.Expr_RecUpd
deriving instance Data P.Expr_VariantCon
deriving instance Data P.Expr_EnumCon
deriving instance Data P.Expr_StructCon
deriving instance Data P.Expr_StructProj
deriving instance Data P.Expr_StructUpd
deriving instance Data P.Expr_App
deriving instance Data P.Expr_TyApp
deriving instance Data P.Expr_Abs
deriving instance Data P.Expr_TyAbs
deriving instance Data P.Expr_Nil
deriving instance Data P.Expr_Cons
deriving instance Data P.Expr_OptionalNone
deriving instance Data P.Expr_OptionalSome
deriving instance Data P.Expr_ToAny
deriving instance Data P.Expr_FromAny
deriving instance Data P.Expr_ToAnyException
deriving instance Data P.Expr_FromAnyException
deriving instance Data P.Expr_Throw
deriving instance Data P.Expr_ToInterface
deriving instance Data P.Expr_FromInterface
deriving instance Data P.Expr_CallInterface
deriving instance Data P.Expr_ViewInterface
deriving instance Data P.Expr_SignatoryInterface
deriving instance Data P.Expr_ObserverInterface
deriving instance Data P.Expr_UnsafeFromInterface
deriving instance Data P.Expr_ToRequiredInterface
deriving instance Data P.Expr_FromRequiredInterface
deriving instance Data P.Expr_UnsafeFromRequiredInterface
deriving instance Data P.Expr_InterfaceTemplateTypeRep
deriving instance Data P.Expr_ChoiceController
deriving instance Data P.Expr_ChoiceObserver
deriving instance Data P.Expr_Experimental
deriving instance Data P.ExprSum
deriving instance Data P.CaseAlt
deriving instance Data P.CaseAlt_Variant
deriving instance Data P.CaseAlt_Enum
deriving instance Data P.CaseAlt_Cons
deriving instance Data P.CaseAlt_OptionalSome
deriving instance Data P.CaseAltSum
deriving instance Data P.Case
deriving instance Data P.Block
deriving instance Data P.Pure
deriving instance Data P.Update
deriving instance Data P.Update_Create
deriving instance Data P.Update_CreateInterface
deriving instance Data P.Update_Exercise
deriving instance Data P.Update_ExerciseInterface
deriving instance Data P.Update_ExerciseByKey
deriving instance Data P.Update_Fetch
deriving instance Data P.Update_FetchInterface
deriving instance Data P.Update_EmbedExpr
deriving instance Data P.Update_RetrieveByKey
deriving instance Data P.Update_TryCatch
deriving instance Data P.UpdateSum
deriving instance Data P.TemplateChoice
deriving instance Data P.InterfaceInstanceBody
deriving instance Data P.InterfaceInstanceBody_InterfaceInstanceMethod
deriving instance Data P.DefTemplate
deriving instance Data P.DefTemplate_DefKey
deriving instance Data P.DefTemplate_Implements
deriving instance Data P.InterfaceMethod
deriving instance Data P.DefInterface
deriving instance Data P.DefException
deriving instance Data P.DefDataType
deriving instance Data P.DefDataType_Fields
deriving instance Data P.DefDataType_EnumConstructors
deriving instance Data P.DefDataTypeDataCons
deriving instance Data P.DefTypeSyn
deriving instance Data P.DefValue
deriving instance Data P.DefValue_NameWithType
deriving instance Data P.FeatureFlags
deriving instance Data P.Module
deriving instance Data P.InternedDottedName
deriving instance Data P.UpgradedPackageId
deriving instance Data P.Package
deriving instance Data P.PackageMetadata

instance Data a => Data (P.Enumerated a) where
  gfoldl f z (P.Enumerated x) = z P.Enumerated `f` x

  gunfold k z c = case constrIndex c of
    1 -> k (z P.Enumerated)
    _ -> error "gunfold: Bad constructor index for Enumerated"

  toConstr (P.Enumerated _) = enumeratedConstr

  dataTypeOf _ = enumeratedDataType

enumeratedConstr :: Constr
enumeratedConstr = mkConstr enumeratedDataType "Enumerated" [] Prefix

enumeratedDataType :: DataType
enumeratedDataType = mkDataType "Proto3.Suite.Types.Enumerated" [enumeratedConstr]

-- kindDepth :: P.Kind -> Int
-- kindDepth = _astDepth

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
  [ kindInterningStarToStar
  , kindInterningStarToNatToStar
  , kindInterningAssertSharing
  ]

runEncodeKindTest :: Kind -> (P.Kind, EncodeTestEnv)
runEncodeKindTest k = envToTestEnv <$> runState (encodeKind k) (initEncodeEnv testVersion)

kindInterningStarToStar :: TestTree
kindInterningStarToStar =
  let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow KStar KStar)
  in  testCase "star to star" $ do
      assertInterned pk
      assertInternedEnv e
      pk @?= pkinterned 0
      iKinds V.! 0 @?= pkarr pkstar pkstar

kindInterningStarToNatToStar :: TestTree
kindInterningStarToNatToStar =
  let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KNat) KStar)
  in  testCase "(star to nat) to star" $ do
      assertInterned pk
      assertInternedEnv e
      pk @?= pkinterned 1
      iKinds V.! 0 @?= pkarr pkstar pknat
      iKinds V.! 1 @?= pkarr (pkinterned 0) pkstar

-- Verify that non-leafs ARE shared
kindInterningAssertSharing :: TestTree
kindInterningAssertSharing =
  let (pk, e@EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KStar) (KArrow KStar KStar))
  in  testCase "Sharing: (* -> *) -> (* -> *)" $ do
      assertInterned pk
      assertInternedEnv e
      pk @?= pkinterned 1
      iKinds V.! 0 @?= pkarr pkstar pkstar
      iKinds V.! 1 @?= pkarr (pkinterned 0) (pkinterned 0)

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

typeInterningForall :: TestTree
typeInterningForall =
  let (pt, e@EncodeTestEnv{..}) = runEncodeTypeTest tyLamTyp
  in  testCase "forall (a : * -> *). a -> a" $ do
      assertInterned pt
      assertInternedEnv e
      pt @?= ptinterned 2
      (iKinds V.! 0) @?= pkarr pkstar pkstar
      (iTypes V.! 1) @?= ptarr (ptinterned 0) (ptinterned 0)
      (iTypes V.! 2) @?= ptforall 0 (pkinterned 0) (ptinterned 1)

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
  ]


runEncodeExprTest :: Expr -> (P.Expr, EncodeTestEnv)
runEncodeExprTest k = envToTestEnv <$> runState (encodeExpr' k) (initEncodeEnv testVersion)

-- ast
eVar :: Text -> Expr
eVar = EVar . ExprVarName

eQual :: a -> Qualified a
eQual x = Qualified SelfPackageId (ModuleName ["Main"]) x

eVal :: Text -> Expr
eVal = EVal . eQual . ExprValName

eTrue :: Expr
eTrue = EBuiltinFun $ BEBool True

eUnit :: Expr
eUnit = EBuiltinFun BEUnit

-- P.ast
liftE :: P.ExprSum -> P.Expr
liftE = P.Expr Nothing . Just

peInterned :: Int32 -> P.Expr
peInterned = liftE . P.ExprSumInterned

peBuiltinCon :: P.BuiltinCon -> P.Expr
peBuiltinCon bit = liftE $ P.ExprSumBuiltinCon $ P.Enumerated $ Right bit

peUnit :: P.Expr
peUnit = peBuiltinCon P.BuiltinConCON_UNIT

peVar :: Int32 -> P.Expr
peVar = liftE . P.ExprSumVarInternedStr

peSelfOrImportedPackageIdSelf :: Maybe P.SelfOrImportedPackageId
peSelfOrImportedPackageIdSelf = Just $ P.SelfOrImportedPackageId $ Just $ P.SelfOrImportedPackageIdSumSelfPackageId P.Unit

peVal :: Int32 -> Int32 -> P.Expr
peVal mod val = liftE $ P.ExprSumVal $ P.ValueId (Just $ P.ModuleId peSelfOrImportedPackageIdSelf mod) val

peTrue :: P.Expr
peTrue = peBuiltinCon P.BuiltinConCON_TRUE

-- tests
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
  let (pe, e@EncodeTestEnv{..}) = runEncodeExprTest eTrue
  in  testCase "True" $ do
      assertInterned pe
      assertInternedEnv e
      pe @?= peInterned 0
      iExprs V.! 0 @?= peTrue

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
letOfDepth 0 = eUnit
letOfDepth i = elet (letOfDepth $ i -1)
  where
    elet :: Expr -> Expr
    elet = ELet unitBinding

    unitBinding :: Binding
    unitBinding = Binding (ExprVarName "x", TUnit) eUnit

deepLetExprTest :: TestTree
deepLetExprTest =
  let level = 100
      (pe, e) = runEncodeExprTest $ letOfDepth level
  in  testCase (printf "deep let test (%d levels)" level) $ do
      assertInterned pe
      assertInternedEnv e
