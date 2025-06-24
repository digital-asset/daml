-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeTest (
        module DA.Daml.LF.Proto3.EncodeTest
      , module DA.Daml.LF.Proto3.EncodeV2 -- for debugging in ghci
      , module Text.Pretty.Simple         -- for debugging in ghci
) where

-- TODO: remove me after switching to internedMap
import qualified Data.List as L
import qualified Data.Map.Strict as Map

import           Control.Monad.State.Strict
import           Data.Int
import qualified Data.Vector                              as V
import           Text.Pretty.Simple

import           DA.Daml.LF.Proto3.EncodeV2
import           DA.Daml.LF.Proto3.InternedMap

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P
import qualified Proto3.Suite                             as P (Enumerated (..))

import           Test.Tasty.HUnit                               (Assertion, testCase, (@?=))
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests"
  [ kind_tests
  , type_tests
  ]

------------------------------------------------------------------------
-- Params
------------------------------------------------------------------------
testVersion :: Version
testVersion = Version V2 PointDev

------------------------------------------------------------------------
-- Kinds
------------------------------------------------------------------------
encodeKindAssert :: Kind -> P.Kind -> Assertion
encodeKindAssert k pk =
  let (pk', _) = runState (encodeKind k) env
  in  pk' @?= pk
    where
      env = initEncodeEnv testVersion

encodeKindTest :: String -> Kind -> P.Kind -> TestTree
encodeKindTest str k pk = testCase str $ encodeKindAssert k pk

kind_tests :: TestTree
kind_tests = testGroup "Kind tests"
  [ kind_pure_tests
  , kind_interning_tests
  ]

kind_pure_tests :: TestTree
kind_pure_tests = testGroup "Kind tests (non-interning)" $
  map (uncurry3 encodeKindTest)
    [ ("Kind star", KStar, pkstar)
    , ("Kind Nat", KNat, pknat)
    ]
  where
    uncurry3 :: (a -> b -> c -> d) -> ((a, b, c) -> d)
    uncurry3 f (a, b, c) = f a b c


kind_interning_tests :: TestTree
kind_interning_tests = testGroup "Kind tests (interning)"
  [ kind_interning_starToStar
  , kind_interning_starToNatToStar
  ]

runEncodeKindTest :: Kind -> (P.Kind, EncodeEnv)
runEncodeKindTest k = runState (encodeKind k) (initEncodeEnv testVersion)

kind_interning_starToStar :: TestTree
kind_interning_starToStar =
  let (pk, EncodeEnv{_internedKindsMap}) = runEncodeKindTest (KArrow KStar KStar)
  in  testCase "star to star" $ do
    pk @?= pkinterned 0
    (liftK $ toVec _internedKindsMap V.! 0) @?= pkarr pkstar pkstar

kind_interning_starToNatToStar :: TestTree
kind_interning_starToNatToStar =
  let (pk, EncodeEnv{_internedKindsMap}) = runEncodeKindTest (KArrow (KArrow KStar KNat) KStar)
  in  testCase "(star to nat) to star" $ do
    pk @?= pkinterned 1
    (liftK $ toVec _internedKindsMap V.! 0) @?= pkarr pkstar pknat
    (liftK $ toVec _internedKindsMap V.! 1) @?= pkarr (pkinterned 0) pkstar

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------
type_tests :: TestTree
type_tests = testGroup "Type tests"
  [ type_pure_tests
  , type_interning_tests
  ]

type_pure_tests :: TestTree
type_pure_tests = testGroup "Type tests (non-interning)"
  [
  ]

type_interning_tests :: TestTree
type_interning_tests = testGroup "Type tests (interning)"
  [ type_interning_unit
  , type_interning_intToBool
  , type_interning_forall
  ]

runEncodeTypeTest :: Type -> (P.Type, EncodeEnv)
runEncodeTypeTest k = runState (encodeType' k) (initEncodeEnv testVersion)

-- TODO remove after switching to internedMap
toVec' :: Ord b => Map.Map a b -> V.Vector a
toVec' it = V.fromList $ map fst $ L.sortOn snd $ Map.toList it

type_interning_unit :: TestTree
type_interning_unit =
  let (pt, EncodeEnv{internedTypes}) = runEncodeTypeTest tunit
  in  testCase "unit" $ do
    pt @?= ptinterned 0
    (liftT $ toVec' internedTypes V.! 0) @?= ptunit

type_interning_intToBool :: TestTree
type_interning_intToBool =
  let (pt, EncodeEnv{internedTypes}) = runEncodeTypeTest $ tarr tint tbool
  in  testCase "Int -> Bool" $ do
    pt @?= ptinterned 2
    (liftT $ toVec' internedTypes V.! 0) @?= ptint
    (liftT $ toVec' internedTypes V.! 1) @?= ptbool
    (liftT $ toVec' internedTypes V.! 2) @?= ptarr (ptinterned 0) (ptinterned 1)

type_interning_forall :: TestTree
type_interning_forall =
  let (pt, EncodeEnv{_internedKindsMap, internedTypes}) = runEncodeTypeTest tyLamTyp
  in  testCase "forall (a : * -> *). a -> a" $ do
    pt @?= ptinterned 2
    (liftK $ toVec _internedKindsMap V.! 0) @?= pkarr pkstar pkstar
    (liftT $ toVec' internedTypes V.! 1) @?= ptarr (ptinterned 0) (ptinterned 0)
    (liftT $ toVec' internedTypes V.! 2) @?= ptforall 0 (pkinterned 0) (ptinterned 1)

------------------------------------------------------------------------
-- Utnil
------------------------------------------------------------------------

------------------------------------------------------------------------
-- Lf AST helpers
------------------------------------------------------------------------
tunit :: Type
tunit = TBuiltin BTUnit

tint :: Type
tint = TBuiltin BTInt64

tbool :: Type
tbool = TBuiltin BTBool

tarr :: Type -> Type -> Type
tarr t1 t2 = TApp (TApp (TBuiltin BTArrow) t1) t2

tyLamTyp :: Type
tyLamTyp = TForall (a, typToTyp) (tarr (TVar a) (TVar a))
  where
    a = TypeVarName "a"
    typToTyp = KArrow KStar KStar


------------------------------------------------------------------------
-- Proto AST helpers
------------------------------------------------------------------------

-- kinds
liftK :: P.KindSum -> P.Kind
liftK = P.Kind . Just

pkstar :: P.Kind
pkstar = (liftK . P.KindSumStar) P.Unit

pknat :: P.Kind
pknat = (liftK . P.KindSumNat) P.Unit

pkarr :: P.Kind -> P.Kind -> P.Kind
pkarr k1 k2 = liftK $ P.KindSumArrow $ P.Kind_Arrow (V.singleton k1) (Just k2)

pkinterned :: Int32 -> P.Kind
pkinterned = liftK . P.KindSumInterned

-- types
liftT :: P.TypeSum -> P.Type
liftT = P.Type . Just

pbuiltin :: P.BuiltinType -> P.Type
pbuiltin bit = liftT $ P.TypeSumBuiltin $ P.Type_Builtin (P.Enumerated $ Right bit) V.empty

ptunit :: P.Type
ptunit = pbuiltin P.BuiltinTypeUNIT

ptint :: P.Type
ptint = pbuiltin P.BuiltinTypeINT64

ptbool :: P.Type
ptbool = pbuiltin P.BuiltinTypeBOOL

ptarr :: P.Type -> P.Type -> P.Type
ptarr t1 t2 = liftT $ P.TypeSumBuiltin $ P.Type_Builtin (P.Enumerated $ Right P.BuiltinTypeARROW) (V.fromList [t1, t2])

ptinterned :: Int32 -> P.Type
ptinterned = liftT . P.TypeSumInterned

ptforall :: Int32 -> P.Kind -> P.Type -> P.Type
ptforall a k t = liftT $ P.TypeSumForall $ P.Type_Forall (V.singleton $ P.TypeVarWithKind a (Just k)) (Just t)
