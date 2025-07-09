-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeTest (
        module DA.Daml.LF.Proto3.EncodeTest
) where

import           Control.Monad.State.Strict
import qualified Data.Text.Lazy                           as TL
import qualified Data.Vector                              as V

import           DA.Daml.LF.Proto3.EncodeV2


import           DA.Daml.LF.Ast
import           DA.Daml.LF.Proto3.Util

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P


import           Test.Tasty.HUnit                               (Assertion, testCase, (@?=))
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests"
  [ kindTests
  , typeInterningTests
  ]

------------------------------------------------------------------------
-- EncodeTestEnv
------------------------------------------------------------------------
data EncodeTestEnv = EncodeTestEnv
    { iStrings :: V.Vector TL.Text
    , iKinds   :: V.Vector P.Kind
    , iTypes   :: V.Vector P.Type
    }

envToTestEnv :: EncodeEnv -> EncodeTestEnv
envToTestEnv EncodeEnv{..} =
  EncodeTestEnv (packInternedStrings internedStrings)
                (packInternedKinds   internedKindsMap)
                (packInternedTypes   internedTypesMap)

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

kindTests :: TestTree
kindTests = testGroup "Kind tests"
  [ kindPureTests
  , kindInterningTests
  -- TODO[RB]: add tests that feature kinds occurring in types occuring in
  -- expressions (will be done when type- and expression interning will be
  -- implemented)
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
  let (pk, EncodeTestEnv{..}) = runEncodeKindTest (KArrow KStar KStar)
  in  testCase "star to star" $ do
      pk @?= pkinterned 0
      iKinds V.! 0 @?= pkarr pkstar pkstar

kindInterningStarToNatToStar :: TestTree
kindInterningStarToNatToStar =
  let (pk, EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KNat) KStar)
  in  testCase "(star to nat) to star" $ do
      pk @?= pkinterned 1
      iKinds V.! 0 @?= pkarr pkstar pknat
      iKinds V.! 1 @?= pkarr (pkinterned 0) pkstar

-- Verify that non-leafs ARE shared
kindInterningAssertSharing :: TestTree
kindInterningAssertSharing =
  let (pk, EncodeTestEnv{..}) = runEncodeKindTest (KArrow (KArrow KStar KStar) (KArrow KStar KStar))
  in  testCase "Sharing: (* -> *) -> (* -> *)" $ do
      pk @?= pkinterned 1
      iKinds V.! 0 @?= pkarr pkstar pkstar
      iKinds V.! 1 @?= pkarr (pkinterned 0) (pkinterned 0)

------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------
typeInterningTests :: TestTree
typeInterningTests = testGroup "Type tests (interning)"
  [ typeInterningVar
  , typeInterningMaybeUnit
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
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ tvar "a"
  in  testCase "tvar a" $ do
      pt @?= ptinterned 0
      iTypes V.! 0 @?= (pliftT $ P.TypeSumVar $ P.Type_Var 0 V.empty)
      iStrings V.! 0 @?= "a"

typeInterningMaybeUnit :: TestTree
typeInterningMaybeUnit =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ tmaybe TUnit
  in  testCase "Maybe ()" $ do
      pt @?= ptinterned 1
      iTypes V.! 0 @?= ptunit
      iTypes V.! 1 @?= ptcon 1 (V.singleton $ ptinterned 0)
      iStrings V.! 0 @?= "Main"
      iStrings V.! 1 @?= "Maybe"

typeInterningMaybeSyn :: TestTree
typeInterningMaybeSyn =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ tsyn "MaybeSyn" [TUnit]
  in  testCase "MaybeSyn ()" $ do
      pt @?= ptinterned 1
      iTypes V.! 0 @?= ptunit
      iTypes V.! 1 @?= ptsyn 1 (V.singleton $ ptinterned 0)
      iStrings V.! 0 @?= "Main"
      iStrings V.! 1 @?= "MaybeSyn"

typeInterningUnit :: TestTree
typeInterningUnit =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest TUnit
  in  testCase "unit" $ do
      pt @?= ptinterned 0
      (iTypes V.! 0) @?= ptunit

typeInterningIntToBool :: TestTree
typeInterningIntToBool =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ TInt64 :-> TBool
  in  testCase "Int -> Bool" $ do
      pt @?= ptinterned 2
      (iTypes V.! 0) @?= ptint
      (iTypes V.! 1) @?= ptbool
      (iTypes V.! 2) @?= ptarr (ptinterned 0) (ptinterned 1)

typeInterningForall :: TestTree
typeInterningForall =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest tyLamTyp
  in  testCase "forall (a : * -> *). a -> a" $ do
      pt @?= ptinterned 2
      (iKinds V.! 0) @?= pkarr pkstar pkstar
      (iTypes V.! 1) @?= ptarr (ptinterned 0) (ptinterned 0)
      (iTypes V.! 2) @?= ptforall 0 (pkinterned 0) (ptinterned 1)

typeInterningTStruct :: TestTree
typeInterningTStruct =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ TStruct [(FieldName "foo", TUnit)]
  in  testCase "struct {foo :: ()}" $ do
      pt @?= ptinterned 1
      (iTypes V.! 0) @?= ptunit
      (iTypes V.! 1) @?= ptstructSingleton 0 (ptinterned 0)
      iStrings V.! 0 @?= "foo"

typeInterningTNat :: TestTree
typeInterningTNat =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ TNat $ typeLevelNat (16 :: Int)
  in  testCase "tnat 16" $ do
      pt @?= ptinterned 0
      (iTypes V.! 0) @?= (pliftT $ P.TypeSumNat 16)

typeInterningAssertSharing :: TestTree
typeInterningAssertSharing =
  let (pt, EncodeTestEnv{..}) = runEncodeTypeTest $ TUnit :-> TUnit
  in  testCase "Sharing: () -> ()" $ do
      pt @?= ptinterned 1
      (iTypes V.! 0) @?= ptunit
      (iTypes V.! 1) @?= ptarr (ptinterned 0) (ptinterned 0)
