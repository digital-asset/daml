-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeTest (
        module DA.Daml.LF.Proto3.EncodeTest
) where


import           Control.Monad.State.Strict
import           Data.Int
import qualified Data.Vector                              as V

import           DA.Daml.LF.Proto3.EncodeV2
import           DA.Daml.LF.Proto3.InternedMap

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests" [encTests]

------------------------------------------------------------------------
-- Params
------------------------------------------------------------------------
testVersion :: Version
testVersion = Version V2 PointDev

------------------------------------------------------------------------
-- EncodeTests
------------------------------------------------------------------------
encodeKindAssert :: Kind -> P.Kind -> Assertion
encodeKindAssert k pk =
  let (pk', _) = runState (encodeKind k) env
  in  pk @=? pk'
    where
      env = initEncodeEnv testVersion

encodeKindTest :: String -> Kind -> P.Kind -> TestTree
encodeKindTest str k pk = testCase str $ encodeKindAssert k pk

encTests :: TestTree
encTests = testGroup "Encoding tests"
  [ encPureTests
  , encInterningTests
  -- TODO[RB]: add tests that feature kinds occurring in types occuring in
  -- expressions (will be done when type- and expression interning will be
  -- implemented)
  ]

encPureTests :: TestTree
encPureTests = testGroup "Encoding tests (non-interning)" $
  map (uncurry3 encodeKindTest)
    [ ("Kind star", KStar, pkstar)
    , ("Kind Nat", KNat, pknat)
    ]
  where
    uncurry3 :: (a -> b -> c -> d) -> ((a, b, c) -> d)
    uncurry3 f (a, b, c) = f a b c


encInterningTests :: TestTree
encInterningTests = testGroup "Encoding tests (interning)"
  [ encInterningStarToStar
  , encInterningStarToNatToStar
  ]

runEncodeTest :: Kind -> (P.Kind, EncodeEnv)
runEncodeTest k = runState (encodeKind k) (initEncodeEnv testVersion)

encInterningStarToStar :: TestTree
encInterningStarToStar =
  let (pk, EncodeEnv{internedKindsMap}) = runEncodeTest (KArrow KStar KStar)
  in  testCase "star to star" $ do
    pk @=? interned 0
    toVec internedKindsMap V.! 0 @=? pkarr' pkstar pkstar

encInterningStarToNatToStar :: TestTree
encInterningStarToNatToStar =
  let (pk, EncodeEnv{internedKindsMap}) = runEncodeTest (KArrow (KArrow KStar KNat) KStar)
  in  testCase "(star to nat) to star" $ do
    pk @=? interned 1
    toVec internedKindsMap V.! 0 @=? pkarr' pkstar pknat
    toVec internedKindsMap V.! 1 @=? pkarr' (interned 0) pkstar

------------------------------------------------------------------------
-- Proto Ast helpers
------------------------------------------------------------------------
pkstar :: P.Kind
pkstar = (P.Kind . Just . P.KindSumStar) P.Unit

pknat :: P.Kind
pknat = (P.Kind . Just . P.KindSumNat) P.Unit

pkarr :: P.Kind -> P.Kind -> P.Kind
pkarr k1 k2 = (P.Kind . Just) (pkarr' k1 k2)

pkarr' :: P.Kind -> P.Kind -> P.KindSum
pkarr' k1 k2 = P.KindSumArrow $ P.Kind_Arrow (V.singleton k1) (Just k2)

interned :: Int32 -> P.Kind
interned = P.Kind . Just . P.KindSumInterned
