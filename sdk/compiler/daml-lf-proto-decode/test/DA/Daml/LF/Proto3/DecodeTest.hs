-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.DecodeTest (
        module DA.Daml.LF.Proto3.DecodeTest
) where


import           Data.Int
import qualified Data.Vector                              as V

import           DA.Daml.LF.Proto3.DecodeV2

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests" [ dec_tests ]

------------------------------------------------------------------------
-- Params
------------------------------------------------------------------------
testVersion :: Version
testVersion = Version V2 PointDev

------------------------------------------------------------------------
-- DecodeTests
------------------------------------------------------------------------
dec_tests :: TestTree
dec_tests = testGroup "decoding tests"
  [ dec_pure_tests
  , dec_interning_tests
  ]

emptyDecodeEnv :: DecodeEnv
emptyDecodeEnv = DecodeEnv V.empty V.empty V.empty V.empty SelfPackageId testVersion

decodeKindAssert :: P.Kind -> Kind -> Assertion
decodeKindAssert pk k =
  either
    (\err -> assertFailure $ "Unexpected error: " ++ show err)
    (\k' -> k @=? k')
    (runDecode env (decodeKind pk))
    where
      env = emptyDecodeEnv


decodeKindTest :: String -> P.Kind -> Kind -> TestTree
decodeKindTest str pk k = testCase str $ decodeKindAssert pk k

dec_pure_tests :: TestTree
dec_pure_tests = testGroup "decoding tests (non-interning)" $ map (uncurry3 decodeKindTest)
  [ ("Kind star", pkstar, KStar)
  , ("Kind Nat", pknat, KNat)
  , ("star to star", pkarr pkstar pkstar, KArrow KStar KStar)
  , ("(star to nat) to star", pkarr (pkarr pkstar pknat) pkstar, KArrow (KArrow KStar KNat) KStar)
  ]
  where
    uncurry3 :: (a -> b -> c -> d) -> ((a, b, c) -> d)
    uncurry3 f (a, b, c) = f a b c


dec_interning_tests :: TestTree
dec_interning_tests = testGroup "decoding tests (interning)"
  [ dec_interning_starToStar
  ]


dec_interning_starToStar :: TestTree
dec_interning_starToStar =
  let kinds = V.singleton (KArrow KStar KStar)
      env = emptyDecodeEnv{internedKinds = kinds}
  in  testCase "star to star" $ either
        (\err -> assertFailure $ "Unexpected error: " ++ show err)
        (\k -> k @=? KArrow KStar KStar)
        (runDecode env (decodeKind (interned 0)))

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
