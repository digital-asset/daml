-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.EncodeDecodeTest (
        module DA.Daml.LF.Proto3.EncodeDecodeTest
) where


import qualified Data.NameMap                             as NM

import           DA.Daml.LF.Proto3.Encode
import           DA.Daml.LF.Proto3.Decode

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests" [ rttTests ]

------------------------------------------------------------------------
-- Round-trip
------------------------------------------------------------------------

roundTripPackage :: Package -> Either Error Package
roundTripPackage p = (decode . encodePackage) p
  where
    decode :: P.Package -> Either Error Package
    decode = decodePackage
      (packageLfVersion p) --from passed package
      SelfPackageId


roundTripAssert :: Package -> Assertion
roundTripAssert p =
  either
    (\err -> assertFailure $ "Unexpected error: " ++ show err)
    (\val -> p @=? val)
    (roundTripPackage p)

rttTests :: TestTree
rttTests = testGroup "Round-trip tests"
    [ rttEmpty
    , rttTyLam
    ]

rttEmpty :: TestTree
rttEmpty = testCase "empty package" $ roundTripAssert $ mkOneModulePackage mkEmptyModule

rttTyLam :: TestTree
rttTyLam = testCase "tylam package" $ roundTripAssert $ mkOneModulePackage tyLamModule

------------------------------------------------------------------------
-- Examples
------------------------------------------------------------------------

tyLamModule :: Module
tyLamModule = mkEmptyModule{moduleValues = NM.singleton mkDefTyLam}

{-
f :: forall (a : * -> *). a -> a
f = Λa . λ (x : a) . x
-}
mkDefTyLam :: DefValue
mkDefTyLam = DefValue Nothing (f, tyLamTyp) tyLam
  where
    a = TypeVarName "a"
    x = ExprVarName "x"
    f = ExprValName "f"

    typToTyp = KArrow KStar KStar

    lam = ETmLam (x, TVar a) (EVar x)

    -- Λa . λ (x : a) . x
    tyLam :: Expr
    tyLam = ETyLam (a, typToTyp) lam

    -- forall (a : * -> *). a -> a
    tyLamTyp :: Type
    tyLamTyp = TForall (a, typToTyp) (TVar a :-> TVar a)
