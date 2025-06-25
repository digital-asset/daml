-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Test (
        module DA.Daml.LF.Proto3.Test
) where


import           Control.Monad.State.Strict
import           Data.Int
import qualified Data.NameMap                             as NM
import           Data.Tuple.Extra
import qualified Data.Vector                              as V

import           DA.Daml.LF.Proto3.EncodeV2
import           DA.Daml.LF.Proto3.InternedMap

import           DA.Daml.LF.Proto3.DecodeV2

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Test.Tasty.HUnit
import           Test.Tasty

entry :: IO ()
entry = defaultMain $ testGroup "All tests"
    [ rtt_tests
    , enc_tests
    , dec_tests
    ]

------------------------------------------------------------------------
-- Params
------------------------------------------------------------------------
testVersion :: Version
testVersion = Version V2 PointDev

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

rtt_tests :: TestTree
rtt_tests = testGroup "Round-trip tests"
    [ rtt_empty
    , rtt_tyLam
    ]

rtt_empty :: TestTree
rtt_empty = testCase "empty package" $ roundTripAssert $ oneModulePackage emptyModule

rtt_tyLam :: TestTree
rtt_tyLam = testCase "tylam package" $ roundTripAssert $ oneModulePackage tyLamModule


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

enc_tests :: TestTree
enc_tests = testGroup "Encoding tests"
  [ enc_pure_tests
  , enc_interning_tests
  ]

enc_pure_tests :: TestTree
enc_pure_tests = testGroup "Encoding tests (non-interning)" $
  map (uncurry3 encodeKindTest)
    [ ("Kind star", KStar, pkstar)
    , ("Kind Nat", KNat, pknat)
    ]


enc_interning_tests :: TestTree
enc_interning_tests = testGroup "Encoding tests (interning)"
  [ enc_interning_starToStar
  , enc_interning_starToNatToStar
  ]

runEncodeTest :: Kind -> (P.Kind, EncodeEnv)
runEncodeTest k = runState (encodeKind k) (initEncodeEnv testVersion)

enc_interning_starToStar :: TestTree
enc_interning_starToStar =
  let (pk, EncodeEnv{internedKindsMap}) = runEncodeTest (KArrow KStar KStar)
  in  testCase "star to star" $ do
    pk @=? interned 0
    toVec internedKindsMap V.! 0 @=? pkarr' pkstar pkstar

enc_interning_starToNatToStar :: TestTree
enc_interning_starToNatToStar =
  let (pk, EncodeEnv{internedKindsMap}) = runEncodeTest (KArrow (KArrow KStar KNat) KStar)
  in  testCase "(star to nat) to star" $ do
    pk @=? interned 1
    toVec internedKindsMap V.! 0 @=? pkarr' pkstar pknat
    toVec internedKindsMap V.! 1 @=? pkarr' (interned 0) pkstar

------------------------------------------------------------------------
-- DecodeTests
------------------------------------------------------------------------
dec_tests :: TestTree
dec_tests = testGroup "decoding tests"
  [ dec_pure_tests
  , dec_interning_tests
  ]

emptyDecodeEnv :: DecodeEnv
emptyDecodeEnv = DecodeEnv V.empty V.empty V.empty V.empty SelfPackageId

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

------------------------------------------------------------------------
-- Examples
------------------------------------------------------------------------
emptyModule :: Module
emptyModule = Module{..}
  where
    moduleName :: ModuleName
    moduleName = ModuleName ["test"]
    moduleSource :: (Maybe FilePath)
    moduleSource = Nothing
    moduleFeatureFlags :: FeatureFlags
    moduleFeatureFlags = FeatureFlags
    moduleSynonyms :: (NM.NameMap DefTypeSyn)
    moduleSynonyms = NM.empty
    moduleDataTypes :: (NM.NameMap DefDataType)
    moduleDataTypes = NM.empty
    moduleValues :: (NM.NameMap DefValue)
    moduleValues = NM.empty
    moduleTemplates :: (NM.NameMap Template)
    moduleTemplates = NM.empty
    moduleExceptions :: (NM.NameMap DefException)
    moduleExceptions = NM.empty
    moduleInterfaces :: (NM.NameMap DefInterface)
    moduleInterfaces = NM.empty

tyLamModule :: Module
tyLamModule = emptyModule{moduleValues = NM.singleton defTyLam}

{-
f :: forall (a : * -> *). a -> a
f = Λa . λ (x : a) . x
-}
defTyLam :: DefValue
defTyLam = DefValue Nothing (f, tyLamTyp) tyLam
  where
    a = TypeVarName "a"
    x = ExprVarName "x"
    f = ExprValName "f"

    typToTyp = KArrow KStar KStar

    lam = ETmLam (x, TVar a) (EVar x)


    arr :: Type -> Type -> Type
    arr x y = TApp (TApp (TBuiltin BTArrow) x) y

    -- Λa . λ (x : a) . x
    tyLam :: Expr
    tyLam = ETyLam (a, typToTyp) lam

    -- forall (a : * -> *). a -> a
    tyLamTyp :: Type
    tyLamTyp = TForall (a, typToTyp) (arr (TVar a) (TVar a))

oneModulePackage :: Module -> Package
oneModulePackage m = Package{..}
  where
    packageLfVersion :: Version
    packageLfVersion = testVersion
    packageModules :: NM.NameMap Module
    packageModules = NM.fromList [m]
    packageMetadata :: PackageMetadata
    packageMetadata = PackageMetadata{..}
      where
        packageName :: PackageName
        packageName = PackageName "test"
        packageVersion :: PackageVersion
        packageVersion = PackageVersion "0.0"
        upgradedPackageId :: Maybe UpgradedPackageId
        upgradedPackageId = Nothing
