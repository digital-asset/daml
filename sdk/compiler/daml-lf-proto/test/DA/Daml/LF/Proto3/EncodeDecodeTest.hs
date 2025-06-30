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
entry = defaultMain $ testGroup "All tests" [ rtt_tests ]

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
