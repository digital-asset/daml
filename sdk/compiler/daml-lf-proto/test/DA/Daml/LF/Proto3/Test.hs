-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-missing-signatures #-}

module DA.Daml.LF.Proto3.Test (
        module DA.Daml.LF.Proto3.Test
) where


import qualified Data.NameMap as NM

import           DA.Daml.LF.Proto3.EncodeV2
import           DA.Daml.LF.Proto3.DecodeV2

import           DA.Daml.LF.Ast
import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P

import           Text.Pretty.Simple

entry :: IO ()
entry = return ()

------------------------------------------------------------------------
-- Params
------------------------------------------------------------------------

version :: Version
version = Version V2 PointDev

------------------------------------------------------------------------
-- Encoding
------------------------------------------------------------------------

encodeTest :: Int -> P.Package
encodeTest 0 = encodePackage $ oneModulePackage emptyModule
encodeTest 1 = encodePackage $ oneModulePackage tyLamModule
encodeTest _ = error "arg out of bounds"

encodeTest' i = pPrint $ encodeTest i


emptyModule :: Module
emptyModule = Module{..}
  where
    moduleName :: ModuleName
    moduleName = ModuleName ["test"]
    moduleSource :: (Maybe FilePath)
    moduleSource = Nothing
    moduleFeatureFlags :: FeatureFlags
    moduleFeatureFlags = FeatureFlags --what is this unit-like type?
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
    packageLfVersion = version
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

------------------------------------------------------------------------
-- Decoding
------------------------------------------------------------------------

decode :: P.Package -> Either Error Package
decode = decodePackage version SelfPackageId

-- decode' = decodePackage' version SelfPackageId
