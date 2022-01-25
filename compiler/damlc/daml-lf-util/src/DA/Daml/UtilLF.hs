-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


{-# OPTIONS_GHC -Wno-orphans #-}
-- | DAML-LF utility functions, may move to the LF utility if they are generally useful
module DA.Daml.UtilLF (
    module DA.Daml.UtilLF
    ) where

import           DA.Daml.LF.Ast
import           DA.Pretty (renderPretty)

import Data.Maybe
import qualified Data.NameMap               as NM
import qualified Data.Text                  as T
import           GHC.Stack                  (HasCallStack)
import Language.LSP.Types
import           Outputable (Outputable(..), text)

mkVar :: T.Text -> ExprVarName
mkVar = ExprVarName

mkVal :: T.Text -> ExprValName
mkVal = ExprValName

mkWorkerName :: T.Text -> ExprValName
mkWorkerName name = ExprValName ("$W" <> name)

mkSelectorName :: T.Text -> T.Text -> ExprValName
mkSelectorName ty sel = ExprValName ("$sel:" <> sel <> ":" <> ty)

mkTypeVar :: T.Text -> TypeVarName
mkTypeVar = TypeVarName

mkModName :: [T.Text] -> ModuleName
mkModName = ModuleName

mkField :: T.Text -> FieldName
mkField = FieldName

mkIndexedField :: Int -> FieldName
mkIndexedField i = mkField ("_" <> T.pack (show i))

mkSuperClassField :: Int -> FieldName
mkSuperClassField i = mkField ("s_" <> T.pack (show i))

mkClassMethodField :: T.Text -> FieldName
mkClassMethodField t = mkField ("m_" <> t)

mkVariantCon :: T.Text -> VariantConName
mkVariantCon = VariantConName

mkChoiceName :: T.Text -> ChoiceName
mkChoiceName = ChoiceName

mkTypeCon :: [T.Text] -> TypeConName
mkTypeCon = TypeConName

mkTypeSyn :: [T.Text] -> TypeSynName
mkTypeSyn = TypeSynName

mkIdentity :: Type -> Expr
mkIdentity t = ETmLam (varV1, t) $ EVar varV1

fieldToVar :: FieldName -> ExprVarName
fieldToVar = ExprVarName . unFieldName

varV1, varV2, varV3 :: ExprVarName
varV1 = mkVar "v1"
varV2 = mkVar "v2"
varV3 = mkVar "v3"

fromTCon :: HasCallStack => Type -> TypeConApp
fromTCon (TConApp con args) = TypeConApp con args
fromTCon t = error $ "fromTCon failed, " ++ show t

-- 'synthesizeVariantRecord' is used to synthesize, e.g., @T.C@ from the type
-- constructor @T@ and the variant constructor @C@.
synthesizeVariantRecord :: VariantConName -> TypeConName -> TypeConName
synthesizeVariantRecord (VariantConName dcon) (TypeConName tcon) = TypeConName (tcon ++ [dcon])

-- | Fails if there are any duplicate module names
buildPackage :: HasCallStack => Maybe PackageName -> Maybe PackageVersion -> Version -> [Module] -> Package
buildPackage mbPkgName mbPkgVersion version mods =
    Package version (NM.fromList mods) pkgMetadata
  where
    pkgMetadata =
        -- In `damlc build` we are guaranteed to have a name and version
        -- however, for `damlc package` (which should really die in a fire)
        -- we might only have a name and for `damlc compile` we don’t even
        -- have a package name <insert sad panda here>.
        -- We require metadata to be present in newer LF versions,
        -- so we set it to some arbitrarily chosen garbage.
        Just $ getPackageMetadata (fromMaybe (PackageName "unknown") mbPkgName) mbPkgVersion

instance Outputable Expr where
    ppr = text . renderPretty

sourceLocToRange :: SourceLoc -> Range
sourceLocToRange (SourceLoc _ slin scol elin ecol) =
  Range
    (Position
        (fromIntegral slin)
        (fromIntegral scol))
    (Position
        (fromIntegral elin)
        (fromIntegral ecol))

mkBuiltinEqual :: BuiltinType -> Expr
mkBuiltinEqual ty = EBuiltin BEEqualGeneric `ETyApp` TBuiltin ty

mkBuiltinLess :: BuiltinType -> Expr
mkBuiltinLess ty = EBuiltin BELessGeneric `ETyApp` TBuiltin ty

mkBuiltinGreater :: BuiltinType -> Expr
mkBuiltinGreater ty = EBuiltin BEGreaterGeneric `ETyApp` TBuiltin ty

preconditionFailedTypeCon :: Qualified TypeConName
preconditionFailedTypeCon = Qualified
    { qualPackage = PRImport (PackageId "f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78")
    , qualModule = ModuleName ["DA", "Exception", "PreconditionFailed"]
    , qualObject = TypeConName ["PreconditionFailed"]
    }

mkPreconditionFailed :: Expr -> Expr
mkPreconditionFailed msg = ERecCon
    { recTypeCon = TypeConApp preconditionFailedTypeCon []
    , recFields = [(FieldName "message", msg)]
    }
