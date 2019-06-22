-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
-- | DAML-LF utility functions, may move to the LF utility if they are generally useful
module DA.Daml.GHC.Compiler.UtilLF(
    module DA.Daml.GHC.Compiler.UtilLF
    ) where

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.Archive  as Archive
import           DA.Pretty (renderPretty)

import qualified Data.ByteString.Char8      as BS
import qualified Data.NameMap               as NM
import qualified Data.Text                  as T
import           GHC.Stack                  (HasCallStack)
import           Outputable

mkVar :: String -> ExprVarName
mkVar = ExprVarName . T.pack

mkVal :: String -> ExprValName
mkVal = ExprValName . T.pack

mkTypeVar :: String -> TypeVarName
mkTypeVar = TypeVarName . T.pack

mkModName :: [String] -> ModuleName
mkModName = ModuleName . map T.pack

mkField :: String -> FieldName
mkField = FieldName . T.pack

mkVariantCon :: String -> VariantConName
mkVariantCon = VariantConName . T.pack

mkChoiceName :: String -> ChoiceName
mkChoiceName = ChoiceName . T.pack

mkTypeCon :: [String] -> TypeConName
mkTypeCon = TypeConName . map T.pack

mkIdentity :: Type -> Expr
mkIdentity t = ETmLam (varV1, t) $ EVar varV1

fieldToVar :: FieldName -> ExprVarName
fieldToVar = ExprVarName . unFieldName

varV1, varV2, varV3 :: ExprVarName
varV1 = mkVar "v1"
varV2 = mkVar "v2"
varV3 = mkVar "v3"

varT1 :: (TypeVarName, Kind)
varT1 = (mkTypeVar "t1", KStar)

fromTCon :: HasCallStack => Type -> TypeConApp
fromTCon (TConApp con args) = TypeConApp con args
fromTCon t = error $ "fromTCon failed, " ++ show t

-- 'synthesizeVariantRecord' is used to synthesize, e.g., @T.C@ from the type
-- constructor @T@ and the variant constructor @C@.
synthesizeVariantRecord :: VariantConName -> TypeConName -> TypeConName
synthesizeVariantRecord (VariantConName dcon) (TypeConName tcon) = TypeConName (tcon ++ [dcon])

writeFileLf :: FilePath -> Package -> IO ()
writeFileLf outFile lfPackage = do
    BS.writeFile outFile $ Archive.encodeArchive lfPackage

-- | Fails if there are any duplicate module names
buildPackage :: HasCallStack => Maybe String -> Version -> [Module] -> Package
buildPackage mbPkgName version mods = Package version $ NM.fromList $ extraMods ++ mods
  where
    extraMods = case mbPkgName of
      Just "daml-prim" -> wiredInModules version
      _ -> []

wiredInModules :: Version -> [Module]
wiredInModules version =
  [ ghcPrim version
  , ghcTypes version
  ]

ghcPrim :: Version -> Module
ghcPrim version = Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleDataTypes = NM.fromList [dataVoid]
  , moduleValues = NM.fromList [valVoid]
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["GHC", "Prim"]
    qual = Qualified PRSelf modName
    conName = mkVariantCon "Void#"
    dataVoid = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Void#"]
      , dataSerializable = IsSerializable False
      , dataParams = []
      , dataCons = mkVariantOrEnumDataCons version [conName]
      }
    valVoid = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal "void#", TCon (qual (dataTypeCon dataVoid)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = mkVariantOrEnumCon version (qual (dataTypeCon dataVoid)) conName
      }

ghcTypes :: Version -> Module
ghcTypes version = Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleDataTypes = NM.fromList [dataOrdering, dataProxy]
  , moduleValues = NM.fromList (proxyCtor : map valCtor cons)
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["GHC", "Types"]
    qual = Qualified PRSelf modName
    cons = ["LT", "EQ", "GT"]
    dataOrdering = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Ordering"]
      , dataSerializable = IsSerializable True
      , dataParams = []
      , dataCons = mkVariantOrEnumDataCons version $ map mkVariantCon cons
      }
    valCtor con = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal ("$ctor:" ++ con), TCon (qual (dataTypeCon dataOrdering)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = mkVariantOrEnumCon version (qual (dataTypeCon dataOrdering)) (mkVariantCon con)
      }
    dataProxy = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Proxy"]
      , dataSerializable = IsSerializable True
      , dataParams = [(mkTypeVar "a", KStar)]
      , dataCons = DataRecord []
      }
    proxyCtor = DefValue
      { dvalLocation = Nothing
      , dvalBinder =
          ( mkVal "$ctor:Proxy"
          , TForall (mkTypeVar "a", KStar) (TConApp (qual (dataTypeCon dataProxy)) [TVar (mkTypeVar "a")])
          )
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = ETyLam
          (mkTypeVar "a", KStar)
          (ERecCon (TypeConApp (qual (dataTypeCon dataProxy)) [TVar (mkTypeVar "a")]) [])
      }

mkVariantOrEnumDataCons :: Version -> [VariantConName] -> DataCons
mkVariantOrEnumDataCons version cons
    | version `supports` featureEnumTypes = DataEnum cons
    | otherwise = DataVariant $ map (, TUnit) cons

mkVariantOrEnumCon :: Version -> Qualified TypeConName -> VariantConName -> Expr
mkVariantOrEnumCon version tcon con
    | version `supports` featureEnumTypes = EEnumCon
        { enumTypeCon = tcon
        , enumDataCon = con
        }
    | otherwise = EVariantCon
        { varTypeCon = TypeConApp tcon []
        , varVariant = con
        , varArg = EBuiltin BEUnit
        }

instance Outputable Expr where
    ppr = text . renderPretty
