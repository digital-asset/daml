-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


{-# OPTIONS_GHC -Wno-orphans #-}
-- | DAML-LF utility functions, may move to the LF utility if they are generally useful
module DA.Daml.LFConversion.UtilLF(
    module DA.Daml.LFConversion.UtilLF
    ) where

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.Archive  as Archive
import           DA.Pretty (renderPretty)

import qualified Data.ByteString.Char8      as BS
import qualified Data.NameMap               as NM
import qualified Data.Text                  as T
import           GHC.Stack                  (HasCallStack)
import Language.Haskell.LSP.Types
import           Outputable (Outputable(..), text)

mkVar :: T.Text -> ExprVarName
mkVar = ExprVarName

mkVal :: T.Text -> ExprValName
mkVal = ExprValName

mkTypeVar :: T.Text -> TypeVarName
mkTypeVar = TypeVarName

mkModName :: [T.Text] -> ModuleName
mkModName = ModuleName

mkField :: T.Text -> FieldName
mkField = FieldName

mkVariantCon :: T.Text -> VariantConName
mkVariantCon = VariantConName

mkChoiceName :: T.Text -> ChoiceName
mkChoiceName = ChoiceName

mkTypeCon :: [T.Text] -> TypeConName
mkTypeCon = TypeConName

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
      Just "daml-prim" -> wiredInModules
      _ -> []

wiredInModules :: [Module]
wiredInModules =
  [ ghcPrim
  , ghcTypes
  ]

ghcPrim :: Module
ghcPrim = Module
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
      , dataCons = DataEnum [conName]
      }
    valVoid = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal "void#", TCon (qual (dataTypeCon dataVoid)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EEnumCon (qual (dataTypeCon dataVoid)) conName
      }

ghcTypes :: Module
ghcTypes = Module
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
      , dataCons = DataEnum $ map mkVariantCon cons
      }
    valCtor con = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal ("$ctor:" <> con), TCon (qual (dataTypeCon dataOrdering)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EEnumCon (qual (dataTypeCon dataOrdering)) (mkVariantCon con)
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

instance Outputable Expr where
    ppr = text . renderPretty

sourceLocToRange :: SourceLoc -> Range
sourceLocToRange (SourceLoc _ slin scol elin ecol) =
  Range (Position slin scol) (Position elin ecol)
