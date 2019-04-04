-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


{-# LANGUAGE OverloadedStrings #-}
-- | DAML-LF utility functions, may move to the LF utility if they are generally useful
module DA.Daml.GHC.Compiler.UtilLF where

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.Archive  as Archive

import qualified Data.ByteString.Char8      as BS
import qualified Data.NameMap               as NM
import           Data.Tagged
import qualified Data.Text                  as T
import           GHC.Stack                  (HasCallStack)

mkVar :: String -> ExprVarName
mkVar = Tagged . T.pack

mkVal :: String -> ExprValName
mkVal = Tagged . T.pack

mkTypeVar :: String -> TypeVarName
mkTypeVar = Tagged . T.pack

mkModName :: [String] -> ModuleName
mkModName = Tagged . map T.pack

mkField :: String -> FieldName
mkField = Tagged . T.pack

mkVariantCon :: String -> VariantConName
mkVariantCon = Tagged . T.pack

mkChoiceName :: String -> ChoiceName
mkChoiceName = Tagged . T.pack

mkTypeCon :: [String] -> TypeConName
mkTypeCon = Tagged . map T.pack

mkIdentity :: Type -> Expr
mkIdentity t = ETmLam (varV1, t) $ EVar varV1

varV1, varV2, varV3 :: ExprVarName
varV1 = mkVar "v1"
varV2 = mkVar "v2"
varV3 = mkVar "v3"

varT1 :: (TypeVarName, Kind)
varT1 = (Tagged $ T.pack "t1", KStar)

fromTCon :: HasCallStack => Type -> TypeConApp
fromTCon (TConApp con args) = TypeConApp con args
fromTCon t = error $ "fromTCon failed, " ++ show t

-- 'synthesizeVariantRecord' is used to synthesize, e.g., @T.C@ from the type
-- constructor @T@ and the variant constructor @C@.
synthesizeVariantRecord :: VariantConName -> TypeConName -> TypeConName
synthesizeVariantRecord dcon = fmap (++ [untag dcon])

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
      , dataCons = DataVariant [(conName, TUnit)]
      }
    valVoid = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal "void#", TCon (qual (dataTypeCon dataVoid)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EVariantCon
          { varTypeCon = TypeConApp (qual (dataTypeCon dataVoid)) []
          , varVariant = conName
          , varArg = EBuiltin (BEEnumCon ECUnit)
          }
      , dvalInfo = Nothing
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
      , dataCons = DataVariant [(mkVariantCon con, TUnit) | con <- cons]
      }
    valCtor con = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal ("$ctor:" ++ con), TCon (qual (dataTypeCon dataOrdering)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EVariantCon
          { varTypeCon = TypeConApp (qual (dataTypeCon dataOrdering)) []
          , varVariant = mkVariantCon con
          , varArg = EBuiltin (BEEnumCon ECUnit)
          }
      , dvalInfo = Nothing
      }
    dataProxy = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Proxy"]
      , dataSerializable = IsSerializable True
      , dataParams = [(Tagged "a", KStar)]
      , dataCons = DataRecord []
      }
    proxyCtor = DefValue
      { dvalLocation = Nothing
      , dvalBinder =
          ( mkVal "$ctor:Proxy"
          , TForall (Tagged "a", KStar) (TConApp (qual (dataTypeCon dataProxy)) [TVar (Tagged "a")])
          )
      , dvalNoPartyLiterals = HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = ETyLam
          (Tagged "a", KStar)
          (ERecCon (TypeConApp (qual (dataTypeCon dataProxy)) [TVar (Tagged "a")]) [])
      , dvalInfo = Nothing
      }

