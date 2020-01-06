-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module GenerateStablePackage (main) where

import qualified Data.ByteString as BS
import qualified Data.NameMap as NM
import Options.Applicative
import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Daml.LF.Proto3.Archive
import DA.Daml.LFConversion.UtilLF

data Opts = Opts
  { optModule :: ModuleName
  -- ^ The module that we generate as a standalone package
  , optModuleDeps :: [ModuleDep]
  -- ^ Dependencies of this module, i.e., modules that we reference.
  -- We don’t want to hardcode package ids in here
  -- (even though we could since they must be stable)
  -- so we require that users pass in a mapping from module names
  -- to package ids.
  , optOutputPath :: FilePath
  } deriving Show

data ModuleDep = ModuleDep
  { depModuleName :: ModuleName
  , depPackageId :: PackageId
  } deriving Show

optParser :: Parser Opts
optParser =
  Opts
    <$> option modNameReader (long "module")
    <*> many (option modDepReader (long "module-dep" <> help "Module.Name:packageid"))
    <*> option str (short 'o')
  where
    modNameReader = maybeReader (Just . ModuleName . T.splitOn "." . T.pack)
    modDepReader = maybeReader $ \s ->
      case T.splitOn ":" (T.pack s) of
        [modName, packageId] -> Just ModuleDep
          { depModuleName = ModuleName (T.splitOn "." modName)
          , depPackageId = PackageId packageId
          }
        _ -> Nothing

main :: IO ()
main = do
  Opts{..} <- execParser (info optParser idm)
  case optModule of
    ModuleName ["GHC", "Types"] ->
      writePackage ghcTypes optOutputPath
    ModuleName ["GHC", "Prim"] ->
      writePackage ghcPrim optOutputPath
    ModuleName ["GHC", "Tuple"] ->
      writePackage ghcTuple optOutputPath
    ModuleName ["DA", "Types"] ->
      writePackage daTypes optOutputPath
    ModuleName ["DA", "Internal", "Template"] ->
      writePackage daInternalTemplate optOutputPath
    ModuleName ["DA", "Internal", "Any"] ->
      writePackage daInternalAny optOutputPath
    _ -> fail $ "Unknown module: " <> show optModule

writePackage :: Package -> FilePath -> IO ()
writePackage pkg path = do
  BS.writeFile path $ encodeArchive pkg

ghcTypes :: Package
ghcTypes = Package version1_6 $ NM.singleton Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleSynonyms = NM.empty
  , moduleDataTypes = NM.fromList [dataOrdering]
  , moduleValues = NM.empty
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["GHC", "Types"]
    cons = ["LT", "EQ", "GT"]
    dataOrdering = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Ordering"]
      , dataSerializable = IsSerializable True
      , dataParams = []
      , dataCons = DataEnum $ map mkVariantCon cons
      }

ghcPrim :: Package
ghcPrim = Package version1_6 $ NM.singleton Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleSynonyms = NM.empty
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

daTypes :: Package
daTypes = Package version1_6 $ NM.singleton Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleTemplates = NM.empty
  , moduleSynonyms = NM.empty
  , moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "Types"]
    types = NM.fromList $
      (DefDataType Nothing (mkTypeCon ["Either"]) (IsSerializable True) eitherTyVars $
         DataVariant [(mkVariantCon "Left", TVar aTyVar), (mkVariantCon "Right", TVar bTyVar)]
      ) : map tupleN [2..20]
    tupleN n = DefDataType
      Nothing
      (tupleTyName n)
      (IsSerializable True)
      [(tupleTyVar i, KStar) | i <- [1..n]]
      (DataRecord [(mkIndexedField i, TVar (tupleTyVar i)) | i <- [1..n]])
    aTyVar = mkTypeVar "a"
    bTyVar = mkTypeVar "b"
    eitherTyVars = [(aTyVar, KStar), (bTyVar, KStar)]
    eitherTyConApp = TypeConApp (Qualified PRSelf modName (mkTypeCon ["Either"])) [TVar aTyVar, TVar bTyVar]
    eitherTy = typeConAppToType eitherTyConApp
    values = NM.fromList $ eitherWorkers ++ tupleWorkers
    eitherWorkers =
      [ DefValue Nothing (mkWorkerName "Left", mkTForalls eitherTyVars (TVar aTyVar :-> eitherTy)) (HasNoPartyLiterals True) (IsTest False) $
          mkETyLams eitherTyVars (ETmLam (mkVar "a", TVar aTyVar) (EVariantCon eitherTyConApp (mkVariantCon "Left") (EVar $ mkVar "a")))
      , DefValue Nothing (mkWorkerName "Right", mkTForalls eitherTyVars (TVar bTyVar :-> eitherTy)) (HasNoPartyLiterals True) (IsTest False) $
          mkETyLams eitherTyVars (ETmLam (mkVar "b", TVar bTyVar) (EVariantCon eitherTyConApp (mkVariantCon "Right") (EVar $ mkVar "b")))
      ]
    tupleTyVar i = mkTypeVar ("t" <> T.pack (show i))
    tupleTyVars n = [(tupleTyVar i, KStar) | i <- [1..n]]
    tupleTyName n = mkTypeCon ["Tuple" <> T.pack (show n)]
    tupleTyConApp n = TypeConApp (Qualified PRSelf modName (tupleTyName n)) (map (TVar . tupleTyVar) [1..n])
    tupleTy = typeConAppToType . tupleTyConApp
    tupleTmVar i = mkVar $ "a" <> T.pack (show i)
    tupleWorker n = DefValue Nothing (mkWorkerName $ "Tuple" <> T.pack (show n), mkTForalls (tupleTyVars n) (mkTFuns (map (TVar . tupleTyVar) [1..n]) $ tupleTy n)) (HasNoPartyLiterals True) (IsTest False) $
      mkETyLams (tupleTyVars n) $ mkETmLams [(tupleTmVar i, TVar $ tupleTyVar i) | i <- [1..n]] $
      ERecCon (tupleTyConApp n) [(mkIndexedField i, EVar $ tupleTmVar i) | i <- [1..n]]
    tupleWorkers = map tupleWorker [2..20]

ghcTuple :: Package
ghcTuple = Package version1_6 $ NM.singleton Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleTemplates = NM.empty
  , moduleSynonyms = NM.empty
  , moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["GHC", "Tuple"]
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    unitTyCon = mkTypeCon ["Unit"]
    unitTyConApp = TypeConApp (Qualified PRSelf modName unitTyCon) [TVar tyVar]
    unitTy = typeConAppToType unitTyConApp
    types = NM.fromList
      [ DefDataType Nothing unitTyCon (IsSerializable True) tyVars $
          DataRecord [(mkIndexedField 1, TVar tyVar)]
      ]
    values = NM.fromList
      [ DefValue Nothing (mkWorkerName "Unit", mkTForalls tyVars (TVar tyVar :-> unitTy)) (HasNoPartyLiterals True) (IsTest False) $
          mkETyLams tyVars $ mkETmLams [(mkVar "a", TVar tyVar)] (ERecCon unitTyConApp [(mkIndexedField 1, EVar $ mkVar "a")])
      ]

daInternalTemplate :: Package
daInternalTemplate = Package version1_6 $ NM.singleton Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleSynonyms = NM.empty
  , moduleDataTypes = types
  , moduleValues = NM.fromList []
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["DA", "Internal", "Template"]
    types = NM.fromList
      [ DefDataType Nothing (mkTypeCon ["Archive"]) (IsSerializable True) [] $
          DataRecord []
      ]

daInternalAny :: Package
daInternalAny = Package version1_7 $ NM.singleton Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleSynonyms = NM.empty
  , moduleDataTypes = types
  , moduleValues = NM.fromList []
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["DA", "Internal", "Any"]
    types = NM.fromList
      [ DefDataType Nothing (mkTypeCon ["AnyTemplate"]) (IsSerializable False) [] $
          DataRecord [(mkField "getAnyTemplate", TAny)]
      , DefDataType Nothing (mkTypeCon ["TemplateTypeRep"]) (IsSerializable False) [] $
          DataRecord [(mkField "getTemplateTypeRep", TTypeRep)]
      , DefDataType Nothing (mkTypeCon ["AnyChoice"]) (IsSerializable False) [] $
          DataRecord [(mkField "getAnyChoice", TAny), (mkField "getAnyChoiceTemplateTypeRep", TCon (Qualified PRSelf modName (mkTypeCon ["TemplateTypeRep"])))]
      , DefDataType Nothing (mkTypeCon ["AnyContractKey"]) (IsSerializable False) [] $
          DataRecord [(mkField "getAnyContractKey", TAny), (mkField "getAnyContractKeyTemplateTypeRep", TCon (Qualified PRSelf modName (mkTypeCon ["TemplateTypeRep"])))]
      ]
