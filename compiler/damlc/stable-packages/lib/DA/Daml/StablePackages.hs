-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.StablePackages
    ( allStablePackages
    , allStablePackagesForVersion
    , numStablePackagesForVersion
    , stablePackageByModuleName
    ) where

import Data.Bifunctor
import qualified Data.Map.Strict as MS
import qualified Data.NameMap as NM
import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Daml.LF.Proto3.Archive.Encode
import DA.Daml.UtilLF

allStablePackages :: MS.Map PackageId Package
allStablePackages =
    MS.fromList $
    map (\pkg  -> (encodePackageHash pkg, pkg))
    [ ghcTypes
    , ghcPrim
    , ghcTuple
    , daTypes
    , daInternalTemplate
    , daInternalAny
    , daTimeTypes
    , daNonEmptyTypes
    , daDateTypes
    , daSemigroupTypes
    , daMonoidTypes
    , daLogicTypes
    , daValidationTypes (encodePackageHash daNonEmptyTypes)
    , daInternalDown
    , daInternalErased
    , daInternalNatSyn
    , daInternalPromotedText
    , daSetTypes
    , daExceptionGeneralError
    , daExceptionArithmeticError
    , daExceptionAssertionFailed
    , daExceptionPreconditionFailed
    , daInternalInterfaceAnyViewTypes
    ]

allStablePackagesForVersion :: Version -> MS.Map PackageId Package
allStablePackagesForVersion v =
    MS.filter (\p -> packageLfVersion p <= v) allStablePackages

numStablePackagesForVersion :: Version -> Int
numStablePackagesForVersion v = MS.size (allStablePackagesForVersion v)

stablePackageByModuleName :: MS.Map ModuleName Package
stablePackageByModuleName = MS.fromListWithKey
    (\k -> error $ "Duplicate module among stable packages: " <> show k)
    [ (moduleName m, p)
    | p <- MS.elems allStablePackages
    , m <- NM.toList (packageModules p) ]

ghcTypes :: Package
ghcTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = NM.fromList [dataOrdering]
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
ghcPrim = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = NM.fromList [dataVoid]
  , moduleValues = NM.fromList [valVoid]
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
      , dvalIsTest = IsTest False
      , dvalBody = EEnumCon (qual (dataTypeCon dataVoid)) conName
      }

package :: Version -> NM.NameMap Module -> Package
package ver mods
    | ver > version1_7 = error "Packages with LF version >= 1.7 need to have package metadata"
    | otherwise = Package ver mods Nothing

daTypes :: Package
daTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
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
      [ DefValue Nothing (mkWorkerName "Left", mkTForalls eitherTyVars (TVar aTyVar :-> eitherTy)) (IsTest False) $
          mkETyLams eitherTyVars (ETmLam (mkVar "a", TVar aTyVar) (EVariantCon eitherTyConApp (mkVariantCon "Left") (EVar $ mkVar "a")))
      , DefValue Nothing (mkWorkerName "Right", mkTForalls eitherTyVars (TVar bTyVar :-> eitherTy)) (IsTest False) $
          mkETyLams eitherTyVars (ETmLam (mkVar "b", TVar bTyVar) (EVariantCon eitherTyConApp (mkVariantCon "Right") (EVar $ mkVar "b")))
      ]
    tupleTyVar i = mkTypeVar ("t" <> T.pack (show i))
    tupleTyVars n = [(tupleTyVar i, KStar) | i <- [1..n]]
    tupleTyName n = mkTypeCon ["Tuple" <> T.pack (show n)]
    tupleTyConApp n = TypeConApp (Qualified PRSelf modName (tupleTyName n)) (map (TVar . tupleTyVar) [1..n])
    tupleTy = typeConAppToType . tupleTyConApp
    tupleTmVar i = mkVar $ "a" <> T.pack (show i)
    tupleWorker n = DefValue Nothing (mkWorkerName $ "Tuple" <> T.pack (show n), mkTForalls (tupleTyVars n) (mkTFuns (map (TVar . tupleTyVar) [1..n]) $ tupleTy n)) (IsTest False) $
      mkETyLams (tupleTyVars n) $ mkETmLams [(tupleTmVar i, TVar $ tupleTyVar i) | i <- [1..n]] $
      ERecCon (tupleTyConApp n) [(mkIndexedField i, EVar $ tupleTmVar i) | i <- [1..n]]
    tupleWorkers = map tupleWorker [2..20]

ghcTuple :: Package
ghcTuple = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["GHC", "Tuple"]
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    unitTyCon = mkTypeCon ["Unit"]
    types = NM.fromList
      [ DefDataType Nothing unitTyCon (IsSerializable True) tyVars $
          DataRecord [(mkIndexedField 1, TVar tyVar)]
      ]
    values = NM.fromList
      [ mkWorkerDef modName unitTyCon tyVars [(mkIndexedField 1, TVar tyVar)]
      ]

daInternalTemplate :: Package
daInternalTemplate = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  }
  where
    modName = mkModName ["DA", "Internal", "Template"]
    types = NM.fromList
      [ DefDataType Nothing (mkTypeCon ["Archive"]) (IsSerializable True) [] $
          DataRecord []
      ]

daInternalAny :: Package
daInternalAny = package version1_7 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
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

daInternalInterfaceAnyViewTypes :: Package
daInternalInterfaceAnyViewTypes = Package
  { packageLfVersion = version1_15
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = datatypes
      }
  , packageMetadata = Just PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Internal-Interface-AnyView-Types"
      , packageVersion = PackageVersion "1.0.0"
      }
  }
  where
    modName = mkModName ["DA", "Internal", "Interface", "AnyView", "Types"]
    datatypes = NM.fromList
      [ DefDataType Nothing (mkTypeCon ["AnyView"]) (IsSerializable False) [] $
          DataRecord
            [ (mkField "getAnyView", TAny)
            , (mkField "getAnyViewInterfaceTypeRep", TCon (Qualified PRSelf modName (mkTypeCon ["InterfaceTypeRep"])))
            ]
      , DefDataType Nothing (mkTypeCon ["InterfaceTypeRep"]) (IsSerializable False) [] $
          DataRecord [(mkField "getInterfaceTypeRep", TTypeRep)]
      ]

daTimeTypes :: Package
daTimeTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "Time", "Types"]
    relTimeTyCon = mkTypeCon ["RelTime"]
    types = NM.fromList
      [ DefDataType Nothing relTimeTyCon (IsSerializable True) [] $
          DataRecord [(usField, TInt64)]
      ]
    values = NM.fromList
      [ mkSelectorDef modName relTimeTyCon [] usField TInt64
      , mkWorkerDef modName relTimeTyCon [] [(usField, TInt64)]
      ]
    usField = mkField "microseconds"

daNonEmptyTypes :: Package
daNonEmptyTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "NonEmpty", "Types"]
    hdField = mkField "hd"
    tlField = mkField "tl"
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    nonEmptyTyCon = mkTypeCon ["NonEmpty"]
    types = NM.fromList
      [ DefDataType Nothing nonEmptyTyCon (IsSerializable True) tyVars $
          DataRecord [(hdField, TVar tyVar), (tlField, TList (TVar tyVar))]
      ]
    values = NM.fromList
      [ mkWorkerDef modName nonEmptyTyCon tyVars [(hdField, TVar tyVar), (tlField, TList (TVar tyVar))]
      , mkSelectorDef modName nonEmptyTyCon tyVars hdField (TVar tyVar)
      , mkSelectorDef modName nonEmptyTyCon tyVars tlField (TList (TVar tyVar))
      ]

daDateTypes :: Package
daDateTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  }
  where
    modName = mkModName ["DA", "Date", "Types"]
    types = NM.fromList
      [ DefDataType Nothing (mkTypeCon ["DayOfWeek"]) (IsSerializable True) [] $
          DataEnum $ map mkVariantCon
            [ "Monday"
            , "Tuesday"
            , "Wednesday"
            , "Thursday"
            , "Friday"
            , "Saturday"
            , "Sunday"
            ]
      , DefDataType Nothing (mkTypeCon ["Month"]) (IsSerializable True) [] $
          DataEnum $ map mkVariantCon
            [ "Jan"
            , "Feb"
            , "Mar"
            , "Apr"
            , "May"
            , "Jun"
            , "Jul"
            , "Aug"
            , "Sep"
            , "Oct"
            , "Nov"
            , "Dec"
            ]
      ]

daSemigroupTypes :: Package
daSemigroupTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "Semigroup", "Types"]
    unpackField = mkField "unpack"
    minTyCon = mkTypeCon ["Min"]
    maxTyCon = mkTypeCon ["Max"]
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    types = NM.fromList
      [ DefDataType Nothing minTyCon (IsSerializable True) tyVars $ DataRecord [(unpackField, TVar tyVar)]
      , DefDataType Nothing maxTyCon (IsSerializable True) tyVars $ DataRecord [(unpackField, TVar tyVar)]
      ]
    values = NM.fromList
      [ mkWorkerDef modName minTyCon tyVars [(unpackField, TVar tyVar)]
      , mkWorkerDef modName maxTyCon tyVars [(unpackField, TVar tyVar)]
      ]

daMonoidTypes :: Package
daMonoidTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "Monoid", "Types"]
    unpackField = mkField "unpack"
    allTyCon = mkTypeCon ["All"]
    anyTyCon = mkTypeCon ["Any"]
    endoTyCon = mkTypeCon ["Endo"]
    sumTyCon = mkTypeCon ["Sum"]
    productTyCon = mkTypeCon ["Product"]
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    getAllField = mkField "getAll"
    getAnyField = mkField "getAny"
    appEndoField = mkField "appEndo"
    types = NM.fromList
      [ DefDataType Nothing allTyCon (IsSerializable True) [] $ DataRecord [(getAllField, TBool)]
      , DefDataType Nothing anyTyCon (IsSerializable True) [] $ DataRecord [(getAnyField, TBool)]
      , DefDataType Nothing endoTyCon (IsSerializable False) tyVars $ DataRecord [(appEndoField, TVar tyVar :-> TVar tyVar)]
      , DefDataType Nothing sumTyCon (IsSerializable True) tyVars $ DataRecord [(unpackField, TVar tyVar)]
      , DefDataType Nothing productTyCon (IsSerializable True) tyVars $ DataRecord [(unpackField, TVar tyVar)]
      ]
    values = NM.fromList
      [ mkSelectorDef modName allTyCon [] getAllField TBool
      , mkSelectorDef modName anyTyCon [] getAnyField TBool
      , mkSelectorDef modName endoTyCon tyVars appEndoField (TVar tyVar :-> TVar tyVar)
      , mkWorkerDef modName allTyCon [] [(getAllField, TBool)]
      , mkWorkerDef modName anyTyCon [] [(getAnyField, TBool)]
      , mkWorkerDef modName endoTyCon tyVars [(appEndoField, TVar tyVar :-> TVar tyVar)]
      , mkWorkerDef modName sumTyCon tyVars [(unpackField, TVar tyVar)]
      , mkWorkerDef modName productTyCon tyVars [(unpackField, TVar tyVar)]
      ]

daValidationTypes :: PackageId -> Package
daValidationTypes nonEmptyPkgId = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    nonEmptyModName = mkModName ["DA", "NonEmpty", "Types"]
    nonEmptyTCon = Qualified (PRImport nonEmptyPkgId) nonEmptyModName (mkTypeCon ["NonEmpty"])
    modName = mkModName ["DA", "Validation", "Types"]
    validationTyCon = mkTypeCon ["Validation"]
    errors = mkVariantCon "Errors"
    success = mkVariantCon "Success"
    errsTyVar = mkTypeVar "errs"
    tyVar = mkTypeVar "a"
    tyVars = [(errsTyVar, KStar), (tyVar, KStar)]
    types = NM.fromList
      [ DefDataType Nothing validationTyCon (IsSerializable True) tyVars $ DataVariant
          [ (errors, TApp (TCon nonEmptyTCon) (TVar errsTyVar))
          , (success, TVar tyVar)
          ]
      ]
    values = NM.fromList
      [ mkVariantWorkerDef modName validationTyCon errors tyVars (TApp (TCon nonEmptyTCon) (TVar errsTyVar))
      , mkVariantWorkerDef modName validationTyCon success tyVars (TVar tyVar)
      ]

daLogicTypes :: Package
daLogicTypes = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "Logic", "Types"]
    formulaTyCon = mkTypeCon ["Formula"]
    proposition = mkVariantCon "Proposition"
    negation = mkVariantCon "Negation"
    conjunction = mkVariantCon "Conjunction"
    disjunction = mkVariantCon "Disjunction"
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    formulaTy = TApp (TCon $ Qualified PRSelf modName formulaTyCon) (TVar tyVar)
    types = NM.fromList
      [ DefDataType Nothing formulaTyCon (IsSerializable True) tyVars $ DataVariant
          [ (proposition, TVar tyVar)
          , (negation, formulaTy)
          , (conjunction, TList formulaTy)
          , (disjunction, TList formulaTy)
          ]
      ]
    values = NM.fromList
      [ mkVariantWorkerDef modName formulaTyCon proposition tyVars (TVar tyVar)
      , mkVariantWorkerDef modName formulaTyCon negation tyVars formulaTy
      , mkVariantWorkerDef modName formulaTyCon conjunction tyVars (TList formulaTy)
      , mkVariantWorkerDef modName formulaTyCon disjunction tyVars (TList formulaTy)
      ]

daInternalDown :: Package
daInternalDown = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  , moduleValues = values
  }
  where
    modName = mkModName ["DA", "Internal", "Down"]
    downTyCon = mkTypeCon ["Down"]
    tyVar = mkTypeVar "a"
    tyVars = [(tyVar, KStar)]
    unpackField = mkField "unpack"
    types = NM.fromList
      [ DefDataType Nothing downTyCon (IsSerializable True) tyVars $ DataRecord [(unpackField, TVar tyVar)]
      ]
    values = NM.fromList
      [ mkWorkerDef modName downTyCon tyVars [(unpackField, TVar tyVar)]
      ]

daSetTypes :: Package
daSetTypes = Package
    { packageLfVersion = version1_11
    , packageModules = NM.singleton (emptyModule modName)
        { moduleDataTypes = types
        , moduleValues = values
        }
    , packageMetadata = Just PackageMetadata
        { packageName = PackageName "daml-stdlib-DA-Set-Types"
        , packageVersion = PackageVersion "1.0.0"
        }
    }
  where
    modName = mkModName ["DA", "Set", "Types"]
    tyCon = mkTypeCon ["Set"]
    tyVar = mkTypeVar "k"
    tyVars = [(tyVar, KStar)]
    mapField = mkField "map"
    mapType = TGenMap (TVar tyVar) TUnit
    types = NM.fromList
      [ DefDataType Nothing tyCon (IsSerializable True) tyVars $ DataRecord [(mapField, mapType)]
      ]
    values = NM.fromList
      [ mkWorkerDef modName tyCon tyVars [(mapField, mapType)]
      ]

daInternalErased :: Package
daInternalErased = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  }
  where
    modName = mkModName ["DA", "Internal", "Erased"]
    erasedTyCon = mkTypeCon ["Erased"]
    types = NM.fromList
      [ DefDataType Nothing erasedTyCon (IsSerializable False) [] $ DataVariant []
      ]

daInternalNatSyn :: Package
daInternalNatSyn = Package
  { packageLfVersion = version1_14
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = types
      , moduleValues = NM.empty
      }
  , packageMetadata = Just PackageMetadata
      { packageName = PackageName "daml-prim-DA-Internal-NatSyn"
      , packageVersion = PackageVersion "1.0.0"
      }
  }
  where
    modName = mkModName ["DA", "Internal", "NatSyn"]
    natSynTyCon = mkTypeCon ["NatSyn"]
    types = NM.fromList
      [ DefDataType Nothing natSynTyCon (IsSerializable False) [(mkTypeVar "n", KNat)] $ DataVariant []
      ]

daInternalPromotedText :: Package
daInternalPromotedText = package version1_6 $ NM.singleton (emptyModule modName)
  { moduleDataTypes = types
  }
  where
    modName = mkModName ["DA", "Internal", "PromotedText"]
    ptextTyCon = mkTypeCon ["PromotedText"]
    types = NM.fromList
      [ DefDataType Nothing ptextTyCon (IsSerializable False) [(mkTypeVar "t", KStar)] $ DataVariant []
      ]

daExceptionGeneralError :: Package
daExceptionGeneralError = builtinExceptionPackage "GeneralError"

daExceptionArithmeticError :: Package
daExceptionArithmeticError = builtinExceptionPackage "ArithmeticError"

daExceptionAssertionFailed :: Package
daExceptionAssertionFailed = builtinExceptionPackage "AssertionFailed"

daExceptionPreconditionFailed :: Package
daExceptionPreconditionFailed = builtinExceptionPackage "PreconditionFailed"

builtinExceptionPackage :: T.Text -> Package
builtinExceptionPackage name = Package
    { packageLfVersion = featureMinVersion featureExceptions
    , packageModules = NM.singleton (emptyModule modName)
        { moduleDataTypes = types
        , moduleValues = values
        , moduleExceptions = exceptions
        }
    , packageMetadata = Just PackageMetadata
        { packageName = PackageName ("daml-prim-DA-Exception-" <> name)
        , packageVersion = PackageVersion "1.0.0"
        }
    }
  where
    modName = mkModName ["DA", "Exception", name]
    tyCon = mkTypeCon [name]
    tyVars = []
    fieldName = mkField "message"
    fieldType = TText
    fields = [(fieldName, fieldType)]
    types = NM.singleton (DefDataType Nothing tyCon (IsSerializable True) tyVars (DataRecord fields))
    values = NM.singleton (mkWorkerDef modName tyCon tyVars fields)
    var = mkVar "x"
    qualify = Qualified PRSelf modName
    exceptions = NM.singleton DefException
        { exnLocation = Nothing
        , exnName = tyCon
        , exnMessage =
            ETmLam (var, TCon (qualify tyCon))
                (ERecProj (TypeConApp (qualify tyCon) []) fieldName (EVar var))
        }


mkSelectorDef :: ModuleName -> TypeConName -> [(TypeVarName, Kind)] -> FieldName -> Type -> DefValue
mkSelectorDef modName tyCon tyVars fieldName fieldTy =
    DefValue Nothing (mkSelectorName (T.intercalate "." $ unTypeConName tyCon) (unFieldName fieldName), mkTForalls tyVars (ty :-> fieldTy)) (IsTest False) $
      mkETyLams tyVars $ mkETmLams [(mkVar "x", ty)] $ ERecProj tyConApp fieldName (EVar $ mkVar "x")
  where tyConApp = TypeConApp (Qualified PRSelf modName tyCon) (map (TVar . fst) tyVars)
        ty = typeConAppToType tyConApp

mkWorkerDef :: ModuleName -> TypeConName -> [(TypeVarName, Kind)] -> [(FieldName, Type)] -> DefValue
mkWorkerDef modName tyCon tyVars fields =
    DefValue Nothing (mkWorkerName (T.intercalate "." $ unTypeConName tyCon), mkTForalls tyVars $ mkTFuns (map snd fields) ty) (IsTest False) $
      mkETyLams tyVars $ mkETmLams (map (first (mkVar . unFieldName)) fields) $ ERecCon tyConApp (map (\(field, _) -> (field, EVar $ mkVar $ unFieldName field)) fields)
  where tyConApp = TypeConApp (Qualified PRSelf modName tyCon) (map (TVar . fst) tyVars)
        ty = typeConAppToType tyConApp

mkVariantWorkerDef :: ModuleName -> TypeConName -> VariantConName -> [(TypeVarName, Kind)] -> Type -> DefValue
mkVariantWorkerDef modName tyCon constr tyVars argTy =
    DefValue Nothing (mkWorkerName (unVariantConName constr), mkTForalls tyVars $ argTy :-> ty) (IsTest False) $
      mkETyLams tyVars $ mkETmLams [(mkVar "x", argTy)] $ EVariantCon tyConApp constr (EVar $ mkVar "x")
  where tyConApp = TypeConApp (Qualified PRSelf modName tyCon) (map (TVar . fst) tyVars)
        ty = typeConAppToType tyConApp

emptyModule :: ModuleName -> Module
emptyModule name = Module
  { moduleName = name
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleSynonyms = NM.empty
  , moduleDataTypes = NM.empty
  , moduleValues = NM.empty
  , moduleTemplates = NM.empty
  , moduleExceptions = NM.empty
  , moduleInterfaces = NM.empty
  }
