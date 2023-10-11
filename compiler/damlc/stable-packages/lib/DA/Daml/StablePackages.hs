-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

allV1StablePackages :: MS.Map PackageId Package
allV1StablePackages =
    MS.fromList $
    map (\pkg  -> (encodePackageHash pkg, pkg))
    [ ghcTypes version1_6
    , ghcPrim version1_6
    , ghcTuple version1_6
    , daTypes version1_6
    , daInternalTemplate version1_6
    , daInternalAny version1_7
    , daTimeTypes version1_6
    , daNonEmptyTypes version1_6
    , daDateTypes version1_6
    , daSemigroupTypes version1_6
    , daMonoidTypes version1_6
    , daLogicTypes version1_6
    , daValidationTypes version1_6 (encodePackageHash (daNonEmptyTypes version1_6))
    , daInternalDown version1_6
    , daInternalErased version1_6
    , daInternalNatSyn version1_14
    , daInternalPromotedText version1_6
    , daSetTypes version1_11
    , daExceptionGeneralError version1_14
    , daExceptionArithmeticError version1_14
    , daExceptionAssertionFailed version1_14
    , daExceptionPreconditionFailed version1_14
    , daInternalInterfaceAnyViewTypes version1_15
    , daActionStateType version1_14 (encodePackageHash (daTypes version1_6))
    , daRandomTypes version1_14
    , daStackTypes version1_14
    ]

-- TODO(#17366): use version2_0 everywhere once we introduce LF 2.0
allV2StablePackages :: MS.Map PackageId Package
allV2StablePackages =
    MS.fromList $
    map (\pkg  -> (encodePackageHash pkg, pkg))
      [ ghcTypes version2_dev
      , ghcPrim version2_dev
      , ghcTuple version2_dev
      , daTypes version2_dev
      , daInternalTemplate version2_dev
      , daInternalAny version2_dev
      , daTimeTypes version2_dev
      , daNonEmptyTypes version2_dev
      , daDateTypes version2_dev
      , daSemigroupTypes version2_dev
      , daMonoidTypes version2_dev
      , daLogicTypes version2_dev
      , daValidationTypes version2_dev (encodePackageHash (daNonEmptyTypes version2_dev))
      , daInternalDown version2_dev
      , daInternalErased version2_dev
      , daInternalNatSyn version2_dev
      , daInternalPromotedText version2_dev
      , daSetTypes version2_dev
      , daExceptionGeneralError version2_dev
      , daExceptionArithmeticError version2_dev
      , daExceptionAssertionFailed version2_dev
      , daExceptionPreconditionFailed version2_dev
      , daInternalInterfaceAnyViewTypes version2_dev
      , daActionStateType version2_dev (encodePackageHash (daTypes version2_dev))
      , daRandomTypes version2_dev
      , daStackTypes version2_dev
      ]

allStablePackages :: MS.Map PackageId Package
allStablePackages =
    MS.unionWithKey
        duplicatePackageIdError
        allV1StablePackages
        allV2StablePackages
  where
    duplicatePackageIdError pkgId _ _ =
        error $ "duplicate package ID among stable packages: " <> show pkgId

allStablePackagesForMajorVersion :: MajorVersion -> MS.Map PackageId Package
allStablePackagesForMajorVersion = \case
    V1 -> allV1StablePackages
    V2 -> allV2StablePackages

allStablePackagesForVersion :: Version -> MS.Map PackageId Package
allStablePackagesForVersion v =
    MS.filter
        (\p -> v `canDependOn` packageLfVersion p)
        (allStablePackagesForMajorVersion (versionMajor v))

numStablePackagesForVersion :: Version -> Int
numStablePackagesForVersion v = MS.size (allStablePackagesForVersion v)

stablePackageByModuleName :: MS.Map (MajorVersion, ModuleName) (PackageId, Package)
stablePackageByModuleName = MS.fromListWithKey
    (\k -> error $ "Duplicate module among stable packages: " <> show k)
    [ ((major, moduleName m), (pid, p))
    | major <- [minBound .. maxBound]
    , (pid, p) <- MS.toList (allStablePackagesForMajorVersion major)
    , m <- NM.toList (packageModules p)
    ]

-- | Helper function for optionally adding metadata to stable packages depending
-- on the LF version of the package.
ifMetadataRequired :: Version -> PackageMetadata -> Maybe PackageMetadata
ifMetadataRequired v metadata =
  if requiresPackageMetadata v
    then Just metadata
    else Nothing

ghcTypes :: Version -> Package
ghcTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = NM.fromList [dataOrdering]
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-GHC-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

ghcPrim :: Version -> Package
ghcPrim version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = NM.fromList [dataVoid]
    , moduleValues = NM.fromList [valVoid]
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-GHC-Prim"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daTypes :: Version -> Package
daTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-DA-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

ghcTuple :: Version -> Package
ghcTuple version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-GHC-Tuple"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daInternalTemplate :: Version -> Package
daInternalTemplate version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
     { moduleDataTypes = types
     }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "ghc-stdlib-DA-Internal-Template"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Internal", "Template"]
    types = NM.fromList
      [ DefDataType Nothing (mkTypeCon ["Archive"]) (IsSerializable True) [] $
          DataRecord []
      ]

daInternalAny :: Version -> Package
daInternalAny version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "ghc-stdlib-DA-Internal-Any"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daInternalInterfaceAnyViewTypes :: Version -> Package
daInternalInterfaceAnyViewTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = datatypes
      , moduleValues = values
      }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Internal-Interface-AnyView-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Internal", "Interface", "AnyView", "Types"]

    anyViewTyCon = mkTypeCon ["AnyView"]
    getAnyViewField = mkField "getAnyView"
    getAnyViewInterfaceTypeRepField = mkField "getAnyViewInterfaceTypeRep"
    interfaceTypeRepType = TCon (Qualified PRSelf modName (mkTypeCon ["InterfaceTypeRep"]))

    interfaceTypeRepTyCon = mkTypeCon ["InterfaceTypeRep"]
    getInterfaceTypeRepField = mkField "getInterfaceTypeRep"

    datatypes = NM.fromList
      [ DefDataType Nothing anyViewTyCon (IsSerializable False) [] $
          DataRecord
            [ (getAnyViewField, TAny)
            , (getAnyViewInterfaceTypeRepField, interfaceTypeRepType)
            ]
      , DefDataType Nothing interfaceTypeRepTyCon (IsSerializable False) [] $
          DataRecord [(getInterfaceTypeRepField, TTypeRep)]
      ]
    values = NM.fromList
      [ mkSelectorDef modName anyViewTyCon [] getAnyViewField TAny
      , mkSelectorDef modName anyViewTyCon [] getAnyViewInterfaceTypeRepField interfaceTypeRepType
      , mkWorkerDef modName anyViewTyCon [] [(getAnyViewField, TAny), (getAnyViewInterfaceTypeRepField, interfaceTypeRepType)]
      , mkSelectorDef modName interfaceTypeRepTyCon [] getInterfaceTypeRepField TTypeRep
      , mkWorkerDef modName interfaceTypeRepTyCon [] [(getInterfaceTypeRepField, TTypeRep)]
      ]

daActionStateType :: Version -> PackageId -> Package
daActionStateType version daTypesPackageId = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = types
      , moduleValues = values
      }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Action-State-Type"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Action", "State", "Type"]

    tuple2QualTyCon = Qualified
      { qualPackage = PRImport daTypesPackageId
      , qualModule = mkModName ["DA", "Types"]
      , qualObject = mkTypeCon ["Tuple2"]
      }

    stateTyCon = mkTypeCon ["State"]
    sTyVar = mkTypeVar "s"
    aTyVar = mkTypeVar "a"
    tyVars = [(sTyVar, KStar), (aTyVar, KStar)]
    runStateField = mkField "runState"
    runStateFieldType =
      TVar sTyVar :->
        TCon tuple2QualTyCon `TApp` TVar aTyVar `TApp` TVar sTyVar

    types = NM.fromList
      [ DefDataType
          { dataLocation = Nothing
          , dataTypeCon = stateTyCon
          , dataSerializable = IsSerializable False
          , dataParams = tyVars
          , dataCons = DataRecord [(runStateField, runStateFieldType)]
          }
      ]
    values = NM.fromList
      [ mkSelectorDef modName stateTyCon tyVars runStateField runStateFieldType
      , mkWorkerDef modName stateTyCon tyVars [(runStateField, runStateFieldType)]
      ]

daRandomTypes :: Version -> Package
daRandomTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = types
      , moduleValues = values
      }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Random-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Random", "Types"]
    minstdTyCon = mkTypeCon ["Minstd"]
    minstdDataCon = mkVariantCon "Minstd"
    types = NM.fromList
      [ DefDataType
          { dataLocation = Nothing
          , dataTypeCon = minstdTyCon
          , dataSerializable = IsSerializable True
          , dataParams = []
          , dataCons = DataVariant [(minstdDataCon, TInt64)]
          }
      ]
    values = NM.fromList
      [ mkVariantWorkerDef modName minstdTyCon minstdDataCon [] TInt64
      ]

daStackTypes :: Version -> Package
daStackTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = types
      , moduleValues = values
      }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Stack-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Stack", "Types"]
    srcLocTyCon = mkTypeCon ["SrcLoc"]
    fields =
      [ (mkField "srcLocPackage", TText)
      , (mkField "srcLocModule", TText)
      , (mkField "srcLocFile", TText)
      , (mkField "srcLocStartLine", TInt64)
      , (mkField "srcLocStartCol", TInt64)
      , (mkField "srcLocEndLine", TInt64)
      , (mkField "srcLocEndCol", TInt64)
      ]
    types = NM.fromList
      [ DefDataType
          { dataLocation = Nothing
          , dataTypeCon = srcLocTyCon
          , dataSerializable = IsSerializable True
          , dataParams = []
          , dataCons = DataRecord fields
          }
      ]
    values = NM.fromList
      $ mkWorkerDef modName srcLocTyCon [] fields
      : fmap (uncurry (mkSelectorDef modName srcLocTyCon [])) fields

daTimeTypes :: Version -> Package
daTimeTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Time-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daNonEmptyTypes :: Version -> Package
daNonEmptyTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-NonEmpty-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daDateTypes :: Version -> Package
daDateTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Date-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daSemigroupTypes :: Version -> Package
daSemigroupTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Semigroup-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daMonoidTypes :: Version -> Package
daMonoidTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Monoid-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daValidationTypes :: Version -> PackageId -> Package
daValidationTypes version nonEmptyPkgId = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Validation-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daLogicTypes :: Version -> Package
daLogicTypes version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Logic-Types"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daInternalDown :: Version -> Package
daInternalDown version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    , moduleValues = values
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-stdlib-DA-Internal-Down"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
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

daSetTypes :: Version -> Package
daSetTypes version = Package
    { packageLfVersion = version
    , packageModules = NM.singleton (emptyModule modName)
        { moduleDataTypes = types
        , moduleValues = values
        }
    , packageMetadata = ifMetadataRequired version $ PackageMetadata
        { packageName = PackageName "daml-stdlib-DA-Set-Types"
        , packageVersion = PackageVersion "1.0.0"
        , upgradedPackageId = Nothing
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

daInternalErased :: Version -> Package
daInternalErased version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-DA-Internal-Erased"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Internal", "Erased"]
    erasedTyCon = mkTypeCon ["Erased"]
    types = NM.fromList
      [ DefDataType Nothing erasedTyCon (IsSerializable False) [] $ DataVariant []
      ]

daInternalNatSyn :: Version -> Package
daInternalNatSyn version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
      { moduleDataTypes = types
      , moduleValues = NM.empty
      }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-DA-Internal-NatSyn"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Internal", "NatSyn"]
    natSynTyCon = mkTypeCon ["NatSyn"]
    types = NM.fromList
      [ DefDataType Nothing natSynTyCon (IsSerializable False) [(mkTypeVar "n", KNat)] $ DataVariant []
      ]

daInternalPromotedText :: Version -> Package
daInternalPromotedText version = Package
  { packageLfVersion = version
  , packageModules = NM.singleton (emptyModule modName)
    { moduleDataTypes = types
    }
  , packageMetadata = ifMetadataRequired version $ PackageMetadata
      { packageName = PackageName "daml-prim-DA-Internal-PromotedText"
      , packageVersion = PackageVersion "1.0.0"
      , upgradedPackageId = Nothing
      }
  }
  where
    modName = mkModName ["DA", "Internal", "PromotedText"]
    ptextTyCon = mkTypeCon ["PromotedText"]
    types = NM.fromList
      [ DefDataType Nothing ptextTyCon (IsSerializable False) [(mkTypeVar "t", KStar)] $ DataVariant []
      ]

daExceptionGeneralError :: Version -> Package
daExceptionGeneralError version = builtinExceptionPackage version "GeneralError"

daExceptionArithmeticError :: Version -> Package
daExceptionArithmeticError version = builtinExceptionPackage version "ArithmeticError"

daExceptionAssertionFailed :: Version -> Package
daExceptionAssertionFailed version = builtinExceptionPackage version "AssertionFailed"

daExceptionPreconditionFailed :: Version -> Package
daExceptionPreconditionFailed version = builtinExceptionPackage version "PreconditionFailed"

builtinExceptionPackage :: Version -> T.Text -> Package
builtinExceptionPackage version name = Package
    { packageLfVersion = version
    , packageModules = NM.singleton (emptyModule modName)
        { moduleDataTypes = types
        , moduleValues = values
        , moduleExceptions = exceptions
        }
    , packageMetadata = ifMetadataRequired version $ PackageMetadata
        { packageName = PackageName ("daml-prim-DA-Exception-" <> name)
        , packageVersion = PackageVersion "1.0.0"
        , upgradedPackageId = Nothing
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
