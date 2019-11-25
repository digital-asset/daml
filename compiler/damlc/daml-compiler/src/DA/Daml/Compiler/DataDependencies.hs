-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DataDependencies
    ( generateSrcPkgFromLf
    , generateTemplateInstancesPkgFromLf
    , generateGenInstancesPkgFromLf
    ) where

import Control.Lens (toListOf)
import Control.Monad
import Data.List.Extra
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import Safe
import System.FilePath

import "ghc-lib-parser" Bag
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" FastString
import "ghc-lib" GHC
import "ghc-lib-parser" Module
import "ghc-lib-parser" Name
import "ghc-lib-parser" Outputable (alwaysQualify, ppr, showSDocForUser)
import "ghc-lib-parser" PrelNames
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" TcEvidence (HsWrapper(..))
import "ghc-lib-parser" TysPrim
import "ghc-lib-parser" TysWiredIn

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics
import DA.Daml.Preprocessor.Generics
import SdkVersion

data Env = Env
    { envGetUnitId :: LF.PackageRef -> UnitId
    , envQualify :: Bool
    , envMod :: LF.Module
    }

-- | Extract all data defintions from a daml-lf module and generate a haskell source file from it.
generateSrcFromLf ::
       Env
    -> LF.PackageId
    -> ParsedSource
generateSrcFromLf env thisPkgId = noLoc mod
  where
    -- TODO (drsk) how come those '#' appear in daml-lf names?
    sanitize = T.dropWhileEnd (== '#')
    modName = mkModuleName $ T.unpack $ LF.moduleNameString $ LF.moduleName $ envMod env
    unitId = envGetUnitId env LF.PRSelf
    thisModule = mkModule unitId modName
    mkConRdr
        | envQualify env = mkRdrUnqual
        | otherwise = mkOrig thisModule
    mod =
        HsModule
            { hsmodImports = imports
            , hsmodName = Just (noLoc modName)
            , hsmodDecls = decls
            , hsmodDeprecMessage = Nothing
            , hsmodHaddockModHeader = Nothing
            , hsmodExports = Nothing
            }
    decls =
        concat $ do
            LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes $ envMod env
            guard $ LF.getIsSerializable dataSerializable
            let numberOfNameComponents = length (LF.unTypeConName dataTypeCon)
            -- we should never encounter more than two name components in dalfs.
            unless (numberOfNameComponents <= 2) $
                errTooManyNameComponents $ LF.unTypeConName dataTypeCon
            -- skip generated data types of sums of products construction in daml-lf
            [dataTypeCon0] <- [LF.unTypeConName dataTypeCon]
            let occName = mkOccName varName $ T.unpack $ sanitize dataTypeCon0
            let dataDecl =
                    noLoc $
                    TyClD noExt $
                    DataDecl
                        { tcdDExt = noExt
                        , tcdLName = noLoc $ mkConRdr occName
                        , tcdTyVars =
                              HsQTvs
                                  { hsq_ext = noExt
                                  , hsq_explicit =
                                        [ mkUserTyVar $ LF.unTypeVarName tyVarName
                                        | (tyVarName, _kind) <- dataParams
                                        ]
                                  }
                        , tcdFixity = Prefix
                        , tcdDataDefn =
                              HsDataDefn
                                  { dd_ext = noExt
                                  , dd_ND = DataType
                                  , dd_ctxt = noLoc []
                                  , dd_cType = Nothing
                                  , dd_kindSig = Nothing
                                  , dd_cons = convDataCons dataTypeCon0 dataCons
                                  , dd_derivs = noLoc []
                                  }
                        }
            pure [dataDecl]

    convDataCons :: T.Text -> LF.DataCons -> [LConDecl GhcPs]
    convDataCons dataTypeCon0 = \case
            LF.DataRecord fields ->
                [ noLoc $
                  ConDeclH98
                      { con_ext = noExt
                      , con_name =
                            noLoc $
                            mkConRdr $
                            mkOccName dataName $ T.unpack $ sanitize dataTypeCon0
                      , con_forall = noLoc False
                      , con_ex_tvs = []
                      , con_mb_cxt = Nothing
                      , con_doc = Nothing
                      , con_args =
                            RecCon $
                            noLoc
                                [ noLoc $
                                ConDeclField
                                    { cd_fld_ext = noExt
                                    , cd_fld_doc = Nothing
                                    , cd_fld_names =
                                          [ noLoc $
                                            FieldOcc
                                                { extFieldOcc = noExt
                                                , rdrNameFieldOcc =
                                                      mkRdrName $
                                                      LF.unFieldName fieldName
                                                }
                                          ]
                                    , cd_fld_type = noLoc $ convType env ty
                                    }
                                | (fieldName, ty) <- fields
                                ]
                      }
                ]
            LF.DataVariant cons ->
                [ noLoc $
                ConDeclH98
                    { con_ext = noExt
                    , con_name =
                          noLoc $
                          mkConRdr $
                          mkOccName varName $
                          T.unpack $ sanitize $ LF.unVariantConName conName
                    , con_forall = noLoc False
                    , con_ex_tvs = []
                    , con_mb_cxt = Nothing
                    , con_doc = Nothing
                    , con_args = let t = convType env ty
                                 in case (t :: HsType GhcPs) of
                                        HsRecTy _ext fs -> RecCon $ noLoc fs
                                        _other -> PrefixCon [noLoc t]
                    }
                | (conName, ty) <- cons
                ]
            LF.DataEnum cons ->
                [ noLoc $
                ConDeclH98
                    { con_ext = noExt
                    , con_name =
                          noLoc $
                          mkConRdr $
                          mkOccName varName $
                          T.unpack $ sanitize $ LF.unVariantConName conName
                    , con_forall = noLoc False
                    , con_ex_tvs = []
                    , con_mb_cxt = Nothing
                    , con_doc = Nothing
                    , con_args = PrefixCon []
                    }
                | conName <- cons
                ]

    imports = declImports ++ additionalImports
    mkImport :: Bool -> String -> [LImportDecl GhcPs]
    mkImport pred modName = [ noLoc $
        ImportDecl
            { ideclExt = noExt
            , ideclSourceSrc = NoSourceText
            , ideclName = noLoc $ mkModuleName modName
            , ideclPkgQual = Nothing
            , ideclSource = False
            , ideclSafe = False
            , ideclImplicit = False
            , ideclQualified = False
            , ideclAs = Nothing
            , ideclHiding = Nothing
            } :: LImportDecl GhcPs
        | pred
        ]
    -- additional imports needed for typechecking
    additionalImports =
        concat
            [ mkImport
                  ((unitIdString $ envGetUnitId env $ LF.PRImport thisPkgId) /= "daml-prim")
                  "GHC.Err"
            , mkImport
                  ((unitIdString $ envGetUnitId env $ LF.PRImport thisPkgId) /= "daml-prim")
                  "GHC.CString"
            , mkImport
                  ((LF.unModuleName $ LF.moduleName $ envMod env) == ["GHC", "Types"])
                  "GHC.Prim"
            , mkImport
                  ((LF.unModuleName $ LF.moduleName $ envMod env) /= ["GHC", "Types"])
                  "GHC.Types"
            ]
    -- imports needed by the module declarations
    declImports
     =
        [ noLoc $
        ImportDecl
            { ideclExt = noExt
            , ideclSourceSrc = NoSourceText
            , ideclName =
                  noLoc $ mkModuleName $ T.unpack $ LF.moduleNameString modRef
            , ideclPkgQual = Nothing
            , ideclSource = False
            , ideclSafe = False
            , ideclImplicit = False
            , ideclQualified = True
            , ideclAs = Nothing
            , ideclHiding = Nothing
            } :: LImportDecl GhcPs
        | (_unitId, modRef) <- modRefs
        , modRef `notElem` [LF.moduleName $ envMod env, LF.ModuleName ["GHC", "Prim"]]
        ]
    modRefs =
        nubSort $
        [ (envGetUnitId env pkg, modRef)
        | (pkg, modRef) <- toListOf moduleModuleRef $ envMod env
        ] ++
        (map builtinToModuleRef $
         concat $ do
             dataTy <- NM.toList $ LF.moduleDataTypes $ envMod env
             pure $ toListOf (dataConsType . builtinType) $ LF.dataCons dataTy)
    builtinToModuleRef = \case
            LF.BTInt64 -> (primUnitId, translateModName intTyCon)
            LF.BTDecimal -> (primUnitId, LF.ModuleName ["GHC", "Types"])
            LF.BTText -> (primUnitId, LF.ModuleName ["GHC", "Types"])
            LF.BTTimestamp -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTDate -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTParty -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTUnit -> (primUnitId, translateModName unitTyCon)
            LF.BTBool -> (primUnitId, translateModName boolTyCon)
            LF.BTList -> (primUnitId, translateModName listTyCon)
            LF.BTUpdate -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTScenario -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTContractId -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTOptional -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "Prelude"])
            LF.BTTextMap -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTGenMap -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
                -- GENMAP TODO (#2256): Verify module name once GenMap implemented in stdlib.
            LF.BTArrow -> (primUnitId, translateModName funTyCon)
            LF.BTNumeric -> (primUnitId, LF.ModuleName ["GHC", "Types"])
            LF.BTAny -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTTypeRep -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])

    translateModName ::
           forall a. NamedThing a
        => a
        -> LF.ModuleName
    translateModName =
        LF.ModuleName .
        map T.pack . split (== '.') . moduleNameString . moduleName . nameModule . getName

-- | Generate the source for a package containing template instances for all templates defined in a
-- package. It _only_ contains the instance stubs. The correct implementation happens in the
-- conversion to daml-lf, where `extenal` calls are inlined to daml-lf contained in the dalf of the
-- external package.
generateTemplateInstancesPkgFromLf ::
       (LF.PackageRef -> UnitId)
    -> LF.PackageId
    -> LF.Package
    -> [(NormalizedFilePath, String)]
generateTemplateInstancesPkgFromLf getUnitId pkgId pkg =
    catMaybes
        [ generateTemplateInstanceModule
            Env
                { envGetUnitId = getUnitId
                , envQualify = False
                , envMod = mod
                }
            pkgId
        | mod <- NM.toList $ LF.packageModules pkg
        ]

-- | Generate a module containing template/generic instances for all the contained templates.
-- Return Nothing if there are no instances, so no unnecessary modules are created.
generateTemplateInstanceModule ::
       Env -> LF.PackageId -> Maybe (NormalizedFilePath, String)
generateTemplateInstanceModule env externPkgId
    | not $ null instances =
        Just
            ( toNormalizedFilePath modFilePath
            , unlines $
              header ++
              nubSort imports ++
              map (showSDocForUser fakeDynFlags alwaysQualify . ppr) instances)
    | otherwise = Nothing
  where
    instances = templateInstances env externPkgId ++ choiceInstances env externPkgId

    mod = envMod env
    unitIdStr = unitIdString $ envGetUnitId env LF.PRSelf
    unitIdChunks = splitOn "-" unitIdStr
    packageName
        | all (`elem` '.' : ['0' .. '9']) $ lastDef "" unitIdChunks =
            intercalate "-" $ init unitIdChunks
        | otherwise = unitIdStr
    modFilePath = (joinPath $ splitOn "." modName) ++ ".daml"
    modName = T.unpack $ LF.moduleNameString $ LF.moduleName mod
    header =
        [ "{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE EmptyCase #-}"
        , "module " <> modName
        , "   ( module " <> modName
        , "   , module X"
        , "   )  where"
        ]
    imports =
        [ "import qualified \"" <> packageName <>
          "\" " <>
          modName <>
          " as X"
        , "import \"" <> packageName <> "\" " <> modName
        , "import qualified DA.Internal.LF"
        , "import qualified DA.Internal.Prelude"
        , "import qualified DA.Internal.Template"
        , "import qualified Sdk.DA.Internal.Template"
        , "import qualified GHC.Types"
        ]

-- | Generate a single template instance for a given template data constructor and parameters.
generateTemplateInstance ::
       Env
    -> LF.TypeConName
    -> [(LF.TypeVarName, LF.Kind)]
    -> LF.PackageId
    -> HsDecl GhcPs
generateTemplateInstance env typeCon typeParams externPkgId =
    InstD noExt $
    ClsInstD
        noExt
        ClsInstDecl
            { cid_ext = noExt
            , cid_poly_ty =
                  HsIB
                      { hsib_ext = noExt
                      , hsib_body =
                            noLoc $
                            HsAppTy noExt templateTy $
                            noLoc $
                            convType env lfTemplateType
                      }
            , cid_binds = mkClassMethodStubBag mkExternalString methodNames
            , cid_sigs = []
            , cid_tyfam_insts = []
            , cid_datafam_insts = []
            , cid_overlap_mode = Nothing
            }
  where
    moduleNameStr = T.unpack $ LF.moduleNameString moduleName0
    moduleName0 = LF.moduleName $ envMod env
    templateTy =
        noLoc $
        HsTyVar noExt NotPromoted $
        noLoc $
        mkRdrQual (mkModuleName "Sdk.DA.Internal.Template") $
        mkOccName varName "Template" :: LHsType GhcPs
    lfTemplateType = mkLfTemplateType moduleName0 typeCon typeParams
    mkExternalString :: T.Text -> String
    mkExternalString funName =
        (T.unpack $ LF.unPackageId externPkgId) <>
        ":" <> moduleNameStr <>
        ":" <> (T.unpack $ T.intercalate "." $ LF.unTypeConName typeCon) <>
        ":" <> T.unpack funName
    methodNames =
        [ "signatory"
        , "observer"
        , "agreement"
        , "fetch"
        , "ensure"
        , "create"
        , "archive"
        , "toAnyTemplate"
        , "fromAnyTemplate"
        , "_templateTypeRep"
        ]

-- | Generate a single choice instance for a given template/choice
generateChoiceInstance ::
       Env
    -> LF.PackageId
    -> LF.Template
    -> LF.TemplateChoice
    -> HsDecl GhcPs
generateChoiceInstance env externPkgId template choice =
    InstD noExt $
    ClsInstD
        noExt
        ClsInstDecl
            { cid_ext = noExt
            , cid_poly_ty =
                  HsIB
                      { hsib_ext = noExt
                      , hsib_body = body
                      }
            , cid_binds = mkClassMethodStubBag mkExternalString methodNames
            , cid_sigs = []
            , cid_tyfam_insts = []
            , cid_datafam_insts = []
            , cid_overlap_mode = Nothing
            }
  where

    body :: LHsType GhcPs =
      choiceClass `mkHsAppTy` arg1 `mkHsAppTy` arg2 `mkHsAppTy` arg3

    choiceClass :: LHsType GhcPs =
        noLoc $
        HsTyVar noExt NotPromoted $
        noLoc $
        mkRdrQual (mkModuleName "Sdk.DA.Internal.Template") $
        mkOccName varName "Choice" :: LHsType GhcPs

    arg1 :: LHsType GhcPs =
      noLoc $ convType env lfTemplateType

    arg2 :: LHsType GhcPs =
      noLoc $ convType env lfChoiceType

    arg3 :: LHsType GhcPs =
      noLoc $ convType env lfChoiceReturnType

    moduleNameStr = T.unpack $ LF.moduleNameString moduleName0
    moduleName0 = LF.moduleName $ envMod env
    lfTemplateType = mkLfTemplateType moduleName0 dataTypeCon dataParams

    tycon :: LF.TypeConName =
      LF.tplTypeCon template

    templateDT = case NM.lookup tycon (LF.moduleDataTypes (envMod env)) of
      Just x -> x
      Nothing -> error $ "Internal error: Could not find template definition for: " <> show tycon

    LF.DefDataType{dataTypeCon,dataParams} = templateDT
    LF.TemplateChoice { chcArgBinder = (_, lfChoiceType)
                      , chcName
                      , chcReturnType = lfChoiceReturnType
                      } = choice

    mkExternalString :: T.Text -> String
    mkExternalString funName =
      (T.unpack $ LF.unPackageId externPkgId) <>
      ":" <> moduleNameStr <>
      ":" <> (T.unpack $ T.intercalate "." $ LF.unTypeConName dataTypeCon) <>
      ":" <> (T.unpack $ LF.unChoiceName chcName) <>
      ":" <> T.unpack funName

    methodNames =
        [ "exercise"
        , "_toAnyChoice"
        , "_fromAnyChoice"
        ]

templateInstances :: Env -> LF.PackageId -> [HsDecl GhcPs]
templateInstances env externPkgId =
    [ generateTemplateInstance env dataTypeCon dataParams externPkgId
    | dataTypeCon <- NM.names $ LF.moduleTemplates mod
    , Just LF.DefDataType {..} <-
          [NM.lookup dataTypeCon (LF.moduleDataTypes mod)]
    ]
  where
    mod = envMod env

choiceInstances :: Env -> LF.PackageId -> [HsDecl GhcPs]
choiceInstances env externPkgId =
    [ generateChoiceInstance env externPkgId template choice
    | template <- NM.elems $ LF.moduleTemplates mod
    , choice <- NM.elems $ LF.tplChoices template
    ]
  where
    mod = envMod env

convType :: Env -> LF.Type -> HsType GhcPs
convType env =
    \case
        LF.TVar tyVarName ->
            HsTyVar noExt NotPromoted $ mkRdrName $ LF.unTypeVarName tyVarName
        LF.TCon LF.Qualified {..}
          | qualModule == LF.ModuleName ["DA", "Types"]
          , [name] <- LF.unTypeConName qualObject
          , Just n <- stripPrefix "Tuple" $ T.unpack name
          , Just i <- readMay n
          , 2 <= i && i <= 20 -> mkTuple i
        LF.TCon LF.Qualified {..} ->
          case LF.unTypeConName qualObject of
                [name] ->
                    HsTyVar noExt NotPromoted $
                    noLoc $
                    mkOrig
                        (mkModule
                             (envGetUnitId env qualPackage)
                             (mkModuleName $
                              T.unpack $ LF.moduleNameString qualModule))
                        (mkOccName varName $ T.unpack name)
                n@[_name0, _name1] ->
                    let fs =
                            MS.findWithDefault
                                (error $
                                 "Internal error: Could not find generated record type: " <>
                                 (T.unpack $ T.intercalate "." n))
                                n
                                (sumProdRecords $ envMod env)
                     in HsRecTy
                            noExt
                            [ noLoc $
                            ConDeclField
                                { cd_fld_ext = noExt
                                , cd_fld_names =
                                      [ noLoc $
                                        FieldOcc
                                            { extFieldOcc = noExt
                                            , rdrNameFieldOcc =
                                                  mkRdrName $
                                                  LF.unFieldName fieldName
                                            }
                                      ]
                                , cd_fld_type = noLoc $ convType env fieldTy
                                , cd_fld_doc = Nothing
                                }
                            | (fieldName, fieldTy) <- fs
                            ]
                cs -> errTooManyNameComponents cs
        LF.TApp ty1 ty2 ->
            HsParTy noExt $
            noLoc $ HsAppTy noExt (noLoc $ convType env ty1) (noLoc $ convType env ty2)
        LF.TBuiltin builtinTy -> convBuiltInTy (envQualify env) builtinTy
        LF.TForall {..} ->
            HsParTy noExt $
            noLoc $
            HsForAllTy
                noExt
                [mkUserTyVar $ LF.unTypeVarName $ fst forallBinder]
                (noLoc $ convType env forallBody)
        -- TODO (drsk): Is this the correct tuple type? What about the field names?
        LF.TTuple fls ->
            HsTupleTy
                noExt
                HsBoxedTuple
                [noLoc $ convType env ty | (_fldName, ty) <- fls]
        LF.TNat n ->
            HsTyLit noExt (HsNumTy NoSourceText (LF.fromTypeLevelNat n))
  where
    mkTuple :: Int -> HsType GhcPs
    mkTuple i =
        HsTyVar noExt NotPromoted $
        noLoc $ mkRdrUnqual $ occName $ tupleTyConName BoxedTuple i

convBuiltInTy :: Bool -> LF.BuiltinType -> HsType GhcPs
convBuiltInTy qualify =
    \case
        LF.BTInt64 -> mkTyConType qualify intTyCon
        LF.BTDecimal -> mkGhcType "Decimal"
        LF.BTText -> mkGhcType "Text"
        LF.BTTimestamp -> mkLfInternalType "Time"
        LF.BTDate -> mkLfInternalType "Date"
        LF.BTParty -> mkLfInternalType "Party"
        LF.BTUnit -> mkTyConTypeUnqual unitTyCon
        LF.BTBool -> mkTyConType qualify boolTyCon
        LF.BTList -> mkTyConTypeUnqual listTyCon
        LF.BTUpdate -> mkLfInternalType "Update"
        LF.BTScenario -> mkLfInternalType "Scenario"
        LF.BTContractId -> mkLfInternalType "ContractId"
        LF.BTOptional -> mkLfInternalPrelude "Optional"
        LF.BTTextMap -> mkLfInternalType "TextMap"
        LF.BTGenMap -> mkLfInternalType "GenMap"
            -- GENMAP TODO  (#2256): Verify type name once implemented in stdlib.
        LF.BTArrow -> mkTyConTypeUnqual funTyCon
        LF.BTNumeric -> mkGhcType "Numeric"
        LF.BTAny -> mkLfInternalType "Any"
        LF.BTTypeRep -> mkLfInternalType "TypeRep"

errTooManyNameComponents :: [T.Text] -> a
errTooManyNameComponents cs =
    error $
    "Internal error: Dalf contains type constructors with more than two name components: " <>
    (T.unpack $ T.intercalate "." cs)

mkUserTyVar :: T.Text -> LHsTyVarBndr GhcPs
mkUserTyVar =
    noLoc .
    UserTyVar noExt . noLoc . mkRdrUnqual . mkOccName tvName . T.unpack

mkRdrName :: T.Text -> Located RdrName
mkRdrName = noLoc . mkRdrUnqual . mkOccName varName . T.unpack

damlStdlibUnitId :: UnitId
damlStdlibUnitId = stringToUnitId damlStdlib

sumProdRecords :: LF.Module -> MS.Map [T.Text] [(LF.FieldName, LF.Type)]
sumProdRecords m =
    MS.fromList
        [ (dataTyCon, fs)
        | LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes m
        , let dataTyCon = LF.unTypeConName dataTypeCon
        , length dataTyCon == 2
        , LF.DataRecord fs <- [dataCons]
        ]

mkTyConType :: Bool -> TyCon -> HsType GhcPs
mkTyConType qualify tyCon
    | qualify =
        HsTyVar noExt NotPromoted . noLoc $
        mkRdrQual (moduleName $ nameModule name) (occName name)
    | otherwise = HsTyVar noExt NotPromoted . noLoc $ mkRdrUnqual (occName name)
  where
    name = getName tyCon

mkGhcType :: String -> HsType GhcPs
mkGhcType =
    HsTyVar noExt NotPromoted .
    noLoc . mkOrig gHC_TYPES . mkOccName varName

mkLfInternalType :: String -> HsType GhcPs
mkLfInternalType =
    HsTyVar noExt NotPromoted .
    noLoc .
    mkOrig (mkModule damlStdlibUnitId $ mkModuleName "DA.Internal.LF") .
    mkOccName varName


mkLfInternalPrelude :: String -> HsType GhcPs
mkLfInternalPrelude =
    HsTyVar noExt NotPromoted .
    noLoc .
    mkOrig (mkModule damlStdlibUnitId $ mkModuleName "DA.Internal.Prelude") .
    mkOccName varName

mkTyConTypeUnqual :: TyCon -> HsType GhcPs
mkTyConTypeUnqual = mkTyConType False

mkClassMethodStubBag :: (T.Text -> String) -> [T.Text] -> Bag (LHsBindLR GhcPs GhcPs)
mkClassMethodStubBag mkExternalString methodNames = do
  let methodMapping = map (\funName -> (funName, mkExternalString funName)) methodNames
  listToBag $ map classMethodStub methodMapping

classMethodStub :: (T.Text, String) -> LHsBindLR GhcPs GhcPs
classMethodStub (funName, xString) =
    noLoc $
    FunBind
        { fun_ext = noExt
        , fun_id = mkRdrName funName
        , fun_matches =
              MG
                  { mg_ext = noExt
                  , mg_alts =
                        noLoc
                            [ noLoc $
                              Match
                                  { m_ext = noExt
                                  , m_ctxt =
                                        FunRhs
                                            { mc_fun = mkRdrName funName
                                            , mc_fixity = Prefix
                                            , mc_strictness = NoSrcStrict
                                            }
                                  , m_pats =
                                        [ noLoc $
                                        VarPat noExt (mkRdrName "proxy")
                                        | funName == "_templateTypeRep"
                                        ] -- NOTE (drsk): we shouldn't need this pattern, but
                                          -- somehow ghc insists on it. We want to fix this in ghc.
                                  , m_rhs_sig = Nothing
                                  , m_grhss =
                                        GRHSs
                                            { grhssExt = noExt
                                            , grhssGRHSs =
                                                  [ noLoc $
                                                    GRHS
                                                        noExt
                                                        []
                                                        (noLoc $
                                                         HsAppType
                                                             noExt
                                                             (noLoc $
                                                              HsVar
                                                                  noExt
                                                                  (noLoc $
                                                                   mkRdrQual
                                                                       (mkModuleName
                                                                            "GHC.Types")
                                                                       (mkOccName
                                                                            varName
                                                                            "external")))
                                                             (HsWC
                                                                  noExt
                                                                  (noLoc $
                                                                   HsTyLit noExt $
                                                                   HsStrTy
                                                                       NoSourceText $
                                                                   mkFastString xString)))
                                                  ]
                                            , grhssLocalBinds =
                                                  noLoc emptyLocalBinds
                                            }
                                  }
                            ]
                  , mg_origin = Generated
                  }
        , fun_co_fn = WpHole
        , fun_tick = []
        }

mkLfTemplateType :: LF.ModuleName -> LF.TypeConName -> [(LF.TypeVarName, a)] -> LF.Type
mkLfTemplateType moduleName0 typeCon typeParams=
  LF.mkTApps
    (LF.TCon (LF.Qualified LF.PRSelf moduleName0 typeCon))
    (map (LF.TVar . fst) typeParams)

-- | Generate the full source for a daml-lf package.
generateSrcPkgFromLf ::
       (LF.PackageRef -> UnitId)
    -> LF.PackageId
    -> LF.Package
    -> [(NormalizedFilePath, String)]
generateSrcPkgFromLf getUnitId thisPkgId pkg = do
    mod <- NM.toList $ LF.packageModules pkg
    guard $ (LF.unModuleName $ LF.moduleName mod) /= ["GHC", "Prim"]
    let fp =
            toNormalizedFilePath $
            (joinPath $ map T.unpack $ LF.unModuleName $ LF.moduleName mod) <.>
            ".daml"
    pure
        ( fp
        , unlines (header mod) ++
          (showSDocForUser fakeDynFlags alwaysQualify $
           ppr $ generateSrcFromLf (Env getUnitId True mod) thisPkgId) ++
          unlines (builtins mod))
  where
    modName = LF.unModuleName . LF.moduleName
    header m = header0 ++ header1 m
    header0 =
        ["{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE NoImplicitPrelude #-}"
        , "{-# LANGUAGE TypeOperators #-}"
        ]
    header1 m
        | modName m == ["GHC", "Types"] = ["", "{-# LANGUAGE MagicHash #-}"]
        | otherwise = []
    --
    -- IMPORTANT
    -- =========
    --
    -- The following are datatypes that are not compiled to daml-lf because they are builtin into
    -- the compiler. They will not show up in any daml-lf package and can hence not be recovered.
    -- They are however needed to generate interface files. Be very careful if you need to delete or
    -- change any of the following data types and make sure that upgrades still work. Generally,
    -- this should be unproblematic as long as the exported API of these files doesn't change.
    builtins m
        | LF.unModuleName (LF.moduleName m) == ["DA", "Internal", "LF"] =
            [ ""
            , "data TextMap a = TextMap GHC.Types.Opaque"
            , "data Time = Time GHC.Types.Opaque"
            , "data Date = Date GHC.Types.Opaque"
            , "data ContractId a = ContractId GHC.Types.Opaque"
            , "data Update a = Update GHC.Types.Opaque"
            , "data Scenario a = Scenario GHC.Types.Opaque"
            , "data Party = Party GHC.Types.Opaque"
            ]
        | LF.unModuleName (LF.moduleName m) == ["DA", "Internal", "Template"] =
            [ ""
            , "class Template c where"
            , "   signatory :: c -> [DA.Internal.LF.Party]"
            ]
        | LF.unModuleName (LF.moduleName m) == ["GHC", "Types"] =
            [ ""
            , "data [] a = [] | a : [a]"
            , "data Opaque = Opaque"
            , "data Int = Int#"
            , "data Char"
            , "data Text = Text Opaque"
            , "type TextLit = [Char]"
            , "data Word"
            ]
            ++ if LF.packageLfVersion pkg `LF.supports` LF.featureNumeric then
                    [ "data Nat"
                    , "data Numeric (n: Nat) = Numeric Opaque"
                    , "type Decimal = Numeric 10"
                    ]
                else
                    [ "data Decimal = Decimal Opaque" ]
            ++
            [ "data Module = Module TrName TrName"
            , "data TrName = TrNameS Addr# | TrNameD [Char]"
            , "data KindBndr = Int"
            , "data RuntimeRep"
            , "data KindRep = KindRepTyConApp TyCon [KindRep] \
                              \ | KindRepVar !KindBndr \
                              \ | KindRepApp KindRep KindRep \
                              \ | KindRepFun KindRep KindRep \
                              \ | KindRepTYPE !RuntimeRep \
                              \ | KindRepTypeLitS TypeLitSort Addr# \
                              \ | KindRepTypeLitD TypeLitSort [Char]"
            , "data TypeLitSort = TypeLitSymbol | TypeLitNat"
            , "data TyCon = TyCon Word# Word# \
                                     \ Module \
                                     \ TrName \
                                     \ Int# \
                                     \ KindRep"

            ]
        | otherwise = []


genericInstances :: Env -> LF.PackageId -> ([ImportDecl GhcPs], [HsDecl GhcPs])
genericInstances env externPkgId =
    ( [unLoc imp | imp <- hsmodImports src]
    , [ unLoc $
      generateGenericInstanceFor
          (nameOccName genClassName)
          tcdLName
          (T.unpack $ LF.unPackageId externPkgId)
          (noLoc $
           mkModuleName $
           T.unpack $ LF.moduleNameString $ LF.moduleName $ envMod env)
          tcdTyVars
          tcdDataDefn
      | L _ (TyClD _x DataDecl {..}) <- hsmodDecls src
      ])
  where
    src = unLoc $ generateSrcFromLf env externPkgId


generateGenInstancesPkgFromLf ::
       (LF.PackageRef -> UnitId)
    -> LF.PackageId
    -> LF.Package
    -> String
    -> [(NormalizedFilePath, String)]
generateGenInstancesPkgFromLf getUnitId pkgId pkg qual =
    catMaybes
        [ generateGenInstanceModule
            Env
                { envGetUnitId = getUnitId
                , envQualify = False
                , envMod = mod
                }
            pkgId
            qual
        | mod <- NM.toList $ LF.packageModules pkg
        ]

generateGenInstanceModule ::
       Env -> LF.PackageId -> String -> Maybe (NormalizedFilePath, String)
generateGenInstanceModule env externPkgId qual
    | not $ null instances =
        Just
            ( toNormalizedFilePath modFilePath
            , unlines $
              header ++
              nubSort imports ++
              map (showSDocForUser fakeDynFlags alwaysQualify . ppr) genImports ++
              [ replace (modName <> ".") (modNameQual <> ".") $
                 unlines $
                 map
                     (showSDocForUser fakeDynFlags alwaysQualify . ppr)
                     instances
              ])
    | otherwise = Nothing
  where
    instances = genInstances
    genImportsAndInstances = genericInstances env externPkgId
    genImports = [idecl{ideclQualified = True} | idecl <- fst genImportsAndInstances]
    genInstances = snd genImportsAndInstances

    mod = envMod env

    modFilePath = (joinPath $ splitOn "." modName) ++ qual ++ "GenInstances" ++ ".daml"
    modName = T.unpack $ LF.moduleNameString $ LF.moduleName mod
    modNameQual = modName <> qual
    header =
        [ "{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE EmptyCase #-}"
        , "module " <> modNameQual <> "GenInstances" <> " where"
        ]
    imports =
        [ "import qualified " <> modNameQual
        , "import qualified DA.Generics"
        ]

