-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--
{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Compiler.Upgrade
    ( generateUpgradeModule
    , generateGenInstancesModule
    , generateSrcFromLf
    , generateSrcPkgFromLf
    , DontQualify(..)
    ) where

import "ghc-lib-parser" BasicTypes
import Control.Lens (toListOf)
import qualified DA.Daml.LF.Ast.Base as LF
import DA.Daml.LF.Ast.Optics
import qualified DA.Daml.LF.Ast.Util as LF
import DA.Daml.Preprocessor.Generics
import Data.List.Extra
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import Data.Tuple
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import "ghc-lib" GHC
import "ghc-lib-parser" Module
import "ghc-lib-parser" Name
import "ghc-lib-parser" Outputable (ppr, showSDoc, showSDocForUser, alwaysQualify)
import "ghc-lib-parser" PrelNames
import "ghc-lib-parser" RdrName
import System.FilePath.Posix
import "ghc-lib-parser" TysPrim
import "ghc-lib-parser" TysWiredIn
import "ghc-lib-parser" FastString
import "ghc-lib-parser" Bag
import "ghc-lib-parser" TcEvidence (HsWrapper(..))
import Control.Monad

-- | Generate a module containing generic instances for data types that don't have them already.
generateGenInstancesModule :: String -> (String, ParsedSource) -> String
generateGenInstancesModule qual (pkg, L _l src) =
    unlines $ header ++ map (showSDoc fakeDynFlags . ppr) genericInstances
  where
    modName =
        moduleNameString $
        unLoc $ fromMaybe (error "missing module name") $ hsmodName src
    header =
        [ "{-# LANGUAGE EmptyCase #-}"
        , "{-# LANGUAGE NoDamlSyntax #-}"
        , "module " <> modName <> "Instances" <> qual <> " where"
        , "import \"" <> pkg <> "\" " <> modName
        , "import DA.Generics"
        ]
    genericInstances =
        [ generateGenericInstanceFor
            (nameOccName genClassName)
            tcdLName
            pkg
            (fromMaybe (error "Missing module name") $ hsmodName src)
            tcdTyVars
            tcdDataDefn
        | L _ (TyClD _x DataDecl {..}) <- hsmodDecls src
        , not $ hasGenDerivation tcdDataDefn
        ]
    hasGenDerivation :: HsDataDefn GhcPs -> Bool
    hasGenDerivation HsDataDefn {..} =
        or [ name `elem` map nameOccName genericClassNames
            | d <- unLoc dd_derivs
            , (HsIB _ (L _ (HsTyVar _ _ (L _ (Unqual name))))) <-
                  unLoc $ deriv_clause_tys $ unLoc d
            ]
    hasGenDerivation XHsDataDefn{} = False

-- | Generate non-consuming choices to upgrade all templates defined in the module.
generateUpgradeModule :: [String] -> String -> String -> String -> String
generateUpgradeModule templateNames modName pkgA pkgB =
    unlines $ header ++ concatMap upgradeTemplate templateNames
  where
    header =
        [ "daml 1.2"
        , "module " <> modName <> " where"
        , "import \"" <> pkgA <> "\" " <> modName <> " qualified as A"
        , "import \"" <> pkgB <> "\" " <> modName <> " qualified as B"
        , "import " <> modName <> "InstancesA()"
        , "import " <> modName <> "InstancesB()"
        , "import DA.Next.Set"
        , "import DA.Upgrade"
        ]

upgradeTemplate :: String -> [String]
upgradeTemplate n =
    [ "template " <> n <> "Upgrade"
    , "    with"
    , "        op : Party"
    , "    where"
    , "        signatory op"
    , "        nonconsuming choice Upgrade: ContractId B." <> n
    , "            with"
    , "                inC : ContractId A." <> n
    , "                sigs : [Party]"
    , "            controller sigs"
    , "                do"
    , "                    d <- fetch inC"
    , "                    assert $ fromList sigs == fromList (signatory d)"
    , "                    create $ conv d"
    ]

-- | Generate the full source for a daml-lf package.
generateSrcPkgFromLf ::
       LF.PackageId
    -> MS.Map GHC.UnitId LF.PackageId
    -> LF.Package
    -> [(NormalizedFilePath, String)]
generateSrcPkgFromLf thisPkgId pkgMap pkg = do
    mod <- NM.toList $ LF.packageModules pkg
    let fp =
            toNormalizedFilePath $
            (joinPath $ map T.unpack $ LF.unModuleName $ LF.moduleName mod) <.>
            ".daml"
    pure
        ( fp
        , unlines header ++
          (showSDocForUser fakeDynFlags alwaysQualify $
           ppr $ generateSrcFromLf (DontQualify False) thisPkgId pkgMap mod) ++
          unlines (builtins mod))
  where
    header =
        [ "{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE NoImplicitPrelude #-}"
        , "{-# LANGUAGE TypeOperators #-}"
        ]
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
        | otherwise = []

newtype DontQualify = DontQualify Bool

-- | Extract all data defintions from a daml-lf module and generate a haskell source file from it.
generateSrcFromLf ::
       DontQualify
    -> LF.PackageId
    -> MS.Map GHC.UnitId LF.PackageId
    -> LF.Module
    -> ParsedSource
generateSrcFromLf (DontQualify dontQualify) thisPkgId pkgMap m = noLoc mod
  where
    pkgMapInv = MS.fromList $ map swap $ MS.toList pkgMap
    getUnitId :: LF.PackageRef -> UnitId
    getUnitId pkgRef =
        fromMaybe (error $ "Unknown package: " <> show pkgRef) $
        case pkgRef of
            LF.PRSelf -> MS.lookup thisPkgId pkgMapInv
            LF.PRImport pkgId -> MS.lookup pkgId pkgMapInv
    -- TODO (drsk) how come those '#' appear in daml-lf names?
    sanitize = T.dropWhileEnd (== '#')
    mod =
        HsModule
            { hsmodImports = imports
            , hsmodName =
                  Just
                      (noLoc $
                       mkModuleName $
                       T.unpack $ LF.moduleNameString $ LF.moduleName m)
            , hsmodDecls = decls
            , hsmodDeprecMessage = Nothing
            , hsmodHaddockModHeader = Nothing
            , hsmodExports = Nothing
            }
    templateTy =
        noLoc $
        HsTyVar noExt NotPromoted $
        noLoc $
        mkRdrQual (mkModuleName "DA.Internal.Template") $
        mkOccName varName "Template" :: LHsType GhcPs
    sigRdrName = noLoc $ mkRdrUnqual $ mkOccName varName "signatory"
    errTooManyNameComponents cs =
        error $
        "Internal error: Dalf contains type constructors with more than two name components: " <>
        (T.unpack $ T.intercalate "." cs)
    sumProdRecords =
        MS.fromList
            [ (dataTyCon, fs)
            | LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes m
            , let dataTyCon = LF.unTypeConName dataTypeCon
            , length dataTyCon == 2
            , LF.DataRecord fs <- [dataCons]
            ]
    decls =
        concat $ do
            LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes m
            guard $ not $ isTypeClass dataCons
            let numberOfNameComponents = length (LF.unTypeConName dataTypeCon)
            -- we should never encounter more than two name components in dalfs.
            unless (numberOfNameComponents <= 2) $
                errTooManyNameComponents $ LF.unTypeConName dataTypeCon
            -- skip generated data types of sums of products construction in daml-lf
            [dataTypeCon0] <- [LF.unTypeConName dataTypeCon]
            let templType =
                    LF.mkTApps
                        (LF.TCon
                             (LF.Qualified LF.PRSelf (LF.moduleName m) dataTypeCon))
                        (map (LF.TVar . fst) dataParams)
            let occName = mkOccName varName $ T.unpack $ sanitize dataTypeCon0
            let dataDecl =
                    noLoc $
                    TyClD noExt $
                    DataDecl
                        { tcdDExt = noExt
                        , tcdLName = noLoc $ mkRdrUnqual occName
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
            -- dummy template instance to make sure we get a template instance in the interface
            -- file
            let templInstDecl =
                    noLoc $
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
                                            noLoc $ convType templType
                                      }
                            , cid_binds =
                                  listToBag
                                      [ noLoc $
                                        FunBind
                                            { fun_ext = noExt
                                            , fun_id = sigRdrName
                                            , fun_matches =
                                                  MG
                                                      { mg_ext = noExt
                                                      , mg_alts =
                                                            noLoc
                                                                [ noLoc $
                                                                  Match
                                                                      { m_ext =
                                                                            noExt
                                                                      , m_ctxt =
                                                                            FunRhs
                                                                                { mc_fun =
                                                                                      sigRdrName
                                                                                , mc_fixity =
                                                                                      Prefix
                                                                                , mc_strictness =
                                                                                      NoSrcStrict
                                                                                }
                                                                      , m_pats = []
                                                                      , m_rhs_sig =
                                                                            Nothing
                                                                      , m_grhss =
                                                                            GRHSs
                                                                                { grhssExt =
                                                                                      noExt
                                                                                , grhssGRHSs =
                                                                                      [ noLoc $
                                                                                        GRHS
                                                                                            noExt
                                                                                            [
                                                                                            ]
                                                                                            (noLoc $
                                                                                             HsApp
                                                                                                 noExt
                                                                                                 (noLoc $
                                                                                                  HsVar
                                                                                                      noExt
                                                                                                      (noLoc
                                                                                                           error_RDR))
                                                                                                 (noLoc $
                                                                                                  HsLit
                                                                                                      noExt $
                                                                                                  HsString
                                                                                                      NoSourceText $
                                                                                                  mkFastString
                                                                                                      "undefined template class method in generated code"))
                                                                                      ]
                                                                                , grhssLocalBinds =
                                                                                      noLoc
                                                                                      emptyLocalBinds
                                                                                }
                                                                      }
                                                                ]
                                                      , mg_origin = Generated
                                                      }
                                            , fun_co_fn = WpHole
                                            , fun_tick = []
                                            }
                                      ]
                            , cid_sigs = []
                            , cid_tyfam_insts = []
                            , cid_datafam_insts = []
                            , cid_overlap_mode = Nothing
                            }
            let templateDataCons = NM.names $ LF.moduleTemplates m
            pure $ dataDecl : [templInstDecl | dataTypeCon `elem` templateDataCons]
    isTypeClass :: LF.DataCons -> Bool
    isTypeClass =
        \case
            LF.DataRecord fields ->
                not $
                null $
                catMaybes
                    [ T.stripPrefix "_" $ LF.unFieldName fieldName
                    | (fieldName, _ty) <- fields
                    ]
            LF.DataVariant _cons -> False
            LF.DataEnum _cons -> False
    convDataCons :: T.Text -> LF.DataCons -> [LConDecl GhcPs]
    convDataCons dataTypeCon0 =
        \case
            LF.DataRecord fields ->
                [ noLoc $
                  ConDeclH98
                      { con_ext = noExt
                      , con_name =
                            noLoc $
                            mkRdrUnqual $
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
                                    , cd_fld_type = noLoc $ convType ty
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
                          mkRdrUnqual $
                          mkOccName varName $
                          T.unpack $ sanitize $ LF.unVariantConName conName
                    , con_forall = noLoc False
                    , con_ex_tvs = []
                    , con_mb_cxt = Nothing
                    , con_doc = Nothing
                    , con_args =
                          case ty of
                              LF.TBuiltin LF.BTUnit -> PrefixCon []
                              otherTy ->
                                  let t = convType otherTy
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
                          mkRdrUnqual $
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
    mkRdrName = noLoc . mkRdrUnqual . mkOccName varName . T.unpack
    mkUserTyVar :: T.Text -> LHsTyVarBndr GhcPs
    mkUserTyVar =
        noLoc .
        UserTyVar noExt . noLoc . mkRdrUnqual . mkOccName tvName . T.unpack
    convType :: LF.Type -> HsType GhcPs
    convType =
        \case
            LF.TVar tyVarName ->
                HsTyVar noExt NotPromoted $ mkRdrName $ LF.unTypeVarName tyVarName
            LF.TCon LF.Qualified {..} ->
                case LF.unTypeConName qualObject of
                    [name] ->
                        HsTyVar noExt NotPromoted $
                        noLoc $
                        mkOrig
                            (mkModule
                                 (getUnitId qualPackage)
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
                                    sumProdRecords
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
                                    , cd_fld_type = noLoc $ convType fieldTy
                                    , cd_fld_doc = Nothing
                                    }
                                | (fieldName, fieldTy) <- fs
                                ]
                    cs -> errTooManyNameComponents cs
            LF.TApp ty1 ty2 ->
                HsParTy noExt $
                noLoc $ HsAppTy noExt (noLoc $ convType ty1) (noLoc $ convType ty2)
            LF.TBuiltin builtinTy -> convBuiltInTy builtinTy
            LF.TForall {..} ->
                HsParTy noExt $
                noLoc $
                HsForAllTy
                    noExt
                    [mkUserTyVar $ LF.unTypeVarName $ fst forallBinder]
                    (noLoc $ convType forallBody)
            -- TODO (drsk): Is this the correct tuple type? What about the field names?
            LF.TTuple fls ->
                HsTupleTy
                    noExt
                    HsBoxedTuple
                    [noLoc $ convType ty | (_fldName, ty) <- fls]
    convBuiltInTy :: LF.BuiltinType -> HsType GhcPs
    convBuiltInTy =
        \case
            LF.BTInt64 -> mkTyConType intTyCon
            LF.BTDecimal -> mkGhcType "Decimal"
            LF.BTText -> mkGhcType "Text"
            LF.BTTimestamp -> mkLfInternalType "Time"
            LF.BTDate -> mkLfInternalType "Date"
            LF.BTParty -> mkLfInternalType "Party"
            LF.BTUnit -> mkTyConTypeUnqual unitTyCon
            LF.BTBool -> mkTyConType boolTyCon
            LF.BTList -> mkTyConTypeUnqual listTyCon
            LF.BTUpdate -> mkLfInternalType "Update"
            LF.BTScenario -> mkLfInternalType "Scenario"
            LF.BTContractId -> mkLfInternalType "ContractId"
            LF.BTOptional -> mkLfInternalPrelude "Optional"
            LF.BTMap -> mkLfInternalType "TextMap"
            LF.BTArrow -> mkTyConTypeUnqual funTyCon
    mkGhcType =
        HsTyVar noExt NotPromoted .
        noLoc . mkOrig gHC_TYPES . mkOccName varName
    damlStdlibUnitId = stringToUnitId "daml-stdlib"
    mkLfInternalType =
        HsTyVar noExt NotPromoted .
        noLoc .
        mkOrig (mkModule damlStdlibUnitId $ mkModuleName "DA.Internal.LF") .
        mkOccName varName
    mkLfInternalPrelude =
        HsTyVar noExt NotPromoted .
        noLoc .
        mkOrig (mkModule damlStdlibUnitId $ mkModuleName "DA.Internal.Prelude") .
        mkOccName varName
    mkTyConType = mkTyConType' dontQualify
    mkTyConTypeUnqual = mkTyConType' True
    mkTyConType' :: Bool -> TyCon -> HsType GhcPs
    mkTyConType' dontQualify tyCon
        | dontQualify = HsTyVar noExt NotPromoted . noLoc $ mkRdrUnqual (occName name)
        | otherwise =
            HsTyVar noExt NotPromoted . noLoc $
            mkRdrQual (moduleName $ nameModule name) (occName name)
      where
        name = getName tyCon
    imports = declImports ++ additionalImports
    additionalImports =
        [ noLoc $
        ImportDecl
            { ideclExt = noExt
            , ideclSourceSrc = NoSourceText
            , ideclName = noLoc $ mkModuleName "GHC.Err"
            , ideclPkgQual = Nothing
            , ideclSource = False
            , ideclSafe = False
            , ideclImplicit = False
            , ideclQualified = True
            , ideclAs = Nothing
            , ideclHiding = Nothing
            } :: LImportDecl GhcPs
        | LF.unModuleName (LF.moduleName m) /= ["GHC", "Err"]
        ]
        ++
        [ noLoc $
        ImportDecl
            { ideclExt = noExt
            , ideclSourceSrc = NoSourceText
            , ideclName = noLoc $ mkModuleName "GHC.CString"
            , ideclPkgQual = Nothing
            , ideclSource = False
            , ideclSafe = False
            , ideclImplicit = False
            , ideclQualified = False
            , ideclAs = Nothing
            , ideclHiding = Nothing
            } :: LImportDecl GhcPs
        | LF.unModuleName (LF.moduleName m) /= ["GHC", "CString"]
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
        , modRef /= LF.moduleName m
        , LF.unModuleName modRef /= ["GHC", "Prim"]
        ]
    modRefs =
        nubSort $
        [ (getUnitId pkg, modRef)
        | (pkg, modRef) <- toListOf moduleModuleRef m
        ] ++
        (map builtinToModuleRef $
         concat $ do
             dataTy <- NM.toList $ LF.moduleDataTypes m
             case LF.dataCons dataTy of
                 LF.DataRecord fs -> map (toListOf builtinType . snd) fs
                 LF.DataVariant vs -> map (toListOf builtinType . snd) vs
                 LF.DataEnum _es -> pure [])
    builtinToModuleRef = \case
            LF.BTInt64 -> (primUnitId, translateModName intTyCon)
            LF.BTDecimal -> (primUnitId, LF.ModuleName ["GHC", "Types"])
            LF.BTText -> (primUnitId, LF.ModuleName ["GHC", "Types"])
            LF.BTTimestamp -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTDate -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTParty -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTUnit -> (primUnitId, translateModName unitTyCon)
            LF.BTBool -> (primUnitId, translateModName boolTyCon)
            LF.BTList -> (primUnitId, translateModName listTyCon)
            LF.BTUpdate -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTScenario -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTContractId -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTOptional -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "Prelude"])
            LF.BTMap -> (stdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTArrow -> (primUnitId, translateModName funTyCon)

    stdlibUnitId = stringToUnitId "daml-stdlib"

    translateModName ::
           forall a. NamedThing a
        => a
        -> LF.ModuleName
    translateModName =
        LF.ModuleName .
        map T.pack . split (== '.') . moduleNameString . moduleName . nameModule . getName
