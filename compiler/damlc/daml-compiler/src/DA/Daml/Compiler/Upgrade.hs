-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--

module DA.Daml.Compiler.Upgrade
    ( generateUpgradeModule
    , generateGenInstancesModule
    , generateSrcFromLf
    , generateSrcPkgFromLf
    , Qualify(..)
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
import SdkVersion

-- | Generate a module containing generic instances for data types that don't have them already.
generateGenInstancesModule :: String -> (String, ParsedSource) -> String
generateGenInstancesModule qual (pkg, L _l src) =
    unlines $ header ++ map (showSDoc fakeDynFlags . ppr) genericInstances
  where
    modName =
        (moduleNameString $
         unLoc $ fromMaybe (error "missing module name") $ hsmodName src) ++
        qual
    header =
        [ "{-# LANGUAGE EmptyCase #-}"
        , "{-# LANGUAGE NoDamlSyntax #-}"
        , "module " <> modName <> "Instances" <> " where"
        , "import " <> modName
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
generateUpgradeModule templateNames modName qualA qualB =
    unlines $ header ++ concatMap upgradeTemplates templateNames
  where
    header =
        [ "daml 1.2"
        , "module " <> modName <> " where"
        , "import " <> modName <> qualA <> " qualified as A"
        , "import " <> modName <> qualB <> " qualified as B"
        , "import " <> modName <> "AInstances()"
        , "import " <> modName <> "BInstances()"
        , "import DA.Upgrade"
        ]

upgradeTemplates :: String -> [String]
upgradeTemplates n =
    [ "type " <> n <> "Upgrade = Upgrade A." <> n <> " B." <> n
    , "type " <> n <> "Rollback = Rollback A." <> n <> " B." <> n
    , "instance Convertible A." <> n <> " B." <> n
    ]

-- | Generate the full source for a daml-lf package.
generateSrcPkgFromLf ::
       LF.PackageId
    -> MS.Map GHC.UnitId LF.PackageId
    -> LF.Package
    -> [(NormalizedFilePath, String)]
generateSrcPkgFromLf thisPkgId pkgMap pkg = do
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
           ppr $ generateSrcFromLf (Qualify True) thisPkgId pkgMap mod) ++
          unlines (builtins mod))
  where
    modName = LF.unModuleName . LF.moduleName
    header m = header0 ++ header1 m
    header0 =
        ["{-# LANGUAGE NoDamlSyntax #-}", "{-# LANGUAGE NoImplicitPrelude #-}"]
    header1 m
        | modName m == ["DA", "Generics"] =
            ["", "{-# LANGUAGE TypeOperators #-}"]
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
            , "data Decimal = Decimal Opaque"
            , "data Module = Module TrName TrName"
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

newtype Qualify = Qualify Bool

-- | Extract all data defintions from a daml-lf module and generate a haskell source file from it.
generateSrcFromLf ::
       Qualify
    -> LF.PackageId
    -> MS.Map GHC.UnitId LF.PackageId
    -> LF.Module
    -> ParsedSource
generateSrcFromLf (Qualify qualify) thisPkgId pkgMap m = noLoc mod
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
    templateMethodNames =
        map mkRdrName
            [ "signatory"
            , "observer"
            , "agreement"
            , "fetch"
            , "ensure"
            , "create"
            , "archive"
            ]
    classMethodStub :: Located RdrName -> LHsBindLR GhcPs GhcPs
    classMethodStub funName =
        noLoc $
        FunBind
            { fun_ext = noExt
            , fun_id = funName
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
                                                { mc_fun = funName
                                                , mc_fixity = Prefix
                                                , mc_strictness = NoSrcStrict
                                                }
                                      , m_pats = []
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
                                                             HsApp
                                                                 noExt
                                                                 (noLoc $
                                                                  HsVar
                                                                      noExt
                                                                      (noLoc
                                                                           error_RDR))
                                                                 (noLoc $
                                                                  HsLit noExt $
                                                                  HsString
                                                                      NoSourceText $
                                                                  mkFastString
                                                                      "undefined template class method in generated code"))
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
    decls =
        concat $ do
            LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes m
            guard $ LF.getIsSerializable dataSerializable
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
                            , cid_binds = listToBag $ map classMethodStub templateMethodNames
                            , cid_sigs = []
                            , cid_tyfam_insts = []
                            , cid_datafam_insts = []
                            , cid_overlap_mode = Nothing
                            }
            let templateDataCons = NM.names $ LF.moduleTemplates m
            pure $ dataDecl : [templInstDecl | dataTypeCon `elem` templateDataCons]
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
            -- TODO (#2289): Add support for nat kind.
            LF.TNat _ -> error "nat kind not yet suppported in upgrades"

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
            -- TODO (#2289): Add support for Numeric types.
            LF.BTNumeric -> error "Numeric type not yet supported in upgrades"
    mkGhcType =
        HsTyVar noExt NotPromoted .
        noLoc . mkOrig gHC_TYPES . mkOccName varName
    damlStdlibUnitId = stringToUnitId damlStdlib
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
    mkTyConType = mkTyConType' qualify
    mkTyConTypeUnqual = mkTyConType' False
    mkTyConType' :: Bool -> TyCon -> HsType GhcPs
    mkTyConType' qualify tyCon
        | qualify =
            HsTyVar noExt NotPromoted . noLoc $
            mkRdrQual (moduleName $ nameModule name) (occName name)
        | otherwise = HsTyVar noExt NotPromoted . noLoc $ mkRdrUnqual (occName name)
      where
        name = getName tyCon
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
                  ((unitIdString $ getUnitId $ LF.PRImport thisPkgId) /= "daml-prim")
                  "GHC.Err"
            , mkImport
                  ((unitIdString $ getUnitId $ LF.PRImport thisPkgId) /= "daml-prim")
                  "GHC.CString"
            , mkImport
                  ((LF.unModuleName $ LF.moduleName m) == ["GHC", "Types"])
                  "GHC.Prim"
            , mkImport
                  ((LF.unModuleName $ LF.moduleName m) /= ["GHC", "Types"])
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
        , modRef `notElem` [LF.moduleName m, LF.ModuleName ["GHC", "Prim"]]
        ]
    modRefs =
        nubSort $
        [ (getUnitId pkg, modRef)
        | (pkg, modRef) <- toListOf moduleModuleRef m
        ] ++
        (map builtinToModuleRef $
         concat $ do
             dataTy <- NM.toList $ LF.moduleDataTypes m
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
            LF.BTMap -> (damlStdlibUnitId, LF.ModuleName ["DA", "Internal", "LF"])
            LF.BTArrow -> (primUnitId, translateModName funTyCon)
            -- TODO (#2289): Add support for Numeric types.
            LF.BTNumeric -> error "Numeric type not yet supported in upgrades"

    translateModName ::
           forall a. NamedThing a
        => a
        -> LF.ModuleName
    translateModName =
        LF.ModuleName .
        map T.pack . split (== '.') . moduleNameString . moduleName . nameModule . getName
