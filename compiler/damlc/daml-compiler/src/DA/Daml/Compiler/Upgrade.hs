-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--
{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Compiler.Upgrade
    ( generateUpgradeModule
    , generateGenInstancesModule
    , generateSrcFromLf
    , generateSrcPkgFromLf
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
        , "daml 1.2"
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
            (T.unpack $ T.intercalate "/" $ LF.unModuleName $ LF.moduleName mod) <.>
            ".daml"
    pure ( fp
         , unlines header ++
           (showSDocForUser fakeDynFlags alwaysQualify $
            ppr $ generateSrcFromLf thisPkgId pkgMap mod))
  where
    header = ["{-# LANGUAGE NoDamlSyntax #-}", "{-# LANGUAGE NoImplicitPrelude #-}"]

-- | Extract all data defintions from a daml-lf module and generate a haskell source file from it.
generateSrcFromLf ::
       LF.PackageId
    -> MS.Map GHC.UnitId LF.PackageId
    -> LF.Module
    -> ParsedSource
generateSrcFromLf thisPkgId pkgMap m = noLoc mod
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
        HsTyVar NoExt NotPromoted $
        noLoc $
        mkRdrQual (mkModuleName "DA.Internal.Template") $
        mkOccName varName "Template" :: LHsType GhcPs
    sigRdrName = noLoc $ mkRdrUnqual $ mkOccName varName "signatory"
    decls =
        concat $ do
            LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes m
            let templType =
                    LF.mkTApps
                        (LF.TCon
                             (LF.Qualified LF.PRSelf (LF.moduleName m) dataTypeCon))
                        (map (LF.TVar . fst) dataParams)
            let occName =
                    mkOccName varName $
                    T.unpack $
                    sanitize $ T.intercalate "." $ LF.unTypeConName dataTypeCon
            let dataDecl =
                    noLoc $
                    TyClD NoExt $
                    DataDecl
                        { tcdDExt = NoExt
                        , tcdLName = noLoc $ mkRdrUnqual occName
                        , tcdTyVars =
                              HsQTvs
                                  { hsq_ext = NoExt
                                  , hsq_explicit =
                                        [ mkUserTyVar $ LF.unTypeVarName tyVarName
                                        | (tyVarName, _kind) <- dataParams
                                        ]
                                  }
                        , tcdFixity = Prefix
                        , tcdDataDefn =
                              HsDataDefn
                                  { dd_ext = NoExt
                                  , dd_ND = DataType
                                  , dd_ctxt = noLoc []
                                  , dd_cType = Nothing
                                  , dd_kindSig = Nothing
                                  , dd_cons = convDataCons dataTypeCon dataCons
                                  , dd_derivs = noLoc []
                                  }
                        }
            -- dummy template instance to make sure we get a template instance in the interface
            -- file
            let templInstDecl =
                    noLoc $
                    InstD NoExt $
                    ClsInstD
                        NoExt
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
                                                                                                      NoExt $
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
    convDataCons :: LF.TypeConName -> LF.DataCons -> [LConDecl GhcPs]
    convDataCons dataTypeCon =
        \case
            LF.DataRecord fields ->
                [ noLoc $
                  ConDeclH98
                      { con_ext = NoExt
                      , con_name =
                            noLoc $
                            mkRdrUnqual $
                            mkOccName dataName $
                            T.unpack $
                            sanitize $
                            T.intercalate "." $ LF.unTypeConName dataTypeCon
                      , con_forall = noLoc False
                      , con_ex_tvs = []
                      , con_mb_cxt = Nothing
                      , con_doc = Nothing
                      , con_args =
                            RecCon $
                            noLoc
                                [ noLoc $
                                ConDeclField
                                    { cd_fld_ext = NoExt
                                    , cd_fld_doc = Nothing
                                    , cd_fld_names =
                                          [ noLoc $
                                            FieldOcc
                                                { extFieldOcc = NoExt
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
                    { con_ext = NoExt
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
                                  PrefixCon
                                      [ noLoc $
                                        HsParTy
                                            NoExt
                                            (noLoc $ convType otherTy)
                                      ]
                    }
                | (conName, ty) <- cons
                ]
            LF.DataEnum cons ->
                [ noLoc $
                ConDeclH98
                    { con_ext = NoExt
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
        UserTyVar NoExt . noLoc . mkRdrUnqual . mkOccName tvName . T.unpack
    convType :: LF.Type -> HsType GhcPs
    convType =
        \case
            LF.TVar tyVarName ->
                HsTyVar NoExt NotPromoted $
                mkRdrName $ LF.unTypeVarName tyVarName
            LF.TCon LF.Qualified {..} ->
                HsTyVar NoExt NotPromoted $
                noLoc $
                mkOrig
                    (mkModule
                         (getUnitId qualPackage)
                         (mkModuleName $
                          T.unpack $ LF.moduleNameString qualModule))
                    (mkOccName varName $
                     T.unpack $ T.intercalate "." $ LF.unTypeConName qualObject)
            LF.TApp ty1 ty2 ->
                HsAppTy NoExt (noLoc $ convType ty1) (noLoc $ convType ty2)
            LF.TBuiltin builtinTy -> convBuiltInTy builtinTy
            LF.TForall {..} ->
                HsForAllTy
                    NoExt
                    [mkUserTyVar $ LF.unTypeVarName $ fst forallBinder]
                    (noLoc $ convType forallBody)
            -- TODO (drsk): Is this the correct tuple type? What about the field names?
            LF.TTuple fls ->
                HsTupleTy
                    NoExt
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
            LF.BTUnit -> mkTyConType unitTyCon
            LF.BTBool -> mkTyConType boolTyCon
            LF.BTList -> mkTyConType listTyCon
            LF.BTUpdate -> mkLfInternalType "Update"
            LF.BTScenario -> mkLfInternalType "Scenario"
            LF.BTContractId -> mkLfInternalType "ContractId"
            LF.BTOptional -> mkLfInternalPrelude "Optional"
            LF.BTMap -> mkLfInternalType "TextMap"
            LF.BTArrow -> mkTyConType funTyCon
    mkGhcType =
        HsTyVar NoExt NotPromoted .
        noLoc . mkOrig gHC_TYPES . mkOccName varName
    damlStdlibUnitId = stringToUnitId "daml-stdlib"
    mkLfInternalType =
        HsTyVar NoExt NotPromoted .
        noLoc .
        mkOrig (mkModule damlStdlibUnitId $ mkModuleName "DA.Internal.LF") .
        mkOccName varName
    mkLfInternalPrelude =
        HsTyVar NoExt NotPromoted .
        noLoc .
        mkOrig (mkModule damlStdlibUnitId $ mkModuleName "DA.Internal.Prelude") .
        mkOccName varName
    mkTyConType :: TyCon -> HsType GhcPs
    mkTyConType tyCon =
        let name = getName tyCon
         in HsTyVar NoExt NotPromoted . noLoc $
            mkOrig (nameModule name) (occName name)
    mkGhcPrimImport :: Bool -> String -> LImportDecl GhcPs
    mkGhcPrimImport qualified  modName =  noLoc $
           ImportDecl
               { ideclExt = NoExt
               , ideclSourceSrc = NoSourceText
               , ideclName = noLoc $ mkModuleName modName
               , ideclPkgQual =
                     Just $ StringLiteral NoSourceText $ mkFastString "daml-prim"
               , ideclSource = False
               , ideclSafe = False
               , ideclImplicit = False
               , ideclQualified = qualified
               , ideclAs = Nothing
               , ideclHiding = Nothing
               }
    imports =
        -- first, imports that we need in any case
        map (mkGhcPrimImport True) ["GHC.Types", "GHC.Err"] ++
        -- qualified imports from daml-prim
        map (mkGhcPrimImport False) ["Data.String"] ++
        -- unqualified importts from daml-prim
        [ noLoc $
        ImportDecl
            { ideclExt = NoExt
            , ideclSourceSrc = NoSourceText
            , ideclName =
                  noLoc $ mkModuleName $ T.unpack $ LF.moduleNameString modRef
            , ideclPkgQual =
                  Just $ StringLiteral NoSourceText $ unitIdFS $ getUnitId pkgRef
            , ideclSource = False
            , ideclSafe = False
            , ideclImplicit = False
            , ideclQualified = True
            , ideclAs = Nothing
            , ideclHiding = Nothing
            } :: LImportDecl GhcPs
        | (pkgRef@(LF.PRImport pkgId), modRef) <-
              nubSort $ toListOf moduleModuleRef m
        , pkgId /= thisPkgId
        ]
        -- imports needed by the module declarations
