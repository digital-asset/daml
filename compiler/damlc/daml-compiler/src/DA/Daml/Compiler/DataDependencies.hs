-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DataDependencies
    ( generateSrcPkgFromLf
    , generateGenInstancesPkgFromLf
    , splitUnitId
    ) where

import Control.Lens (toListOf)
import Control.Lens.MonoTraversal (monoTraverse)
import Control.Monad
import Data.List.Extra
import Data.Set (Set)
import qualified Data.Set as Set
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
import "ghc-lib-parser" TcEvidence (HsWrapper (WpHole))
import "ghc-lib-parser" TysPrim
import "ghc-lib-parser" TysWiredIn

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics
import DA.Daml.Preprocessor.Generics
import SdkVersion

data Env = Env
    { envPkgs :: MS.Map UnitId LF.Package
    , envGetUnitId :: LF.PackageRef -> UnitId
    , envStablePackages :: Set LF.PackageId
    , envQualify :: Bool
    , envSdkPrefix :: Maybe String
    , envMod :: LF.Module
    }

data ModRef = ModRef
    { modRefIsStable :: Bool
    , modRefUnitId :: UnitId
    , modRefModule :: LF.ModuleName
    } deriving (Eq, Ord)

-- | Extract all data defintions from a daml-lf module and generate a haskell source file from it.
generateSrcFromLf ::
       Env
    -> ParsedSource
generateSrcFromLf env = noLoc mod
  where
    lfModName = LF.moduleName $ envMod env
    ghcModName = mkModuleName $ T.unpack $ LF.moduleNameString lfModName
    unitId = envGetUnitId env LF.PRSelf
    thisModule = mkModule unitId ghcModName
    mod =
        HsModule
            { hsmodImports = imports
            , hsmodName = Just (noLoc ghcModName)
            , hsmodDecls = decls
            , hsmodDeprecMessage = Nothing
            , hsmodHaddockModHeader = Nothing
            , hsmodExports = Nothing
            }

    decls = classDecls ++ dataTypeDecls ++ valueDecls

    classMethodNames :: Set T.Text
    classMethodNames = Set.fromList
        [ methodName
        | LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        , LF.TStruct fields <- [synType]
        , (fieldName, _) <- fields
        , Just methodName <- [getClassMethodName fieldName]
        ]

    classDecls :: [LHsDecl GhcPs]
    classDecls = do
        LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        LF.TStruct fields <- [synType]
        LF.TypeSynName [name] <- [synName]

        let supers =
                [ convType env fieldType
                | (fieldName, LF.TUnit LF.:-> fieldType) <- fields
                , isSuperClassField fieldName
                ]

            methods =
                [ (methodName, convType env fieldType)
                | (fieldName, LF.TUnit LF.:-> fieldType) <- fields
                , Just methodName <- [getClassMethodName fieldName]
                ]
            occName = mkOccName clsName . T.unpack $ sanitize name

        [ noLoc . TyClD noExt $ ClassDecl
            { tcdCExt = noExt
            , tcdCtxt = noLoc (map noLoc supers)
            , tcdLName = noLoc $ mkRdrUnqual occName
            , tcdTyVars = HsQTvs noExt (map (convTyVarBinder env) synParams)
            , tcdFixity = Prefix
            , tcdFDs = [] -- can't reconstruct fundeps without LF annotations
            , tcdSigs =
                [ noLoc $ ClassOpSig noExt False
                    [mkRdrName methodName]
                    (HsIB noExt (noLoc methodType))
                | (methodName, methodType) <- methods
                ]
            , tcdMeths = emptyBag -- can't reconstruct default methods yet
            , tcdATs = [] -- associated types not supported
            , tcdATDefs = []
            , tcdDocs = []
            } ]

    dataTypeDecls = do
        dtype@LF.DefDataType {..} <- NM.toList $ LF.moduleDataTypes $ envMod env
        guard $ shouldExposeDefDataType dtype
        let numberOfNameComponents = length (LF.unTypeConName dataTypeCon)
        -- we should never encounter more than two name components in dalfs.
        unless (numberOfNameComponents <= 2) $
            errTooManyNameComponents $ LF.unTypeConName dataTypeCon
        -- skip generated data types of sums of products construction in daml-lf
        -- the type will be inlined into the definition of the variant in
        -- convDataCons.
        [dataTypeCon0] <- [LF.unTypeConName dataTypeCon]
        let occName = mkOccName varName $ T.unpack $ sanitize dataTypeCon0
        pure $ mkDataDecl env thisModule occName dataParams (convDataCons dataTypeCon0 dataCons)

    valueDecls = do
        dval@LF.DefValue {..} <- NM.toList $ LF.moduleValues $ envMod env
        guard $ shouldExposeDefValue dval
        let (lfName, lfType) = dvalBinder
            ltype = noLoc $ convType env lfType :: LHsType GhcPs
            lname = mkRdrName (LF.unExprValName lfName) :: Located RdrName
            sig = TypeSig noExt [lname] (HsWC noExt $ HsIB noExt ltype)
            lsigD = noLoc $ SigD noExt sig :: LHsDecl GhcPs
            lexpr = noLoc $ HsPar noExt $ noLoc $ HsVar noExt lname :: LHsExpr GhcPs
            lgrhs = noLoc $ GRHS noExt [] lexpr :: LGRHS GhcPs (LHsExpr GhcPs)
            grhss = GRHSs noExt [lgrhs] (noLoc $ EmptyLocalBinds noExt)
            matchContext = FunRhs lname Prefix NoSrcStrict
            lmatch = noLoc $ Match noExt matchContext [] Nothing grhss
            lalts = noLoc [lmatch]
            bind = FunBind noExt lname (MG noExt lalts Generated) WpHole []
            lvalD = noLoc $ ValD noExt bind :: LHsDecl GhcPs
        [ lsigD, lvalD ]

    shouldExposeDefDataType :: LF.DefDataType -> Bool
    shouldExposeDefDataType typeDef
        = not (defDataTypeIsOldTypeClass typeDef)

    shouldExposeDefValue :: LF.DefValue -> Bool
    shouldExposeDefValue LF.DefValue{..}
        | (lfName, lfType) <- dvalBinder
        = not ("$" `T.isPrefixOf` LF.unExprValName lfName)
        && not (typeHasOldTypeclass env lfType)
        && (LF.moduleNameString lfModName /= "GHC.Prim")
        && not (LF.unExprValName lfName `Set.member` classMethodNames)

    convDataCons :: T.Text -> LF.DataCons -> [LConDecl GhcPs]
    convDataCons dataTypeCon0 = \case
            LF.DataRecord fields ->
                [ mkConDecl env thisModule (mkOccName dataName $ T.unpack $ sanitize dataTypeCon0) $
                      RecCon $ noLoc $ map (uncurry $ mkConDeclField env) fields
                ]
            LF.DataVariant cons ->
                [ mkConDecl env thisModule (mkOccName varName $ T.unpack $ sanitize $ LF.unVariantConName conName)
                    (let t = convType env ty
                     in case (t :: HsType GhcPs) of
                            -- In DAML we have sums of products, in DAML-LF a variant only has a single field.
                            -- Here we combine them back into a single type.
                            HsRecTy _ext fs -> RecCon $ noLoc fs
                            _other -> PrefixCon [noLoc t]
                    )
                | (conName, ty) <- cons
                ]
            LF.DataEnum cons ->
                [ mkConDecl env thisModule (mkOccName varName $ T.unpack $ sanitize $ LF.unVariantConName conName)
                    (PrefixCon [])
                | conName <- cons
                ]

    -- imports needed by the module declarations
    imports
     =
        [ noLoc $
        ImportDecl
            { ideclExt = noExt
            , ideclSourceSrc = NoSourceText
            , ideclName =
                  noLoc $ mkModuleName $ T.unpack $ LF.moduleNameString modRefModule
            , ideclPkgQual = do
                guard $ not modRefIsStable -- we don’t do package qualified imports
                    -- for modules that should come from the current SDK.
                Just $ StringLiteral NoSourceText $ mkFastString $
                    -- Package qualified imports for the current package
                    -- need to use "this" instead of the package id.
                    if modRefUnitId == unitId
                        then "this"
                        else fst (splitUnitId $ unitIdString modRefUnitId)
            , ideclSource = False
            , ideclSafe = False
            , ideclImplicit = False
            , ideclQualified = True
            , ideclAs = Nothing
            , ideclHiding = Nothing
            } :: LImportDecl GhcPs
        | ModRef{..} <- Set.toList modRefs
         -- don’t import ourselves
        , not (modRefModule == lfModName && modRefUnitId == unitId)
        -- GHC.Prim doesn’t need to and cannot be explicitly imported (it is not exposed since the interface file is black magic
        -- hardcoded in GHC).
        , modRefModule /= LF.ModuleName ["CurrentSdk", "GHC", "Prim"]
        ]
    isStable LF.PRSelf = False
    isStable (LF.PRImport pkgId) = pkgId `Set.member` envStablePackages env

    modRefs :: Set ModRef
    modRefs = Set.unions
        [ foldMap modRefsFromDefTypeSyn (LF.moduleSynonyms (envMod env))
        , foldMap modRefsFromDefDataType (LF.moduleDataTypes (envMod env))
        , foldMap modRefsFromDefValue (LF.moduleValues (envMod env))
        ]

    mkModRef :: LF.PackageRef -> LF.ModuleName -> ModRef
    mkModRef pkg mod = ModRef (isStable pkg) (envGetUnitId env pkg)
        (addSdkPrefixIfStable env pkg mod)

    modRefsFromDefDataType :: LF.DefDataType -> Set ModRef
    modRefsFromDefDataType typeDef@LF.DefDataType{..} =
        if shouldExposeDefDataType typeDef
            then Set.fromList $ concat
                [ map (uncurry mkModRef) (toListOf monoTraverse typeDef)
                , map builtinTypeToModRef (toListOf (dataConsType . builtinType) dataCons)
                , [ ModRef True primUnitId sdkGhcTypes
                    | LF.DataEnum [_] <- [dataCons]
                    ] -- ^ single constructor enums spawn a reference to
                      -- CurrentSdk.GHC.Types.DamlEnum in the daml-preprocessor.
                ]
            else Set.empty

    modRefsFromDefTypeSyn :: LF.DefTypeSyn -> Set ModRef
    modRefsFromDefTypeSyn LF.DefTypeSyn{..} =
        modRefsFromType synType

    modRefsFromDefValue :: LF.DefValue -> Set ModRef
    modRefsFromDefValue dval@LF.DefValue{..} | (_, dvalType) <- dvalBinder =
        if shouldExposeDefValue dval
            then modRefsFromType dvalType
            else Set.empty

    modRefsFromType :: LF.Type -> Set ModRef
    modRefsFromType ty = Set.fromList $ concat
        [ map (uncurry mkModRef) (toListOf monoTraverse ty)
        , map builtinTypeToModRef (toListOf builtinType ty)
        ]

    builtinTypeToModRef :: LF.BuiltinType -> ModRef
    builtinTypeToModRef = uncurry (ModRef True) . \case
        LF.BTInt64 -> (primUnitId, sdkGhcTypes)
        LF.BTDecimal -> (primUnitId, sdkGhcTypes)
        LF.BTText -> (primUnitId, sdkGhcTypes)
        LF.BTTimestamp -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTDate -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTParty -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTUnit -> (primUnitId, sdkGhcTuple)
        LF.BTBool -> (primUnitId, sdkGhcTypes)
        LF.BTList -> (primUnitId, sdkGhcTypes)
        LF.BTUpdate -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTScenario -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTContractId -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTOptional -> (damlStdlibUnitId, sdkInternalPrelude)
        LF.BTTextMap -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTGenMap -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTArrow -> (primUnitId, sdkGhcPrim)
        LF.BTNumeric -> (primUnitId, sdkGhcTypes)
        LF.BTAny -> (damlStdlibUnitId, sdkDaInternalLf)
        LF.BTTypeRep -> (damlStdlibUnitId, sdkDaInternalLf)

    sdkDaInternalLf = LF.ModuleName $  sdkPrefix ++ ["DA", "Internal", "LF"]
    sdkInternalPrelude = LF.ModuleName $ sdkPrefix ++ ["DA", "Internal", "Prelude"]
    sdkGhcTypes = LF.ModuleName $ sdkPrefix ++ ["GHC", "Types"]
    sdkGhcPrim = LF.ModuleName $ sdkPrefix ++ ["GHC", "Prim"]
    sdkGhcTuple = LF.ModuleName $ sdkPrefix ++ ["GHC", "Tuple"]
    sdkPrefix = [T.pack prefix | Just prefix <- [envSdkPrefix env]]

-- TODO (drsk) how come those '#' appear in daml-lf names?
sanitize :: T.Text -> T.Text
sanitize = T.dropWhileEnd (== '#')

mkConRdr :: Env -> Module -> OccName -> RdrName
mkConRdr env thisModule
 | envQualify env = mkRdrUnqual
 | otherwise = mkOrig thisModule

mkDataDecl :: Env -> Module -> OccName -> [(LF.TypeVarName, LF.Kind)] -> [LConDecl GhcPs] -> LHsDecl GhcPs
mkDataDecl env thisModule occName tyVars cons = noLoc $ TyClD noExt $ DataDecl
    { tcdDExt = noExt
    , tcdLName = noLoc $ mkConRdr env thisModule occName
    , tcdTyVars =
          HsQTvs
              { hsq_ext = noExt
              , hsq_explicit = map (convTyVarBinder env) tyVars
              }
    , tcdFixity = Prefix
    , tcdDataDefn =
          HsDataDefn
              { dd_ext = noExt
              , dd_ND = DataType
              , dd_ctxt = noLoc []
              , dd_cType = Nothing
              , dd_kindSig = Nothing
              , dd_cons = cons
              , dd_derivs = noLoc []
              }
    }

mkConDecl :: Env -> Module -> OccName -> HsConDeclDetails GhcPs -> LConDecl GhcPs
mkConDecl env thisModule conName details = noLoc $ ConDeclH98
    { con_ext = noExt
    , con_name = noLoc $ mkConRdr env thisModule conName
    , con_forall = noLoc False -- No foralls from existentials
    , con_ex_tvs = [] -- No existential type vars.
    , con_mb_cxt = Nothing
    , con_doc = Nothing
    , con_args = details
    }

mkConDeclField :: Env -> LF.FieldName -> LF.Type -> LConDeclField GhcPs
mkConDeclField env fieldName fieldTy = noLoc $ ConDeclField
    { cd_fld_ext = noExt
    , cd_fld_doc = Nothing
    , cd_fld_names =
      [ noLoc $ FieldOcc { extFieldOcc = noExt, rdrNameFieldOcc = mkRdrName $ LF.unFieldName fieldName } ]
    , cd_fld_type = noLoc $ convType env fieldTy
    }

isConstraint :: LF.Type -> Bool
isConstraint = \case
    LF.TSynApp _ _ -> True
    LF.TStruct fields -> and
        [ isSuperClassField fieldName && isConstraint fieldType
        | (fieldName, fieldType) <- fields
        ]
    _ -> False

convType :: Env -> LF.Type -> HsType GhcPs
convType env =
    \case
        LF.TVar tyVarName ->
            HsTyVar noExt NotPromoted $ mkRdrName $ LF.unTypeVarName tyVarName

        ty1 LF.:-> ty2
            | isConstraint ty1 ->
                HsQualTy noExt (noLoc [noLoc $ convType env ty1]) (noLoc $ convType env ty2)
            | otherwise ->
                HsFunTy noExt (noLoc $ convType env ty1) (noLoc $ convType env ty2)

        LF.TSynApp LF.Qualified{..} args ->
            let tycls = case LF.unTypeSynName qualObject of
                    [name] ->
                        HsTyVar noExt NotPromoted $ noLoc $ mkOrig
                            (mkModule
                                (envGetUnitId env qualPackage)
                                (mkModuleName $ T.unpack $ LF.moduleNameString
                                (addSdkPrefixIfStable env qualPackage qualModule)))
                        (mkOccName clsName $ T.unpack name)
                    ns -> error ("DamlDependencies: unexpected typeclass name " <> show ns)
            in foldl (HsAppTy noExt . noLoc) tycls $ map (noLoc . convType env) args

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
                             (mkModuleName $ T.unpack $ LF.moduleNameString
                                (addSdkPrefixIfStable env qualPackage qualModule)))
                        (mkOccName varName $ T.unpack name)
                n@[_name0, _name1] -> case MS.lookup n (sumProdRecords $ envMod env) of
                    Nothing ->
                        error $ "Internal error: Could not find generated record type: " <> T.unpack (T.intercalate "." n)
                    Just fs -> HsRecTy noExt $ map (uncurry $ mkConDeclField env) fs
                cs -> errTooManyNameComponents cs
        LF.TApp ty1 ty2 ->
            HsParTy noExt $
            noLoc $ HsAppTy noExt (noLoc $ convType env ty1) (noLoc $ convType env ty2)
        LF.TBuiltin builtinTy -> convBuiltInTy env builtinTy
        LF.TForall {..} ->
            HsParTy noExt $
            noLoc $
            HsForAllTy
                noExt
                [convTyVarBinder env forallBinder]
                (noLoc $ convType env forallBody)
        ty@(LF.TStruct fls) ->
            HsTupleTy
                noExt
                (if isConstraint ty then HsConstraintTuple else HsBoxedTuple)
                [noLoc $ convType env ty | (_fldName, ty) <- fls]
        LF.TNat n ->
            HsTyLit noExt (HsNumTy NoSourceText (LF.fromTypeLevelNat n))
  where
    mkTuple :: Int -> HsType GhcPs
    mkTuple i =
        HsTyVar noExt NotPromoted $
        noLoc $ mkRdrUnqual $ occName $ tupleTyConName BoxedTuple i


addSdkPrefixIfStable :: Env -> LF.PackageRef -> LF.ModuleName -> LF.ModuleName
addSdkPrefixIfStable _ LF.PRSelf mod = mod
addSdkPrefixIfStable env (LF.PRImport pkgId) m@(LF.ModuleName n)
    | pkgId `Set.member` envStablePackages env
    = LF.ModuleName (sdkPrefix ++ n)

    | otherwise
    = m
  where
    sdkPrefix = case envSdkPrefix env of
        Nothing -> []
        Just p -> [T.pack p]

convBuiltInTy :: Env -> LF.BuiltinType -> HsType GhcPs
convBuiltInTy env =
    \case
        LF.BTInt64 -> mkGhcType "Int"
        LF.BTDecimal -> mkGhcType "Decimal"
        LF.BTText -> mkGhcType "Text"
        LF.BTTimestamp -> mkLfInternalType env "Time"
        LF.BTDate -> mkLfInternalType env "Date"
        LF.BTParty -> mkLfInternalType env "Party"
        LF.BTUnit -> mkTyConTypeUnqual unitTyCon
        LF.BTBool -> mkGhcType "Bool"
        LF.BTList -> mkTyConTypeUnqual listTyCon
        LF.BTUpdate -> mkLfInternalType env "Update"
        LF.BTScenario -> mkLfInternalType env "Scenario"
        LF.BTContractId -> mkLfInternalType env "ContractId"
        LF.BTOptional -> mkLfInternalPrelude env "Optional"
        LF.BTTextMap -> mkLfInternalType env "TextMap"
        LF.BTGenMap -> mkLfInternalType env "Map"
        LF.BTArrow -> mkTyConTypeUnqual funTyCon
        LF.BTNumeric -> mkGhcType "Numeric"
        LF.BTAny -> mkLfInternalType env "Any"
        LF.BTTypeRep -> mkLfInternalType env "TypeRep"

errTooManyNameComponents :: [T.Text] -> a
errTooManyNameComponents cs =
    error $
    "Internal error: Dalf contains type constructors with more than two name components: " <>
    (T.unpack $ T.intercalate "." cs)

convKind :: Env -> LF.Kind -> LHsKind GhcPs
convKind env = \case
    LF.KStar -> noLoc $ HsStarTy noExt False
    LF.KNat -> noLoc $ mkGhcType "Nat"
    LF.KArrow k1 k2 -> noLoc $ HsFunTy noExt (convKind env k1) (convKind env k2)

convTyVarBinder :: Env -> (LF.TypeVarName, LF.Kind) -> LHsTyVarBndr GhcPs
convTyVarBinder env = \case
    (LF.TypeVarName tyVar, LF.KStar) ->
        mkUserTyVar tyVar
    (LF.TypeVarName tyVar, kind) ->
        mkKindedTyVar tyVar (convKind env kind)

mkUserTyVar :: T.Text -> LHsTyVarBndr GhcPs
mkUserTyVar =
    noLoc .
    UserTyVar noExt . noLoc . mkRdrUnqual . mkOccName tvName . T.unpack

mkKindedTyVar :: T.Text -> LHsKind GhcPs -> LHsTyVarBndr GhcPs
mkKindedTyVar name = noLoc .
    (KindedTyVar noExt . noLoc . mkRdrUnqual . mkOccName tvName $ T.unpack name)


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
    noLoc . mkOrig (Module primUnitId (mkModuleName "CurrentSdk.GHC.Types")) . mkOccName varName

mkLfInternalType :: Env -> String -> HsType GhcPs
mkLfInternalType env =
    HsTyVar noExt NotPromoted .
    noLoc .
    mkOrig (mkModule damlStdlibUnitId $ mkModuleName $ prefixStdlibImport env "DA.Internal.LF") .
    mkOccName varName

prefixStdlibImport :: Env -> String -> String
prefixStdlibImport env impString = (maybe "" (<> ".") $ envSdkPrefix env) <> impString

mkLfInternalPrelude :: Env -> String -> HsType GhcPs
mkLfInternalPrelude env =
    HsTyVar noExt NotPromoted .
    noLoc .
    mkOrig (mkModule damlStdlibUnitId $ mkModuleName $ prefixStdlibImport env "DA.Internal.Prelude") .
    mkOccName varName

mkTyConTypeUnqual :: TyCon -> HsType GhcPs
mkTyConTypeUnqual = mkTyConType False

-- | Generate the full source for a daml-lf package.
generateSrcPkgFromLf ::
       MS.Map UnitId LF.Package
    -> (LF.PackageRef -> UnitId)
    -> Set LF.PackageId
    -> Maybe String
    -> LF.Package
    -> [(NormalizedFilePath, String)]
generateSrcPkgFromLf pkgs getUnitId stablePkgs mbSdkPrefix pkg = do
    mod <- NM.toList $ LF.packageModules pkg
    let fp =
            toNormalizedFilePath $
            (joinPath $ map T.unpack $ LF.unModuleName $ LF.moduleName mod) <.>
            ".daml"
    pure
        ( fp
        , unlines header ++
          (showSDocForUser fakeDynFlags alwaysQualify $
           ppr $ generateSrcFromLf $ env mod))
  where
    env m = Env pkgs getUnitId stablePkgs True mbSdkPrefix m
    header =
        [ "{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE NoImplicitPrelude #-}"
        , "{-# LANGUAGE NoOverloadedStrings #-}"
        , "{-# LANGUAGE TypeOperators #-}"
        , "{-# OPTIONS_GHC -Wno-unused-imports #-}"
        ]


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
    src = unLoc $ generateSrcFromLf env


generateGenInstancesPkgFromLf ::
       (LF.PackageRef -> UnitId)
    -> Set LF.PackageId
    -> Maybe String
    -> LF.PackageId
    -> LF.Package
    -> String
    -> [(NormalizedFilePath, String)]
generateGenInstancesPkgFromLf getUnitId stablePkgs mbSdkPrefix pkgId pkg qual =
    catMaybes
        [ generateGenInstanceModule
            Env
                { envPkgs = MS.empty -- for now at least, since this doesn't care about type definitions in other packages like generateSrcFromLf does
                , envGetUnitId = getUnitId
                , envStablePackages = stablePkgs
                , envQualify = False
                , envMod = mod
                , envSdkPrefix = mbSdkPrefix
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
        , "{-# OPTIONS_GHC -Wno-unused-imports #-}"
        , "module " <> modNameQual <> "GenInstances" <> " where"
        ]
    imports =
        [ "import qualified " <> modNameQual
        , "import qualified DA.Generics"
        ]

-- | Take a string of the form daml-stdlib-"0.13.43" and split it into ("daml-stdlib", Just "0.13.43")
splitUnitId :: String -> (String, Maybe String)
splitUnitId unitId = fromMaybe (unitId, Nothing) $ do
    (name, ver) <- stripInfixEnd "-" unitId
    guard $ all (`elem` '.' : ['0' .. '9']) ver
    pure (name, Just ver)

-- | Returns 'True' if an LF type contains a reference to an
-- old-style typeclass. See 'tconIsOldTypeclass' for more details.
typeHasOldTypeclass :: Env -> LF.Type -> Bool
typeHasOldTypeclass env = \case
    LF.TVar _ -> False
    LF.TCon tcon -> tconIsOldTypeclass env tcon
    LF.TSynApp _ _ -> False
        -- Type synonyms came after the switch to new-style
        -- typeclasses, so we can assume there are no old-style
        -- typeclasses being referenced.
    LF.TApp a b -> typeHasOldTypeclass env a || typeHasOldTypeclass env b
    LF.TBuiltin _ -> False
    LF.TForall _ b -> typeHasOldTypeclass env b
    LF.TStruct fields -> any (typeHasOldTypeclass env . snd) fields
    LF.TNat _ -> False

-- | Determine whether a typecon refers to an old-style
-- typeclass. By "old-style" I mean a typeclass based on
-- nominal LF record types. There's no foolproof way of
-- determining this, but we can approximate it by taking
-- advantage of the typeclass sanitization that is introduced
-- during LF conversion: every field in a typeclass dictionary
-- is represented by a type @() -> _@ due to sanitization.
-- Thus, if a type is given by a record, and the record has
-- at least one field, and every field in the record is a
-- function from unit, then there's a good chance this
-- record represents an old-style typeclass.
--
-- The caveat here is that if a user creates a record that
-- matches these criteria, it will be treated as an old-style
-- typeclass and therefore any functions that use this type
-- will not be exposed via the data-dependency mechanism,
-- (see 'generateSrcFromLf').
tconIsOldTypeclass :: Env -> LF.Qualified LF.TypeConName -> Bool
tconIsOldTypeclass env tcon =
    case envLookupDataType tcon env of
        Nothing -> error ("Unknown reference to type " <> show tcon)
        Just dtype -> defDataTypeIsOldTypeClass dtype

defDataTypeIsOldTypeClass :: LF.DefDataType -> Bool
defDataTypeIsOldTypeClass LF.DefDataType{..}
    | LF.DataRecord fields <- dataCons
    = notNull fields && all isDesugarField fields

    | otherwise
    = False
  where
    isDesugarField :: (LF.FieldName, LF.Type) -> Bool
    isDesugarField (_fieldName, fieldType) =
        case fieldType of
            LF.TUnit LF.:-> _ -> True
            _ -> False


envLookupDataType :: LF.Qualified LF.TypeConName -> Env -> Maybe LF.DefDataType
envLookupDataType tcon env = do
    mod <- envLookupModuleOf tcon env
    NM.lookup (LF.qualObject tcon) (LF.moduleDataTypes mod)

envLookupModuleOf :: LF.Qualified a -> Env -> Maybe LF.Module
envLookupModuleOf qual = envLookupModule (LF.qualPackage qual) (LF.qualModule qual)

envLookupModule :: LF.PackageRef -> LF.ModuleName -> Env -> Maybe LF.Module
envLookupModule pkgRef modName env = do
    pkg <- envLookupPackage pkgRef env
    NM.lookup modName (LF.packageModules pkg)

envLookupPackage :: LF.PackageRef -> Env -> Maybe LF.Package
envLookupPackage ref env = MS.lookup (envGetUnitId env ref) (envPkgs env)

getClassMethodName :: LF.FieldName -> Maybe T.Text
getClassMethodName (LF.FieldName fieldName) =
    T.stripPrefix "m_" fieldName

isSuperClassField :: LF.FieldName -> Bool
isSuperClassField (LF.FieldName fieldName) =
    "s_" `T.isPrefixOf` fieldName
