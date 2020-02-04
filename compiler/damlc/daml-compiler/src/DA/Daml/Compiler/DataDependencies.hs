-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DataDependencies
    ( Config (..)
    , generateSrcPkgFromLf
    , generateGenInstancesPkgFromLf
    , splitUnitId
    ) where

import Control.Monad
import Control.Monad.Trans.Writer.CPS
import Data.List.Extra
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Map.Strict as MS
import Data.Maybe
import Data.Either
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
import qualified DA.Daml.LF.Ast.Type as LF
import qualified DA.Daml.LF.TypeChecker.Check as LF
import qualified DA.Daml.LF.TypeChecker.Env as LF

import DA.Daml.Preprocessor.Generics
import SdkVersion

data Config = Config
    { configPackages :: MS.Map UnitId LF.Package
        -- ^ maps unit ids to packages, and contain all package dependencies,
        -- including stable packages and data dependencies.
    , configGetUnitId :: LF.PackageRef -> UnitId
        -- ^ maps a package reference to a unit id
    , configSelfPkgId :: LF.PackageId
        -- ^ package id for this package, we need it to build a closed LF.World
    , configStablePackages :: Set LF.PackageId
        -- ^ set of package ids for stable packages
    , configDependencyPackages :: Set LF.PackageId
        -- ^ set of package ids for dependencies (not data-dependencies)
    , configSdkPrefix :: [T.Text]
        -- ^ prefix to use for current SDK in data-dependencies
    }

data Env = Env
    { envConfig :: Config
    , envQualifyThisModule :: Bool
        -- ^ True if refences to this module should be qualified
    , envWorld :: LF.World
        -- ^ World built from dependencies, stable packages, and current package.
    , envDepClassMap :: DepClassMap
        -- ^ Map of typeclasses from dependencies.
    , envMod :: LF.Module
        -- ^ The module under consideration.
    }

-- | Build an LF world up from the Config data.
buildWorld :: Config -> LF.World
buildWorld Config{..} =
    fromMaybe (error "Failed to build LF World for data-dependencies") $ do
        let packageIds = concat
                [ [configSelfPkgId] -- We need to add this here,
                    -- instead of relying on the self argument below,
                    -- because package references in the current
                    -- package have also been rewritten during decoding.
                , Set.toList configStablePackages
                , Set.toList configDependencyPackages ]
            mkExtPackage pkgId = do
                pkg <- MS.lookup (configGetUnitId (LF.PRImport pkgId)) configPackages
                Just (LF.ExternalPackage pkgId pkg)
        extPackages <- mapM mkExtPackage packageIds
        self <- MS.lookup (configGetUnitId LF.PRSelf) configPackages
        Just (LF.initWorldSelf extPackages self)

envLfVersion :: Env -> LF.Version
envLfVersion = LF.packageLfVersion . LF.getWorldSelf . envWorld

-- | Type classes coming from dependencies. This maps a (module, synonym)
-- name pair to a corresponding dependency package id and synonym definition.
newtype DepClassMap = DepClassMap
    { unDepClassMap :: MS.Map
        (LF.ModuleName, LF.TypeSynName)
        (LF.PackageId, LF.DefTypeSyn)
    }

buildDepClassMap :: Config -> DepClassMap
buildDepClassMap Config{..} = DepClassMap $ MS.fromList
    [ ((moduleName, synName), (packageId, dsyn))
    | packageId <- Set.toList configDependencyPackages
    , let unitId = configGetUnitId (LF.PRImport packageId)
    , Just LF.Package{..} <- [MS.lookup unitId configPackages]
    , LF.Module{..} <- NM.toList packageModules
    , dsyn@LF.DefTypeSyn{..} <- NM.toList moduleSynonyms
    ]

envLookupDepClass :: LF.TypeSynName -> Env -> Maybe (LF.PackageId, LF.DefTypeSyn)
envLookupDepClass synName env =
    let modName = LF.moduleName (envMod env)
        classMap = unDepClassMap (envDepClassMap env)
    in MS.lookup (modName, synName) classMap

-- | Determine whether two type synonym definitions are similar enough to
-- reexport one as the other. This is done by computing alpha equivalence
-- after expanding all type synonyms.
safeToReexport :: Env -> LF.DefTypeSyn -> LF.DefTypeSyn -> Bool
safeToReexport env syn1 syn2 =
    fromRight False $ do
        LF.runGamma (envWorld env) (envLfVersion env) $ do
            esyn1 <- LF.expandTypeSynonyms (closedType syn1)
            esyn2 <- LF.expandTypeSynonyms (closedType syn2)
            pure (LF.alphaEquiv esyn1 esyn2)

  where
    -- | Turn a type synonym definition into a closed type.
    closedType :: LF.DefTypeSyn -> LF.Type
    closedType LF.DefTypeSyn{..} = LF.mkTForalls synParams synType

-- | A module reference coming from DAML-LF.
data ModRef = ModRef
    { modRefIsStable :: Bool
    , modRefUnitId :: UnitId
    , modRefModule :: LF.ModuleName
    } deriving (Eq, Ord)

-- | Monad for generating a value together with its module references.
newtype Gen t = Gen (Writer (Set ModRef) t)
    deriving (Functor, Applicative, Monad)

runGen :: Gen t -> (t, Set ModRef)
runGen (Gen m) = runWriter m

emitModRef :: ModRef -> Gen ()
emitModRef = Gen . tell . Set.singleton

-- | Extract all data definitions from a daml-lf module and generate
-- a haskell source file from it.
generateSrcFromLf ::
       Env
    -> ParsedSource
generateSrcFromLf env = noLoc mod
  where
    config = envConfig env
    lfModName = LF.moduleName $ envMod env
    ghcModName = mkModuleName $ T.unpack $ LF.moduleNameString lfModName
    unitId = configGetUnitId config LF.PRSelf
    thisModule = mkModule unitId ghcModName
    mod =
        HsModule
            { hsmodImports = imports
            , hsmodName = Just (noLoc ghcModName)
            , hsmodDecls = decls
            , hsmodDeprecMessage = Nothing
            , hsmodHaddockModHeader = Nothing
            , hsmodExports = Just (noLoc exports)
            }

    decls :: [LHsDecl GhcPs]
    exports :: [LIE GhcPs]
    modRefs :: Set ModRef
    ((exports, decls), modRefs) = runGen
        ((,) <$> genExports <*> genDecls)

    genExports :: Gen [LIE GhcPs]
    genExports = sequence $ selfReexport : classReexports

    genDecls :: Gen [LHsDecl GhcPs]
    genDecls = sequence . concat $
        [ classDecls
        , dataTypeDecls
        , valueDecls
        , instanceDecls
        ]

    classMethodNames :: Set T.Text
    classMethodNames = Set.fromList
        [ methodName
        | LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        , LF.TStruct fields <- [synType]
        , (fieldName, _) <- fields
        , Just methodName <- [getClassMethodName fieldName]
        ]

    selfReexport :: Gen (LIE GhcPs)
    selfReexport = pure . noLoc $
        IEModuleContents noExt (noLoc ghcModName)

    classReexports :: [Gen (LIE GhcPs)]
    classReexports = map snd (MS.toList classReexportMap)

    classReexportMap :: MS.Map LF.TypeSynName (Gen (LIE GhcPs))
    classReexportMap = MS.fromList $ do
        synDef@LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        LF.TStruct _ <- [synType]
        LF.TypeSynName [name] <- [synName]
        Just (pkgId, depDef) <- [envLookupDepClass synName env]
        guard (safeToReexport env synDef depDef)
        let occName = mkOccName clsName . T.unpack $ sanitize name
        pure . (synName,) $ do
            ghcMod <- genModule env (LF.PRImport pkgId) (LF.moduleName (envMod env))
            pure . noLoc . IEThingAll noExt
                . noLoc . IEName . noLoc
                $ mkOrig ghcMod occName

    classDecls :: [Gen (LHsDecl GhcPs)]
    classDecls = do
        LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        LF.TStruct fields <- [synType]
        LF.TypeSynName [name] <- [synName]
        guard (synName `MS.notMember` classReexportMap)
        let occName = mkOccName clsName . T.unpack $ sanitize name
        pure $ do
            supers <- sequence
                [ convType env fieldType
                | (fieldName, LF.TUnit LF.:-> fieldType) <- fields
                , isSuperClassField fieldName
                ]
            methods <- sequence
                [ (methodName,) <$> convType env fieldType
                | (fieldName, LF.TUnit LF.:-> fieldType) <- fields
                , Just methodName <- [getClassMethodName fieldName]
                ]
            params <- mapM (convTyVarBinder env) synParams
            pure . noLoc . TyClD noExt $ ClassDecl
                { tcdCExt = noExt
                , tcdCtxt = noLoc (map noLoc supers)
                , tcdLName = noLoc $ mkRdrUnqual occName
                , tcdTyVars = HsQTvs noExt params
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
                }

    dataTypeDecls :: [Gen (LHsDecl GhcPs)]
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
        [ mkDataDecl env thisModule occName dataParams =<<
            convDataCons dataTypeCon0 dataCons ]

    valueDecls :: [Gen (LHsDecl GhcPs)]
    valueDecls = do
        dval@LF.DefValue {..} <- NM.toList $ LF.moduleValues $ envMod env
        guard $ shouldExposeDefValue dval
        let (lfName, lfType) = dvalBinder
            ltype = noLoc <$> convType env lfType :: Gen (LHsType GhcPs)
            lname = mkRdrName (LF.unExprValName lfName) :: Located RdrName
            sig = TypeSig noExt [lname] . HsWC noExt . HsIB noExt <$> ltype
            lsigD = noLoc . SigD noExt <$> sig :: Gen (LHsDecl GhcPs)
            bind = mkStubBind lname
            lvalD = noLoc $ ValD noExt bind :: LHsDecl GhcPs
        [ lsigD, pure lvalD ]

    -- | Generate instance declarations from dictionary functions.
    instanceDecls :: [Gen (LHsDecl GhcPs)]
    instanceDecls = do
        LF.DefValue {..} <- NM.toList $ LF.moduleValues $ envMod env
        Just dfunSig <- [getDFunSig dvalBinder]
        guard (isDFunBody dvalBody)
        pure $ do
            polyTy <- HsIB noExt . noLoc <$> convDFunSig env dfunSig
            pure . noLoc . InstD noExt . ClsInstD noExt $ ClsInstDecl
                { cid_ext = noExt
                , cid_poly_ty = polyTy
                , cid_binds = emptyBag
                , cid_sigs = []
                , cid_tyfam_insts = []
                , cid_datafam_insts = []
                , cid_overlap_mode = Nothing
                }
      where
        -- | Check that the body matches that of a dictionary function,
        -- as opposed to a superclass projection function.
        isDFunBody :: LF.Expr -> Bool
        isDFunBody = \case
            LF.ETyLam _ body -> isDFunBody body
            LF.ETmLam _ body -> isDFunBody body
            LF.EStructCon _ -> True
            _ -> False


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

    convDataCons :: T.Text -> LF.DataCons -> Gen [LConDecl GhcPs]
    convDataCons dataTypeCon0 = \case
        LF.DataRecord fields -> do
            fields' <- mapM (uncurry (mkConDeclField env)) fields
            pure [ mkConDecl occName (RecCon (noLoc fields')) ]
        LF.DataVariant cons -> do
            sequence
                [ mkConDecl (occNameFor conName) . details <$> convType env ty
                | (conName, ty) <- cons
                ]
        LF.DataEnum cons -> do
            when (length cons == 1) (void $ mkGhcType env "DamlEnum")
                -- ^ Single constructor enums spawn a reference to
                -- GHC.Types.DamlEnum in the daml-preprocessor.
            pure
                [ mkConDecl (occNameFor conName) (PrefixCon [])
                | conName <- cons
                ]
      where
        occName = mkOccName varName $ T.unpack (sanitize dataTypeCon0)
        occNameFor (LF.VariantConName c) = mkOccName varName $ T.unpack (sanitize c)

        mkConDecl :: OccName -> HsConDeclDetails GhcPs -> LConDecl GhcPs
        mkConDecl conName details = noLoc $ ConDeclH98
            { con_ext = noExt
            , con_name = noLoc $ mkConRdr env thisModule conName
            , con_forall = noLoc False -- No foralls from existentials
            , con_ex_tvs = [] -- No existential type vars.
            , con_mb_cxt = Nothing
            , con_doc = Nothing
            , con_args = details
            }

        -- In DAML we have sums of products, in DAML-LF a variant only has
        -- a single field. Here we combine them back into a single type.
        details :: HsType GhcPs -> HsConDeclDetails GhcPs
        details = \case
            HsRecTy _ext fs -> RecCon $ noLoc fs
            ty -> PrefixCon [noLoc ty]


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

-- TODO (drsk) how come those '#' appear in daml-lf names?
sanitize :: T.Text -> T.Text
sanitize = T.dropWhileEnd (== '#')

mkConRdr :: Env -> Module -> OccName -> RdrName
mkConRdr env thisModule
 | envQualifyThisModule env = mkOrig thisModule
 | otherwise = mkRdrUnqual

mkDataDecl :: Env -> Module -> OccName -> [(LF.TypeVarName, LF.Kind)] -> [LConDecl GhcPs] -> Gen (LHsDecl GhcPs)
mkDataDecl env thisModule occName tyVars cons = do
    tyVars' <- mapM (convTyVarBinder env) tyVars
    pure . noLoc . TyClD noExt $ DataDecl
        { tcdDExt = noExt
        , tcdLName = noLoc $ mkConRdr env thisModule occName
        , tcdTyVars = HsQTvs noExt tyVars'
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

-- | Make a binding of the form "x = x". If a qualified name is passed,
-- we turn the left-hand side into the unqualified form of that name (LHS
-- must always be unqualified), and the right-hand side remains qualified.
mkStubBind :: Located RdrName -> HsBind GhcPs
mkStubBind lname =
    let lexpr = noLoc $ HsPar noExt $ noLoc $ HsVar noExt lname :: LHsExpr GhcPs
        lgrhs = noLoc $ GRHS noExt [] lexpr :: LGRHS GhcPs (LHsExpr GhcPs)
        grhss = GRHSs noExt [lgrhs] (noLoc $ EmptyLocalBinds noExt)
        lnameUnqual = noLoc . mkRdrUnqual . rdrNameOcc $ unLoc lname
        matchContext = FunRhs lnameUnqual Prefix NoSrcStrict
        lmatch = noLoc $ Match noExt matchContext [] Nothing grhss
        lalts = noLoc [lmatch]
        bind = FunBind noExt lnameUnqual (MG noExt lalts Generated) WpHole []
    in bind

mkConDeclField :: Env -> LF.FieldName -> LF.Type -> Gen (LConDeclField GhcPs)
mkConDeclField env fieldName fieldTy = do
    fieldTy' <- convType env fieldTy
    pure . noLoc $ ConDeclField
        { cd_fld_ext = noExt
        , cd_fld_doc = Nothing
        , cd_fld_names =
            [ noLoc $ FieldOcc { extFieldOcc = noExt, rdrNameFieldOcc = mkRdrName $ LF.unFieldName fieldName } ]
        , cd_fld_type = noLoc fieldTy'
        }

isConstraint :: LF.Type -> Bool
isConstraint = \case
    LF.TSynApp _ _ -> True
    LF.TStruct fields -> and
        [ isSuperClassField fieldName && isConstraint fieldType
        | (fieldName, fieldType) <- fields
        ]
    _ -> False

genModule :: Env -> LF.PackageRef -> LF.ModuleName -> Gen Module
genModule env pkgRef modName = do
    let Config{..} = envConfig env
        isStable
            | LF.PRSelf <- pkgRef = False
            | LF.PRImport pkgId <- pkgRef =
                pkgId `Set.member` configStablePackages
        unitId = configGetUnitId pkgRef
    genModuleAux env isStable unitId modName

genStableModule :: Env -> UnitId -> LF.ModuleName -> Gen Module
genStableModule env = genModuleAux env True

genModuleAux :: Env -> Bool -> UnitId -> LF.ModuleName -> Gen Module
genModuleAux env isStable unitId modName = do
    let sdkPrefix = configSdkPrefix (envConfig env)
        newModName
            | isStable = LF.ModuleName (sdkPrefix ++ LF.unModuleName modName)
            | otherwise = modName
        ghcModName = mkModuleName . T.unpack $ LF.moduleNameString newModName
        modRef = ModRef isStable unitId newModName
    emitModRef modRef
    pure $ mkModule unitId ghcModName

convType :: Env -> LF.Type -> Gen (HsType GhcPs)
convType env =
    \case
        LF.TVar tyVarName -> pure $
            HsTyVar noExt NotPromoted $ mkRdrName $ LF.unTypeVarName tyVarName

        ty1 LF.:-> ty2 -> do
            ty1' <- convType env ty1
            ty2' <- convType env ty2
            pure $ if isConstraint ty1
                then HsQualTy noExt (noLoc [noLoc ty1']) (noLoc ty2')
                else HsFunTy noExt (noLoc ty1') (noLoc ty2')

        LF.TSynApp LF.Qualified{..} lfArgs -> do
            ghcMod <- genModule env qualPackage qualModule
            let tyname = case LF.unTypeSynName qualObject of
                    [n] -> n
                    ns -> error ("DamlDependencies: unexpected typeclass name " <> show ns)
                tyvar = HsTyVar noExt NotPromoted . noLoc
                    . mkOrig ghcMod . mkOccName clsName $ T.unpack tyname
            args <- mapM (convType env) lfArgs
            pure $ foldl (HsAppTy noExt . noLoc) tyvar (map noLoc args)

        LF.TCon LF.Qualified {..}
          | qualModule == LF.ModuleName ["DA", "Types"]
          , [name] <- LF.unTypeConName qualObject
          , Just n <- stripPrefix "Tuple" $ T.unpack name
          , Just i <- readMay n
          , 2 <= i && i <= 20 -> mkTuple i
        LF.TCon LF.Qualified {..} ->
            case LF.unTypeConName qualObject of
                [name] -> do
                    ghcMod <- genModule env qualPackage qualModule
                    pure . HsTyVar noExt NotPromoted . noLoc
                        . mkOrig ghcMod . mkOccName varName $ T.unpack name
                n@[_name0, _name1] -> case MS.lookup n (sumProdRecords $ envMod env) of
                    Nothing ->
                        error $ "Internal error: Could not find generated record type: " <> T.unpack (T.intercalate "." n)
                    Just fs ->
                        HsRecTy noExt <$> mapM (uncurry (mkConDeclField env)) fs
                cs -> errTooManyNameComponents cs
        LF.TApp ty1 ty2 -> do
            ty1' <- convType env ty1
            ty2' <- convType env ty2
            pure . HsParTy noExt . noLoc $ HsAppTy noExt (noLoc ty1') (noLoc ty2')
        LF.TBuiltin builtinTy -> convBuiltInTy env builtinTy
        LF.TForall {..} -> do
            binder <- convTyVarBinder env forallBinder
            body <- convType env forallBody
            pure . HsParTy noExt . noLoc $ HsForAllTy noExt [binder] (noLoc body)
        ty@(LF.TStruct fls) -> do
            tys <- mapM (convType env . snd) fls
            pure $ HsTupleTy noExt
                (if isConstraint ty then HsConstraintTuple else HsBoxedTuple)
                (map noLoc tys)

        LF.TNat n -> pure $
            HsTyLit noExt (HsNumTy NoSourceText (LF.fromTypeLevelNat n))
  where
    mkTuple :: Int -> Gen (HsType GhcPs)
    mkTuple i =
        pure $ HsTyVar noExt NotPromoted $
        noLoc $ mkRdrUnqual $ occName $ tupleTyConName BoxedTuple i

convBuiltInTy :: Env -> LF.BuiltinType -> Gen (HsType GhcPs)
convBuiltInTy env =
    \case
        LF.BTInt64 -> mkGhcType env "Int"
        LF.BTDecimal -> mkGhcType env "Decimal"
        LF.BTText -> mkGhcType env "Text"
        LF.BTTimestamp -> mkLfInternalType env "Time"
        LF.BTDate -> mkLfInternalType env "Date"
        LF.BTParty -> mkLfInternalType env "Party"
        LF.BTUnit -> pure $ mkTyConTypeUnqual unitTyCon
        LF.BTBool -> mkGhcType env "Bool"
        LF.BTList -> pure $ mkTyConTypeUnqual listTyCon
        LF.BTUpdate -> mkLfInternalType env "Update"
        LF.BTScenario -> mkLfInternalType env "Scenario"
        LF.BTContractId -> mkLfInternalType env "ContractId"
        LF.BTOptional -> mkLfInternalPrelude env "Optional"
        LF.BTTextMap -> mkLfInternalType env "TextMap"
        LF.BTGenMap -> mkLfInternalType env "Map"
        LF.BTArrow -> pure $ mkTyConTypeUnqual funTyCon
        LF.BTNumeric -> mkGhcType env "Numeric"
        LF.BTAny -> mkLfInternalType env "Any"
        LF.BTTypeRep -> mkLfInternalType env "TypeRep"

errTooManyNameComponents :: [T.Text] -> a
errTooManyNameComponents cs =
    error $
    "Internal error: Dalf contains type constructors with more than two name components: " <>
    (T.unpack $ T.intercalate "." cs)

convKind :: Env -> LF.Kind -> Gen (LHsKind GhcPs)
convKind env = \case
    LF.KStar -> pure . noLoc $ HsStarTy noExt False
    LF.KNat -> noLoc <$> mkGhcType env "Nat"
    LF.KArrow k1 k2 -> do
        k1' <- convKind env k1
        k2' <- convKind env k2
        pure . noLoc $ HsFunTy noExt k1' k2'

convTyVarBinder :: Env -> (LF.TypeVarName, LF.Kind) -> Gen (LHsTyVarBndr GhcPs)
convTyVarBinder env = \case
    (LF.TypeVarName tyVar, LF.KStar) ->
        pure $ mkUserTyVar tyVar
    (LF.TypeVarName tyVar, kind) ->
        mkKindedTyVar tyVar <$> convKind env kind

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

mkStableType :: Env -> UnitId -> LF.ModuleName -> String -> Gen (HsType GhcPs)
mkStableType env unitId modName tyName = do
    ghcMod <- genStableModule env unitId modName
    pure . HsTyVar noExt NotPromoted . noLoc
        . mkOrig ghcMod $ mkOccName varName tyName

mkGhcType :: Env -> String -> Gen (HsType GhcPs)
mkGhcType env = mkStableType env primUnitId $
    LF.ModuleName ["GHC", "Types"]

mkLfInternalType :: Env -> String -> Gen (HsType GhcPs)
mkLfInternalType env = mkStableType env damlStdlibUnitId $
    LF.ModuleName ["DA", "Internal", "LF"]

mkLfInternalPrelude :: Env -> String -> Gen (HsType GhcPs)
mkLfInternalPrelude env = mkStableType env damlStdlibUnitId $
    LF.ModuleName ["DA", "Internal", "Prelude"]

mkTyConTypeUnqual :: TyCon -> HsType GhcPs
mkTyConTypeUnqual tyCon = HsTyVar noExt NotPromoted . noLoc $ mkRdrUnqual (occName name)
    where name = getName tyCon

-- | Generate the full source for a daml-lf package.
generateSrcPkgFromLf :: Config -> LF.Package -> [(NormalizedFilePath, String)]
generateSrcPkgFromLf config pkg = do
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
    env m = Env
        { envConfig = config
        , envQualifyThisModule = False
        , envDepClassMap = buildDepClassMap config
        , envWorld = buildWorld config
        , envMod = m
        }
    header =
        [ "{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE NoImplicitPrelude #-}"
        , "{-# LANGUAGE NoOverloadedStrings #-}"
        , "{-# LANGUAGE TypeOperators #-}"
        , "{-# OPTIONS_GHC -Wno-unused-imports -Wno-missing-methods #-}"
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
       Config
    -> LF.PackageId
    -> LF.Package
    -> String
    -> [(NormalizedFilePath, String)]
generateGenInstancesPkgFromLf config pkgId pkg qual =
    catMaybes
        [ generateGenInstanceModule
            Env
                { envConfig = config
                , envQualifyThisModule = True
                , envMod = mod
                , envDepClassMap = buildDepClassMap config
                , envWorld = buildWorld config
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
envLookupPackage ref env = MS.lookup (configGetUnitId ref) configPackages
    where Config{..} = envConfig env

getClassMethodName :: LF.FieldName -> Maybe T.Text
getClassMethodName (LF.FieldName fieldName) =
    T.stripPrefix "m_" fieldName

isSuperClassField :: LF.FieldName -> Bool
isSuperClassField (LF.FieldName fieldName) =
    "s_" `T.isPrefixOf` fieldName

-- | Signature data for a dictionary function.
data DFunSig
    = DFunSigNormal -- ^ signature for a normal dictionary function
        { dfsType :: !LF.Type -- ^ LF type signature for the dictionary function
        }
    | DFunSigHasField -- ^ signature for a HasField dictionary function
        { dfsBinders :: ![(LF.TypeVarName, LF.Kind)] -- ^ foralls
        , dfsContext :: ![LF.Type] -- ^ constraints
        , dfsName :: LF.Qualified LF.TypeSynName -- ^ name of type synonym
        , dfsField :: !T.Text -- ^ first arg (a type level string)
        , dfsArgs :: ![LF.Type] -- ^ rest of the args
        }

-- | Break a value type signature down into a dictionary function signature.
getDFunSig :: (LF.ExprValName, LF.Type) -> Maybe DFunSig
getDFunSig (valName, valType) = do
    (dfsBinders, dfsContext, dfsName, args) <- go valType
    if isHasField dfsName
        then do
            (LF.TUnit : dfsArgs) <- Just args
            dfsField <- getFieldArg valName
            guard (not $ T.null dfsField)
            Just DFunSigHasField {..}
        else
            Just DFunSigNormal { dfsType = valType }
  where
    -- | Break a dictionary function type down into a tuple
    -- (foralls, constraints, synonym name, args).
    go = \case
        LF.TForall b rest -> do
            (bs, cs, name, args) <- go rest
            Just (b:bs, cs, name, args)
        (LF.:->) c rest -> do
            guard (isConstraint c)
            (bs, cs, name, args) <- go rest
            Just (bs, c:cs, name, args)
        LF.TSynApp name args -> do
            Just ([], [], name, args)
        _ ->
            Nothing

    -- | Is this an instance of the HasField typeclass?
    isHasField :: LF.Qualified LF.TypeSynName -> Bool
    isHasField LF.Qualified{..} =
        qualModule == LF.ModuleName ["DA", "Internal", "Record"]
        && qualObject == LF.TypeSynName ["HasField"]

    -- | Get the first arg of HasField (the name of the field) from
    -- the name of the binding.
    getFieldArg :: LF.ExprValName -> Maybe T.Text
    getFieldArg (LF.ExprValName name) = do
        name' <- T.stripPrefix "$fHasField\"" name
        Just $ fst (T.breakOn "\"" name')

-- | Convert dictionary function signature into a DAML type.
convDFunSig :: Env -> DFunSig -> Gen (HsType GhcPs)
convDFunSig env = \case
    DFunSigNormal ty ->
        convType env ty

    DFunSigHasField {..} -> do
        binders <- mapM (convTyVarBinder env) dfsBinders
        context <- mapM (convType env) dfsContext
        args <- mapM (convType env) dfsArgs
        ghcMod <- genModule env (LF.qualPackage dfsName) (LF.qualModule dfsName)
        let tyvar :: HsType GhcPs
                = HsTyVar noExt NotPromoted . noLoc
                $ mkOrig ghcMod (mkOccName clsName "HasField")

            arg0 :: HsType GhcPs
            arg0 = HsTyLit noExt . HsStrTy NoSourceText
                . mkFastString $ T.unpack dfsField

        pure . HsParTy noExt . noLoc
            . HsForAllTy noExt binders . noLoc
            . HsQualTy noExt (noLoc (map noLoc context)) . noLoc
            $ foldl (HsAppTy noExt . noLoc) tyvar (map noLoc (arg0 : args))
