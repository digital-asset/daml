-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DataDependencies
    ( Config (..)
    , generateSrcPkgFromLf
    , prefixDependencyModule
    ) where

import DA.Pretty
import Control.Applicative
import Control.Monad
import Control.Monad.Trans.Writer.CPS
import Data.Char (isDigit)
import qualified Data.DList as DL
import Data.Foldable (fold)
import Data.Hashable (Hashable)
import qualified Data.HashMap.Strict as HMS
import Data.List.Extra
import Data.Ord (Down (Down))
import Data.Semigroup.FixedPoint
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Map.Strict as MS
import Data.Maybe
import Data.Either
import qualified Data.NameMap as NM
import qualified Data.Text as T
import Development.IDE.Types.Location
import GHC.Generics (Generic)
import Safe
import System.FilePath

import "ghc-lib-parser" Bag
import "ghc-lib-parser" BasicTypes
import "ghc-lib-parser" FastString
import "ghc-lib" GHC
import "ghc-lib-parser" Module
import "ghc-lib-parser" Name
import "ghc-lib-parser" Outputable (alwaysQualify, ppr, showSDocForUser)
import "ghc-lib-parser" RdrName
import "ghc-lib-parser" TcEvidence (HsWrapper (WpHole))
import "ghc-lib-parser" TysPrim
import "ghc-lib-parser" TysWiredIn

import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Alpha as LF
import qualified DA.Daml.LF.TypeChecker.Check as LF
import qualified DA.Daml.LF.TypeChecker.Env as LF
import DA.Daml.Options

import SdkVersion

data Config = Config
    { configPackages :: MS.Map LF.PackageId LF.Package
     -- ^ All packages we know about, i.e., dependencies,
     -- data-dependencies and stable packages.
    , configGetUnitId :: LF.PackageRef -> UnitId
        -- ^ maps a package reference to a unit id
    , configSelfPkgId :: LF.PackageId
        -- ^ package id for this package, we need it to build a closed LF.World
    , configStablePackages :: MS.Map LF.PackageId UnitId
        -- ^ map from a package id of a stable package to the unit id
        -- of the corresponding package, i.e., daml-prim/daml-stdlib.
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
    , envHiddenRefMap :: HMS.HashMap Ref Bool
        -- ^ Set of references that should be hidden, not exposed.
    , envDepClassMap :: DepClassMap
        -- ^ Map of typeclasses from dependencies.
    , envDepInstances :: MS.Map LF.TypeSynName [LF.Qualified LF.Type]
        -- ^ Map of instances from dependencies.
        -- We only store the name since the real check happens in `isDuplicate`.
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
                , MS.keys configPackages
                ]
            mkExtPackage pkgId = do
                pkg <- MS.lookup pkgId configPackages
                Just (LF.ExternalPackage pkgId pkg)
        extPackages <- mapM mkExtPackage packageIds
        self <- MS.lookup configSelfPkgId configPackages
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
    , Just LF.Package{..} <- [MS.lookup packageId configPackages]
    , LF.Module{..} <- NM.toList packageModules
    , dsyn@LF.DefTypeSyn{..} <- NM.toList moduleSynonyms
    ]

buildDepInstances :: Config -> MS.Map LF.TypeSynName [LF.Qualified LF.Type]
buildDepInstances Config{..} = MS.fromListWith (<>)
    [ (clsName, [LF.Qualified (LF.PRImport packageId) moduleName (snd dvalBinder)])
    | packageId <- Set.toList configDependencyPackages
    , Just LF.Package{..} <- [MS.lookup packageId configPackages]
    , LF.Module{..} <- NM.toList packageModules
    , dval@LF.DefValue{..} <- NM.toList moduleValues
    , Just dfun <- [getDFunSig dval]
    , let clsName = LF.qualObject $ dfhName $ dfsHead dfun
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
    -- this should never fail so we just call `error` if it does
    either (error . ("Internal LF type error: " <>) . renderPretty) id $ do
        LF.runGamma (envWorld env) (envLfVersion env) $ do
            esyn1 <- LF.expandTypeSynonyms (closedType syn1)
            esyn2 <- LF.expandTypeSynonyms (closedType syn2)
            pure (LF.alphaType esyn1 esyn2)

  where
    -- | Turn a type synonym definition into a closed type.
    closedType :: LF.DefTypeSyn -> LF.Type
    closedType LF.DefTypeSyn{..} = LF.mkTForalls synParams synType

-- | Check if an instance is a duplicate of another one.
-- This is needed to filter out duplicate instances which would
-- result in a type error.
isDuplicate :: Env -> LF.Type -> LF.Type -> Bool
isDuplicate env ty1 ty2 =
    fromRight False $ do
        LF.runGamma (envWorld env) (envLfVersion env) $ do
            esyn1 <- LF.expandTypeSynonyms ty1
            esyn2 <- LF.expandTypeSynonyms ty2
            pure (LF.alphaType esyn1 esyn2)

data ImportOrigin = FromCurrentSdk UnitId | FromPackage LF.PackageId
    deriving (Eq, Ord)

-- | A module reference coming from DAML-LF.
data ModRef = ModRef
    { modRefModule :: LF.ModuleName
    , modRefOrigin :: ImportOrigin
    } deriving (Eq, Ord)

modRefImport :: Config -> ModRef -> LImportDecl GhcPs
modRefImport Config{..} ModRef{..} = noLoc ImportDecl
    { ideclSourceSrc = NoSourceText
    , ideclName = (noLoc . mkModuleName . T.unpack . LF.moduleNameString) modName
    , ideclPkgQual = Nothing
    , ideclSource = False
    , ideclSafe = False
    , ideclImplicit = False
    , ideclQualified = False
    , ideclAs = Nothing
    , ideclHiding = Nothing
    , ideclExt = noExt
    }
  where
      modName = case modRefOrigin of
          FromCurrentSdk _ -> LF.ModuleName (configSdkPrefix <> LF.unModuleName modRefModule)
          FromPackage importPkgId
             | importPkgId == configSelfPkgId -> modRefModule
             -- The module names from the current package are the only ones that are not modified
             | otherwise -> prefixDependencyModule importPkgId modRefModule


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
    genDecls = do
        decls <- sequence . concat $
            [ classDecls
            , synonymDecls
            , dataTypeDecls
            , valueDecls
            ]
        instDecls <- sequence instanceDecls
        pure $ decls <> catMaybes instDecls


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
    classReexports = map snd (MS.elems classReexportMap)

    classReexportMap :: MS.Map LF.TypeSynName (LF.PackageId, Gen (LIE GhcPs))
    classReexportMap = MS.fromList $ do
        synDef@LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        guard $ case synType of
            LF.TStruct _ -> True
            -- Type classes with no fields are translated to TUnit
            -- since LF structs need to have a non-zero number of
            -- fields.
            LF.TUnit -> True
            _ -> False
        LF.TypeSynName [name] <- [synName]
        Just (pkgId, depDef) <- [envLookupDepClass synName env]
        guard (safeToReexport env synDef depDef)
        let occName = mkOccName clsName . T.unpack $ sanitize name
        pure . (\x -> (synName,(pkgId, x))) $ do
            ghcMod <- genModule env (LF.PRImport pkgId) (LF.moduleName (envMod env))
            pure . noLoc . IEThingAll noExt
                . noLoc . IEName . noLoc
                $ mkOrig ghcMod occName

    reexportedClasses :: MS.Map LF.TypeSynName LF.PackageId
    reexportedClasses = MS.map fst classReexportMap

    classDecls :: [Gen (LHsDecl GhcPs)]
    classDecls = do
        defTypeSyn@LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        fields <- case synType of
            LF.TStruct fields -> [fields]
            LF.TUnit -> [[]]
            _ -> []
        LF.TypeSynName [name] <- [synName]
        guard (synName `MS.notMember` classReexportMap)
        guard (shouldExposeDefTypeSyn defTypeSyn)
        let occName = mkOccName clsName . T.unpack $ sanitize name
        pure $ do
            supers <- sequence
                [ convType env reexportedClasses fieldType
                | (fieldName, LF.TUnit LF.:-> fieldType) <- fields
                , isSuperClassField fieldName
                ]
            methods <- sequence
                [ (methodName,) <$> convType env reexportedClasses fieldType
                | (fieldName, LF.TUnit LF.:-> fieldType) <- fields
                , Just methodName <- [getClassMethodName fieldName]
                ]
            params <- mapM (convTyVarBinder env) synParams

            defaultMethods <- sequence
                [ do
                    sig <- convType env reexportedClasses defaultSig
                    bind <- mkStubBind env (mkRdrName methodName) defaultSig
                    pure (methodName, sig, bind)
                | (fieldName, LF.TUnit LF.:-> _) <- fields
                , Just methodName <- [getClassMethodName fieldName]
                , Just LF.DefValue  { dvalBinder = (_, LF.TForalls _ (LF.TSynApp _ _ LF.:-> defaultSig)) }
                    <- [NM.lookup (defaultMethodName methodName) (LF.moduleValues (envMod env))]
                ]

            pure . noLoc . TyClD noExt $ ClassDecl
                { tcdCExt = noExt
                , tcdCtxt = noLoc (map noLoc supers)
                , tcdLName = noLoc $ mkRdrUnqual occName
                , tcdTyVars = HsQTvs noExt params
                , tcdFixity = Prefix
                , tcdFDs = mkFunDeps name
                , tcdSigs =
                    [ mkOpSig False name ty | (name, ty) <- methods ] ++
                    [ mkOpSig True  name ty | (name, ty, _) <- defaultMethods ]
                , tcdMeths = listToBag
                    [ noLoc bind | (_, _, bind) <- defaultMethods ]
                , tcdATs = [] -- associated types not supported
                , tcdATDefs = []
                , tcdDocs = []
                }
      where
        mkOpSig :: Bool -> T.Text -> HsType GhcPs -> LSig GhcPs
        mkOpSig isDefault methodName methodType =
            noLoc $ ClassOpSig noExt isDefault
                    [mkRdrName methodName]
                    (HsIB noExt (noLoc methodType))

        mkFunDeps :: T.Text -> [LHsFunDep GhcPs]
        mkFunDeps className = fromMaybe [] $ do
            let values = LF.moduleValues (envMod env)
            LF.DefValue{..} <- NM.lookup (funDepName className) values
            LF.TForalls _ ty <- pure (snd dvalBinder)
            decodeTypeList decodeFunDep ty

        decodeFunDep :: LF.Type -> Maybe (LHsFunDep GhcPs)
        decodeFunDep ty = do
            (left LF.:-> right) <- pure ty
            left' <- decodeTypeList decodeTypeVar left
            right' <- decodeTypeList decodeTypeVar right
            pure (noLoc (left', right'))

        decodeTypeVar :: LF.Type -> Maybe (Located RdrName)
        decodeTypeVar ty = do
            LF.TVar (LF.TypeVarName x) <- pure ty
            pure (mkRdrName x)

        decodeTypeList :: (LF.Type -> Maybe t) -> LF.Type -> Maybe [t]
        decodeTypeList f ty = do
            LF.TStruct fields <- pure ty
            pairs <- sortOn fst <$> mapM (decodeTypeListField f) fields
            guard (map fst pairs == [1 .. length pairs])
            pure (map snd pairs)

        decodeTypeListField :: (LF.Type -> Maybe t) -> (LF.FieldName, LF.Type) -> Maybe (Int, t)
        decodeTypeListField f (LF.FieldName fieldName, x) = do
            suffix <- T.stripPrefix "_" fieldName
            i <- readMay (T.unpack suffix)
            y <- f x
            pure (i, y)

    synonymDecls :: [Gen (LHsDecl GhcPs)]
    synonymDecls = do
        defTypeSyn@LF.DefTypeSyn{..} <- NM.toList . LF.moduleSynonyms $ envMod env
        guard $ case synType of
            LF.TStruct _ -> False
            LF.TUnit -> False
            _ -> True
        LF.TypeSynName [name] <- [synName]
        guard (shouldExposeDefTypeSyn defTypeSyn)
        let occName = mkOccName tcName . T.unpack $ sanitize name
        pure $ do
            params <- mapM (convTyVarBinder env) synParams
            rhs <- convType env reexportedClasses synType
            pure . noLoc . TyClD noExt $ SynDecl
                { tcdSExt = noExt
                , tcdLName = noLoc $ mkRdrUnqual occName
                , tcdTyVars = HsQTvs noExt params
                , tcdFixity = Prefix
                , tcdRhs = noLoc rhs
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
            ltype = noLoc <$> convType env reexportedClasses lfType :: Gen (LHsType GhcPs)
            lname = mkRdrName (LF.unExprValName lfName) :: Located RdrName
            sig = TypeSig noExt [lname] . HsWC noExt . HsIB noExt <$> ltype
            lsigD = noLoc . SigD noExt <$> sig :: Gen (LHsDecl GhcPs)
        let lvalD = noLoc . ValD noExt <$> mkStubBind env lname lfType
        [ lsigD, lvalD ]

    -- | Generate instance declarations from dictionary functions.
    instanceDecls :: [Gen (Maybe (LHsDecl GhcPs))]
    instanceDecls = do
        dval@LF.DefValue {..} <- sortOn (Down . nameKey) $ NM.toList $ LF.moduleValues $ envMod env
        Just dfunSig <- [getDFunSig dval]
        guard (shouldExposeInstance dval)
        let clsName = LF.qualObject $ dfhName $ dfsHead dfunSig
        case find (isDuplicate env (snd dvalBinder) . LF.qualObject) (MS.findWithDefault [] clsName $ envDepInstances env) of
            Just qualInstance ->
                -- If the instance already exists, we still
                -- need to import it so that we can refer to it from other
                -- instances.
                [Nothing <$ genModule env (LF.qualPackage qualInstance) (LF.qualModule qualInstance)]
            Nothing -> pure $ do
                polyTy <- HsIB noExt . noLoc <$> convDFunSig env reexportedClasses dfunSig
                pure . Just . noLoc . InstD noExt . ClsInstD noExt $ ClsInstDecl
                    { cid_ext = noExt
                    , cid_poly_ty = polyTy
                    , cid_binds = emptyBag
                    , cid_sigs = []
                    , cid_tyfam_insts = []
                    , cid_datafam_insts = []
                    , cid_overlap_mode = Nothing
                    }
      where
        -- Split a DefValue's name, into the lexical part and the numeric part
        -- if it exists. For example, the name "$dFooBar123" is split into a
        -- pair ("$dFooBar", Just 123), and the name "$dFooBar" would be turned
        -- into ("$dFooBar", Nothing). This gives us the correct (or, close
        -- enough) order for recreating dictionary function names in GHC without
        -- mismatches.
        --
        -- See issue #7362, and the corresponding regression test in the
        -- packaging test suite.
        nameKey :: LF.DefValue -> (T.Text, Maybe Int)
        nameKey dval =
            let name = LF.unExprValName (fst (LF.dvalBinder dval))
                (intR,tagR) = T.span isDigit (T.reverse name)
            in (T.reverse tagR, readMay (T.unpack (T.reverse intR)))

    hiddenRefMap :: HMS.HashMap Ref Bool
    hiddenRefMap = envHiddenRefMap env

    isHidden :: Ref -> Bool
    isHidden ref =
        case HMS.lookup ref hiddenRefMap of
            Nothing -> error
                ("Internal Error: Missing type dependency from hiddenRefMap: "
                <> show ref)
            Just b -> b

    qualify :: a -> LF.Qualified a
    qualify x = LF.Qualified
        { qualPackage = LF.PRImport (configSelfPkgId config)
        , qualModule = lfModName
        , qualObject = x
        }

    shouldExposeDefDataType :: LF.DefDataType -> Bool
    shouldExposeDefDataType LF.DefDataType{..}
        = not (isHidden (RTypeCon (qualify dataTypeCon)))

    shouldExposeDefTypeSyn :: LF.DefTypeSyn -> Bool
    shouldExposeDefTypeSyn LF.DefTypeSyn{..}
        = not (isHidden (RTypeSyn (qualify synName)))

    shouldExposeDefValue :: LF.DefValue -> Bool
    shouldExposeDefValue LF.DefValue{..}
        | (lfName, lfType) <- dvalBinder
        = not ("$" `T.isPrefixOf` LF.unExprValName lfName)
        && not (any isHidden (DL.toList (refsFromType lfType)))
        && (LF.moduleNameString lfModName /= "GHC.Prim")
        && not (LF.unExprValName lfName `Set.member` classMethodNames)

    shouldExposeInstance :: LF.DefValue -> Bool
    shouldExposeInstance LF.DefValue{..}
        = isDFunBody dvalBody
        && not (isHidden (RValue (qualify (fst dvalBinder))))

    convDataCons :: T.Text -> LF.DataCons -> Gen [LConDecl GhcPs]
    convDataCons dataTypeCon0 = \case
        LF.DataRecord fields -> do
            fields' <- mapM (uncurry (mkConDeclField env)) fields
            pure [ mkConDecl occName (RecCon (noLoc fields')) ]
        LF.DataVariant cons -> do
            let hasExactlyOneConstructor = length cons == 1
            sequence
                [ mkConDecl (occNameFor conName) <$> convConDetails hasExactlyOneConstructor ty
                | (conName, ty) <- cons
                ]
        LF.DataEnum cons -> do
            when (length cons == 1) (void $ mkGhcType env "DamlEnum")
                -- Single constructor enums spawn a reference to
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

        convConDetails :: Bool -> LF.Type -> Gen (HsConDeclDetails GhcPs)
        convConDetails hasExactlyOneConstructor = \case
            -- nullary variant constructor (see issue #7207)
            --
            -- We translate a variant constructor `C ()` to `C` in DAML. But
            -- if it's the only constructor, we leave it as `C ()` to distinguish
            -- it from an enum type.
            LF.TUnit | not hasExactlyOneConstructor ->
                pure $ PrefixCon []

            -- variant record constructor
            LF.TConApp LF.Qualified{..} _
                | LF.TypeConName ns <- qualObject
                , length ns == 2 ->
                    case MS.lookup ns (sumProdRecords $ envMod env) of
                        Nothing ->
                            error $ "Internal error: Could not find generated record type: " <> T.unpack (T.intercalate "." ns)
                        Just fs ->
                            RecCon . noLoc <$> mapM (uncurry (mkConDeclField env)) fs

            -- normal payload
            ty ->
                PrefixCon . pure . noLoc <$> convType env reexportedClasses ty

    -- imports needed by the module declarations
    imports
     =
        [ modRefImport config modRef
        | modRef@ModRef{..} <- Set.toList modRefs
         -- don’t import ourselves
        , not (modRefModule == lfModName && modRefOrigin == FromPackage (configSelfPkgId config))
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

-- | Make a binding of the form "x = error \"data-dependency stub\"". If a qualified name is passed,
-- we turn the left-hand side into the unqualified form of that name (LHS
-- must always be unqualified), and the right-hand side remains qualified.
mkStubBind :: Env -> Located RdrName -> LF.Type -> Gen (HsBind GhcPs)
mkStubBind env lname ty = do
    -- Producing an expression that GHC will accept for
    -- an arbitrary type:
    --
    -- 1. A simple recursive binding x = x falls apart in the presence of
    --    AllowAmbiguousTypes.
    -- 2. TypeApplications could in theory fix this but GHC is
    --    extremely pedantic when it comes to which type variables are
    --    in scope, e.g., the following is an error
    --
    --    f :: (forall a. Show a => a -> String)
    --    f = f @a -- not in scope error
    --
    --    So making that actually work is very annoying.
    -- 3. One would hope that just calling `error` does the trick but that
    --    fails due to ImpredicativeTypes in something like Lens s t a b -> Lens s t a b.
    --
    -- The solution we use is to count the number of arguments and add wildcard
    -- matches so that the type variable in `error`s type only needs to be
    -- unified with the non-impredicative result.
    lexpr <- mkErrorCall env "data-dependency stub"
    let args = countFunArgs ty
    let lgrhs = noLoc $ GRHS noExt [] lexpr :: LGRHS GhcPs (LHsExpr GhcPs)
        grhss = GRHSs noExt [lgrhs] (noLoc $ EmptyLocalBinds noExt)
        lnameUnqual = noLoc . mkRdrUnqual . rdrNameOcc $ unLoc lname
        matchContext = FunRhs lnameUnqual Prefix NoSrcStrict
        lmatch = noLoc $ Match noExt matchContext (replicate args $ noLoc (WildPat noExt)) Nothing grhss
        lalts = noLoc [lmatch]
        bind = FunBind noExt lnameUnqual (MG noExt lalts Generated) WpHole []
    pure bind
  where
    countFunArgs = \case
        (arg LF.:-> t)
          | isConstraint arg -> countFunArgs t
          | otherwise -> 1 + countFunArgs t
        LF.TForall _ t -> countFunArgs t
        _ -> 0


mkErrorCall :: Env -> String -> Gen (LHsExpr GhcPs)
mkErrorCall env msg = do
    ghcErr <- genStableModule env primUnitId (LF.ModuleName ["GHC", "Err"])
    dataString <- genStableModule env primUnitId (LF.ModuleName ["Data", "String"])
    let errorFun = noLoc $ HsVar noExt $ noLoc $ mkOrig ghcErr $ mkOccName varName "error" :: LHsExpr GhcPs
    let fromStringFun = noLoc $ HsVar noExt $ noLoc $ mkOrig dataString $ mkOccName varName "fromString" :: LHsExpr GhcPs
    let errorMsg = noLoc $ HsLit noExt (HsString (SourceText $ show msg) $ mkFastString msg) :: LHsExpr GhcPs
    pure $ noLoc $ HsPar noExt $ noLoc $ HsApp noExt errorFun (noLoc $ HsPar noExt $ noLoc $ HsApp noExt fromStringFun errorMsg)

mkConDeclField :: Env -> LF.FieldName -> LF.Type -> Gen (LConDeclField GhcPs)
mkConDeclField env fieldName fieldTy = do
    fieldTy' <- convType env MS.empty fieldTy
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
        origin = case pkgRef of
            LF.PRImport pkgId
                | Just unitId <- MS.lookup pkgId configStablePackages -> FromCurrentSdk unitId
                | otherwise -> FromPackage pkgId
            LF.PRSelf -> FromPackage configSelfPkgId
    genModuleAux (envConfig env) origin modName

genStableModule :: Env -> UnitId -> LF.ModuleName -> Gen Module
genStableModule env currentSdkPkg = genModuleAux (envConfig env) (FromCurrentSdk currentSdkPkg)

prefixModuleName :: [T.Text] -> LF.ModuleName -> LF.ModuleName
prefixModuleName prefix (LF.ModuleName mod) = LF.ModuleName (prefix <> mod)

prefixDependencyModule :: LF.PackageId -> LF.ModuleName -> LF.ModuleName
prefixDependencyModule (LF.PackageId pkgId) = prefixModuleName ["Pkg_" <> pkgId]

genModuleAux :: Config -> ImportOrigin -> LF.ModuleName -> Gen Module
genModuleAux conf origin moduleName = do
    let modRef = ModRef moduleName origin
    let ghcModuleName = (unLoc . ideclName . unLoc . modRefImport conf) modRef
    let unitId = case origin of
            FromCurrentSdk unitId -> unitId
            FromPackage pkgId -> configGetUnitId conf (LF.PRImport pkgId)
    emitModRef modRef
    pure $ mkModule unitId ghcModuleName

-- | We cannot refer to a class C reexported from the current module M using M.C. Therefore
-- we have to rewrite it to the original module. The map only contains type synonyms reexported
-- from the current module.
rewriteClassReexport :: Env -> MS.Map LF.TypeSynName LF.PackageId -> LF.Qualified LF.TypeSynName -> LF.Qualified LF.TypeSynName
rewriteClassReexport env reexported syn@LF.Qualified{..}
  | Just reexportPkgId <- MS.lookup qualObject reexported
  -- Only rewrite a reference to the current module
  , case qualPackage of
        LF.PRSelf -> True
        LF.PRImport synPkgId -> synPkgId == configSelfPkgId (envConfig env)
  , LF.moduleName (envMod env) == qualModule
  = syn { LF.qualPackage = LF.PRImport reexportPkgId }
  | otherwise = syn


convType :: Env -> MS.Map LF.TypeSynName LF.PackageId -> LF.Type -> Gen (HsType GhcPs)
convType env reexported =
    \case
        LF.TVar tyVarName -> pure $
            HsTyVar noExt NotPromoted $ mkRdrName $ LF.unTypeVarName tyVarName

        ty1 LF.:-> ty2 -> do
            ty1' <- convType env reexported ty1
            ty2' <- convType env reexported ty2
            pure $ if isConstraint ty1
                then HsQualTy noExt (noLoc [noLoc ty1']) (noLoc ty2')
                else HsParTy noExt (noLoc $ HsFunTy noExt (noLoc ty1') (noLoc ty2'))

        LF.TSynApp (rewriteClassReexport env reexported -> LF.Qualified{..}) lfArgs -> do
            ghcMod <- genModule env qualPackage qualModule
            let tyname = case LF.unTypeSynName qualObject of
                    [n] -> n
                    ns -> error ("DamlDependencies: unexpected typeclass name " <> show ns)
                tyvar = HsTyVar noExt NotPromoted . noLoc
                    . mkOrig ghcMod . mkOccName clsName $ T.unpack tyname
            args <- mapM (convType env reexported) lfArgs
            pure $ HsParTy noExt (noLoc $ foldl' (HsAppTy noExt . noLoc) tyvar (map noLoc args))

        ty | Just text <- getPromotedText ty ->
            pure $ HsTyLit noExt (HsStrTy NoSourceText (mkFastString $ T.unpack text))

        LF.TCon LF.Qualified {..}
            | qualModule == LF.ModuleName ["DA", "Types"]
            , [name] <- LF.unTypeConName qualObject
            , Just n <- stripPrefix "Tuple" $ T.unpack name
            , Just i <- readMay n
            , 2 <= i && i <= 20
            -> mkTuple i

        LF.TCon LF.Qualified {..} ->
            case LF.unTypeConName qualObject of
                [name] -> do
                    ghcMod <- genModule env qualPackage qualModule
                    pure . HsTyVar noExt NotPromoted . noLoc
                        . mkOrig ghcMod . mkOccName varName $ T.unpack name
                cs -> errTooManyNameComponents cs
        LF.TApp ty1 ty2 -> do
            ty1' <- convType env reexported ty1
            ty2' <- convType env reexported ty2
            pure . HsParTy noExt . noLoc $ HsAppTy noExt (noLoc ty1') (noLoc ty2')
        LF.TBuiltin builtinTy -> convBuiltInTy env builtinTy
        LF.TForall {..} -> do
            binder <- convTyVarBinder env forallBinder
            body <- convType env reexported forallBody
            pure . HsParTy noExt . noLoc $ HsForAllTy noExt [binder] (noLoc body)
        ty@(LF.TStruct fls) -> do
            tys <- mapM (convType env reexported . snd) fls
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
        pure . noLoc . HsParTy noExt . noLoc $ HsFunTy noExt k1' k2'

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
mkLfInternalType env = mkStableType env damlStdlib $
    LF.ModuleName ["DA", "Internal", "LF"]

mkLfInternalPrelude :: Env -> String -> Gen (HsType GhcPs)
mkLfInternalPrelude env = mkStableType env damlStdlib $
    LF.ModuleName ["DA", "Internal", "Prelude"]

mkTyConTypeUnqual :: TyCon -> HsType GhcPs
mkTyConTypeUnqual tyCon = HsTyVar noExt NotPromoted . noLoc $ mkRdrUnqual (occName name)
    where name = getName tyCon

-- | Generate the full source for a daml-lf package.
generateSrcPkgFromLf :: Config -> LF.Package -> [(NormalizedFilePath, String)]
generateSrcPkgFromLf envConfig pkg = do
    mod <- NM.toList $ LF.packageModules pkg
    let fp =
            toNormalizedFilePath' $
            (joinPath $ map T.unpack $ LF.unModuleName $ LF.moduleName mod) <.>
            ".daml"
    pure
        ( fp
        , unlines header ++
          (showSDocForUser fakeDynFlags alwaysQualify $
           ppr $ generateSrcFromLf $ env mod))
  where
    env envMod = Env {..}
    envQualifyThisModule = False
    envDepClassMap = buildDepClassMap envConfig
    envDepInstances = buildDepInstances envConfig
    envWorld = buildWorld envConfig
    envHiddenRefMap = buildHiddenRefMap envConfig envWorld
    header =
        [ "{-# LANGUAGE NoDamlSyntax #-}"
        , "{-# LANGUAGE NoImplicitPrelude #-}"
        , "{-# LANGUAGE NoOverloadedStrings #-}"
        , "{-# LANGUAGE TypeOperators #-}"
        , "{-# LANGUAGE UndecidableInstances #-}"
        , "{-# LANGUAGE AllowAmbiguousTypes #-}"
        , "{-# OPTIONS_GHC -Wno-unused-imports -Wno-missing-methods #-}"
        ]

-- | A reference that can appear in a type or expression. We need to track
-- all of these in order to control what gets hidden in data-dependencies.
data Ref
    = RTypeCon (LF.Qualified LF.TypeConName)
        -- ^ necessary to track hidden types and old-style typeclasses
    | RTypeSyn (LF.Qualified LF.TypeSynName)
        -- ^ necessary to track hidden new-style typeclasses
    | RValue (LF.Qualified LF.ExprValName)
        -- ^ necessary to track hidden typeclass instances
    deriving (Eq, Ord, Show, Generic)

instance Hashable Ref

type RefGraph = HMS.HashMap Ref (Bool, [Ref])

-- | Calculate the set of all references that should be hidden.
buildHiddenRefMap :: Config -> LF.World -> HMS.HashMap Ref Bool
buildHiddenRefMap config world =
    case leastFixedPointBy (||) refGraphList of
        Left ref -> error
            ("Internal error: missing reference in RefGraph "
            <> show ref)
        Right m -> m
  where
    refGraphList :: [(Ref, Bool, [Ref])]
    refGraphList =
        [ (ref, hidden, deps)
        | (ref, (hidden, deps)) <- HMS.toList refGraph
        ]

    refGraph :: RefGraph
    refGraph = foldl' visitRef HMS.empty (rootRefs config world)

    visitRef :: RefGraph -> Ref -> RefGraph
    visitRef !refGraph ref
        | HMS.member ref refGraph
            = refGraph -- already in the map
        | ref == RTypeCon erasedTCon
            = HMS.insert ref (True, []) refGraph -- Erased is always erased

        | RTypeCon tcon <- ref
        , Right defDataType <- LF.lookupDataType tcon world
        , refs <- DL.toList (refsFromDefDataType defDataType)
        , hidden <- defDataTypeIsOldTypeClass defDataType
        , refGraph' <- HMS.insert ref (hidden, refs) refGraph
            = foldl' visitRef refGraph' refs

        | RTypeSyn tsyn <- ref
        , Right defTypeSyn <- LF.lookupTypeSyn tsyn world
        , refs <- DL.toList (refsFromDefTypeSyn defTypeSyn)
        , refGraph' <- HMS.insert ref (False, refs) refGraph
            = foldl' visitRef refGraph' refs

        | RValue val <- ref
        , Right dval@LF.DefValue{..} <- LF.lookupValue val world
        , refs <- if hasDFunSig dval -- we only care about typeclass instances
            then DL.toList (refsFromDFun dval)
            else mempty
        , refGraph' <- HMS.insert ref (False, refs) refGraph
            = foldl' visitRef refGraph' refs

        | otherwise
            = error ("Unknown reference to type dependency " <> show ref)

-- | Get the list of type constructors that this type references.
-- This uses difference lists to reduce overhead.
refsFromType :: LF.Type -> DL.DList Ref
refsFromType = go
  where
    go = \case
        LF.TCon tcon -> pure (RTypeCon tcon)
        LF.TSynApp tsyn xs -> pure (RTypeSyn tsyn) <> foldMap go xs
        LF.TVar _ -> mempty
        LF.TBuiltin _ -> mempty
        LF.TNat _ -> mempty
        LF.TApp a b -> go a <> go b
        LF.TForall _ b -> go b
        LF.TStruct fields -> foldMap (go . snd) fields

refsFromDefTypeSyn :: LF.DefTypeSyn -> DL.DList Ref
refsFromDefTypeSyn = refsFromType . LF.synType

refsFromDefDataType :: LF.DefDataType -> DL.DList Ref
refsFromDefDataType = refsFromDataCons . LF.dataCons

refsFromDataCons :: LF.DataCons -> DL.DList Ref
refsFromDataCons = \case
    LF.DataRecord fields -> foldMap (refsFromType . snd) fields
    LF.DataVariant cons -> foldMap (refsFromType . snd) cons
    LF.DataEnum _ -> mempty

rootRefs :: Config -> LF.World -> DL.DList Ref
rootRefs config world = fold
    [ modRootRefs (configSelfPkgId config) mod
    | mod <- NM.toList (LF.packageModules (LF.getWorldSelf world))
    ]

modRootRefs :: LF.PackageId -> LF.Module -> DL.DList Ref
modRootRefs pkgId mod = fold
    [ DL.fromList
        [ RTypeCon (qualify (LF.dataTypeCon defDataType))
        | defDataType <- NM.toList (LF.moduleDataTypes mod)
        ]
    , DL.fromList
        [ RTypeSyn (qualify (LF.synName defTypeSyn))
        | defTypeSyn <- NM.toList (LF.moduleSynonyms mod)
        ]
    , DL.fromList
        [ RValue (qualify (fst dvalBinder))
        | dval@LF.DefValue{..} <- NM.toList (LF.moduleValues mod)
        , hasDFunSig dval
        ]
    , fold
        [ refsFromType (snd dvalBinder)
        | dval@LF.DefValue{..} <- NM.toList (LF.moduleValues mod)
        , not (hasDFunSig dval)
        ]
    ]
  where
    qualify :: a -> LF.Qualified a
    qualify = LF.Qualified (LF.PRImport pkgId) (LF.moduleName mod)

-- | Check that an expression matches the body of a dictionary function.
isDFunBody :: LF.Expr -> Bool
isDFunBody = \case
    LF.ETyLam _ body -> isDFunBody body
    LF.ETmLam _ body -> isDFunBody body
    LF.EStructCon _ -> True
    _ -> False

-- | Get the relevant references from a dictionary function.
refsFromDFun :: LF.DefValue -> DL.DList Ref
refsFromDFun LF.DefValue{..}
    = refsFromType (snd dvalBinder)
    <> refsFromDFunBody dvalBody

-- | Extract instance references from a dictionary function, and possibly some
-- more value references that get swept up by accident. This is general enough
-- to catch both @$f...@ generic dictionary functions, and @$d...@ specialised
-- dictionary functions.
--
-- Read https://wiki.haskell.org/Inlining_and_Specialisation for more info on
-- dictionary function specialisation.
refsFromDFunBody :: LF.Expr -> DL.DList Ref
refsFromDFunBody = \case
    LF.ETyLam _ body -> refsFromDFunBody body
    LF.ETmLam (_, ty) body -> refsFromType ty <> refsFromDFunBody body
    LF.EStructCon fields -> foldMap refsFromDFunField fields
    LF.EStructProj _fieldName a -> refsFromDFunBody a
    LF.ETyApp a b -> refsFromDFunBody a <> refsFromType b
    LF.ETmApp a b -> refsFromDFunBody a <> refsFromDFunBody b
    LF.EVal val -> pure (RValue val)
    LF.ELet (LF.Binding (_, ty) a) b -> refsFromType ty <> refsFromDFunBody a <> refsFromDFunBody b
    LF.EBuiltin _ -> mempty
    LF.EVar _ -> mempty
    t -> error ("Unhandled expression type in dictionary function body: " <> show t)
  where
    -- | We only care about superclasses instances.
    refsFromDFunField :: (LF.FieldName, LF.Expr) -> DL.DList Ref
    refsFromDFunField (fieldName, fieldBody)
        | isSuperClassField fieldName
        = refsFromDFunBody fieldBody

        | otherwise
        = mempty

-- | Determine whether a data type def is for an old-style
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

getClassMethodName :: LF.FieldName -> Maybe T.Text
getClassMethodName (LF.FieldName fieldName) =
    T.stripPrefix "m_" fieldName

isSuperClassField :: LF.FieldName -> Bool
isSuperClassField (LF.FieldName fieldName) =
    "s_" `T.isPrefixOf` fieldName

defaultMethodName :: T.Text -> LF.ExprValName
defaultMethodName name = LF.ExprValName ("$dm" <> name)

funDepName :: T.Text -> LF.ExprValName
funDepName x = LF.ExprValName ("$$fd" <> x)

-- | Signature data for a dictionary function.
data DFunSig = DFunSig
    { dfsBinders :: ![(LF.TypeVarName, LF.Kind)] -- ^ foralls
    , dfsContext :: ![LF.Type] -- ^ constraints
    , dfsHead :: !DFunHead
    , dfsSuper :: ![(LF.PackageRef, LF.ModuleName)]
        -- ^ references from superclass dependencies
    }

-- | Instance declaration head
data DFunHead
    = DFunHeadHasField -- ^ HasField instance
          { dfhName :: LF.Qualified LF.TypeSynName -- ^ name of type synonym
          , dfhField :: !T.Text -- ^ first arg (a type level string)
          , dfhArgs :: [LF.Type] -- ^ rest of the args
          }
    | DFunHeadNormal -- ^ Normal, i.e., non-HasField, instance
          { dfhName :: LF.Qualified LF.TypeSynName -- ^ name of type synonym
          , dfhArgs :: [LF.Type] -- ^ arguments
          }

-- | Is this the type signature for a dictionary function? Note that this
-- accepts both generic dictionary functions "$f..." and specialised
-- dictionary functions "$d...".
hasDFunSig :: LF.DefValue -> Bool
hasDFunSig dval = isJust (getDFunSig dval)

-- | Break a value type signature down into a dictionary function signature.
getDFunSig :: LF.DefValue -> Maybe DFunSig
getDFunSig LF.DefValue {..} = do
    let (valName, valType) = dvalBinder
    (dfsBinders, dfsContext, dfhName, dfhArgs) <- go valType
    dfsHead <- if isHasField dfhName
        then do
            (symbolTy : dfhArgs) <- Just dfhArgs
            -- We handle both the old state where symbol was translated to unit
            -- and new state where it is translated to PromotedText.
            dfhField <- getPromotedText symbolTy <|> getFieldArg valName
            guard (not $ T.null dfhField)
            Just DFunHeadHasField {..}
        else do
          guard (not $ isIP dfhName)
          Just DFunHeadNormal {..}
    let dfsSuper = getSuperclassReferences dvalBody
    pure $ DFunSig {..}
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

    -- | Is this an implicit parameters instance?
    -- Those need to be filtered out since
    -- 1: They are not regular instances anyway and shouldn’t be made available.
    -- 2: They are not unique so this actually breaks compilation.
    isIP :: LF.Qualified LF.TypeSynName -> Bool
    isIP LF.Qualified{..} =
        qualModule == LF.ModuleName ["GHC", "Classes"]
        && qualObject == LF.TypeSynName ["IP"]

    -- | Get the first arg of HasField (the name of the field) from
    -- the name of the binding.
    getFieldArg :: LF.ExprValName -> Maybe T.Text
    getFieldArg (LF.ExprValName name) = do
        name' <- T.stripPrefix "$fHasField\"" name
        Just $ fst (T.breakOn "\"" name')

getSuperclassReferences :: LF.Expr -> [(LF.PackageRef, LF.ModuleName)]
getSuperclassReferences body =
    [ (qualPackage, qualModule)
    | RValue LF.Qualified{..} <- DL.toList (refsFromDFunBody body)
    , isDFunName qualObject
    ]

isDFunName :: LF.ExprValName -> Bool
isDFunName (LF.ExprValName t) = any (`T.isPrefixOf` t) ["$f", "$d"]

-- | Convert dictionary function signature into a DAML type.
convDFunSig :: Env -> MS.Map LF.TypeSynName LF.PackageId -> DFunSig -> Gen (HsType GhcPs)
convDFunSig env reexported DFunSig{..} = do
    binders <- mapM (convTyVarBinder env) dfsBinders
    context <- mapM (convType env reexported) dfsContext
    let headName = rewriteClassReexport env reexported (dfhName dfsHead)
    ghcMod <- genModule env (LF.qualPackage headName) (LF.qualModule headName)
    let cls = case LF.unTypeSynName (LF.qualObject headName) of
            [n] -> HsTyVar noExt NotPromoted . noLoc $ mkOrig ghcMod . mkOccName clsName $ T.unpack n
            ns -> error ("DamlDependencies: unexpected typeclass name " <> show ns)
    mapM_ (uncurry (genModule env)) dfsSuper
    args <- case dfsHead of
      DFunHeadHasField{..} -> do
          let arg0 = HsTyLit noExt . HsStrTy NoSourceText . mkFastString $ T.unpack dfhField
          args <- mapM (convType env reexported) dfhArgs
          pure (arg0 : args)
      DFunHeadNormal{..} -> do mapM (convType env reexported) dfhArgs
    pure
      . HsParTy noExt . noLoc
      . HsForAllTy noExt binders . noLoc
      . HsQualTy noExt (noLoc (map noLoc context)) . noLoc
      $ foldl' (HsAppTy noExt . noLoc) cls (map noLoc args)

getPromotedText :: LF.Type -> Maybe T.Text
getPromotedText = \case
    LF.TApp (LF.TCon qtcon) argTy
        | qtcon == promotedTextTCon
        ->
            case argTy of
                LF.TStruct [(LF.FieldName text, LF.TUnit)]
                    | T.length text >= 1 && T.head text == '_' ->
                        Just (T.tail text) -- strip leading underscore
                _ ->
                    error ("Unexpected argument type to DA.Internal.PromotedText: " <> show argTy)
    _ -> Nothing

stableTCon :: T.Text -> [T.Text] -> T.Text -> LF.Qualified LF.TypeConName
stableTCon packageId moduleName tconName = LF.Qualified
    { qualPackage = LF.PRImport (LF.PackageId packageId)
    , qualModule = LF.ModuleName moduleName
    , qualObject = LF.TypeConName [tconName]
    }

erasedTCon, promotedTextTCon :: LF.Qualified LF.TypeConName
erasedTCon = stableTCon
    "76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f"
    ["DA", "Internal", "Erased"]
    "Erased"
promotedTextTCon = stableTCon
    "d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97"
    ["DA", "Internal", "PromotedText"]
    "PromotedText"
