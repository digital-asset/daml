-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-} -- Because the pattern match checker is garbage

-- Useful references:
--
-- * DAML-LF AST: https://github.com/DACH-NY/da/blob/master/compiler/daml-lf-ast/src/DA/Daml/LF/Ast/Base.hs
-- * GHC Syntax: https://hackage.haskell.org/package/ghc-8.4.1/docs/CoreSyn.html#t:Expr
--
-- The conversion works element by element, in a fairly direct way, apart from the exceptions
-- (not all of these need fixing, but all are worth being aware of):
--
-- * Type-defs are expanded everywhere they are used since they aren't in LF.
-- * GHC type class desugaring for default methods relies on lazy top-level
--   bindings. We eta-expand them to avoid the problem. See the comment on
--   DICTIONARY SANITIZATION below.
--
---------------------------------------------------------------------
-- DICTIONARY SANITIZATION
--
-- GHC's desugaring for default methods relies on the the fact that Haskell is
-- lazy. In contract, DAML-LF is strict. This mismatch causes a few problems.
-- For instance, GHC desugars:
--
-- > class Foo a where
-- >   bar :: a -> a
-- >   baz :: a -> a
-- >   baz = bar
-- >
-- > instance Foo Bool where
-- >   bar x = x
--
-- Into:
--
-- > bar = \ (@ a) (v_B1 :: Foo a) ->
--           case v_B1 of v_B1 { C:Foo v_B2 v_B3 -> v_B2 }
-- > baz = \ (@ a) (v_B1 :: Foo a) ->
--           case v_B1 of v_B1 { C:Foo v_B2 v_B3 -> v_B3 }
-- >
-- > $fFooBool = C:Foo @ Bool $cbar $cbaz
-- > $cbar = \ (x :: Bool) -> x
-- > $cbaz = $dmbaz @ Bool $fFooBool
-- > $dmbaz = \ (@ a) ($dFoo :: Foo a) -> bar @ a $dFoo
--
-- If you evaluate @$fFooBool@ eagerly that evaluates @$cbaz@, which evaluates
-- @$fFooBool@ and you loop forever.
--
-- To fix this problem, we make the fields of dictionary types (like @Foo@)
-- lazy by introducing an artificial argument of type @Unit@ to them. In order
-- to do so, we do three transformations on the generated DAML-LF:
--
-- (1) Translate dictionary type definitions with an extra @Unit@ argument
--     for each field.
--
-- (2) Apply an extra @Unit@ argument to each record projection on a
--     dictionary type.
--
-- (3) Wrap every argument to each dictionary type constructor in a lambda
--     introducing a @Unit@.
--
--
--
-- GHC produces a @newtype@ rather than a @data@ type for dictionar types of
-- type classes with a single method and no super classes. Since DAML-LF does
-- not support @newtype@, we could either treat them as some sort of type
-- synonym or translate them to a record type with a single field. We have
-- chosen to do the latter for the sake of uniformity among all dictionary
-- types. GHC Core contains neither constructor nor projection functions for
-- @newtype@s but uses coercions instead. We translate these coercions to
-- proper record construction/projection.

module DA.Daml.GHC.Compiler.Convert(convertModule, sourceLocToRange) where

import           DA.Daml.LF.Simplifier (freeVarsStep)
import           DA.Daml.GHC.Compiler.Primitives
import           DA.Daml.GHC.Compiler.UtilGHC
import           DA.Daml.GHC.Compiler.UtilLF

import Development.IDE.Functions.Compile (GhcModule(..))
import           Development.IDE.Types.Diagnostics
import           Development.IDE.UtilGHC

import           Control.Applicative
import           Control.Lens
import           Control.Monad.Except.Extended
import Control.Monad.Fail
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           DA.Daml.LF.Ast as LF
import           Data.Data hiding (TyCon)
import           Data.Functor.Foldable (cata)
import           Data.Int
import           Data.List.Extra
import qualified Data.Map.Strict as MS
import           Data.Maybe
import qualified Data.NameMap as NM
import           Data.Ratio
import qualified Data.Set as Set
import           Data.Tagged
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Tuple.Extra
import           "ghc-lib" GHC
import           "ghc-lib" GhcPlugins as GHC hiding ((<>))
import           "ghc-lib-parser" Pair
import           "ghc-lib-parser" PrelNames
import           "ghc-lib-parser" TysPrim
import qualified "ghc-lib-parser" Name
import           Safe.Exact (zipExact, zipExactMay)

---------------------------------------------------------------------
-- FAILURE REPORTING

conversionError :: String -> ConvertM e
conversionError msg = do
  ConversionEnv{..} <- ask
  throwError Diagnostic
      { dFilePath = fromMaybe "" convModuleFilePath
      , dRange = maybe noRange sourceLocToRange convRange
      , dSeverity = Error
      , dSource = T.pack "Core to DAML-LF"
      , dMessage = T.pack msg
      }

unsupported :: (HasCallStack, Outputable a) => String -> a -> ConvertM e
unsupported typ x = conversionError errMsg
    where
         errMsg =
             "Failure to process DAML program, this feature is not currently supported.\n" ++
             typ ++ ".\n" ++
             prettyPrint x

unknown :: HasCallStack => GHC.UnitId -> MS.Map GHC.UnitId T.Text -> ConvertM e
unknown unitId pkgMap = conversionError errMsg
    where errMsg =
              "Unknown package: " ++ GHC.unitIdString unitId
              ++ "\n" ++  "Loaded packages are:" ++ prettyPrint (MS.keys pkgMap)

unhandled :: (HasCallStack, Data a, Outputable a) => String -> a -> ConvertM e
unhandled typ x = unsupported (typ ++ " with " ++ lower (show (toConstr x))) x

---------------------------------------------------------------------
-- FUNCTIONS ON THE ENVIRONMENT

data Env = Env
    {envLFModuleName :: LF.ModuleName
    ,envGHCModuleName :: GHC.ModuleName
    ,envModuleUnitId :: GHC.UnitId
    ,envBindings :: MS.Map Var (GHC.Expr Var)
    ,envChoices :: MS.Map String [GHC.Expr Var]
    ,envKeys :: MS.Map String [GHC.Expr Var]
    ,envDefaultMethods :: Set.Set Name
    ,envAliases :: MS.Map Var LF.Expr
    ,envPkgMap :: MS.Map GHC.UnitId T.Text
    ,envLfVersion :: LF.Version
    }

envFindBind :: Env -> Var -> ConvertM (GHC.Expr Var)
envFindBind Env{..} var =
  case MS.lookup var envBindings of
    Nothing -> conversionError errMsg
    Just v -> pure v
    where errMsg = "Looking for local binding failed when looking up " ++ prettyPrint var ++ ", available " ++ prettyPrint (MS.keys envBindings)

envFindChoices :: Env -> String -> [GHC.Expr Var]
envFindChoices Env{..} x = fromMaybe [] $ MS.lookup x envChoices

envFindKeys :: Env -> String -> [GHC.Expr Var]
envFindKeys Env{..} x = fromMaybe [] $ MS.lookup x envKeys

-- x is an alias for y
envInsertAlias :: Var -> Var -> Env -> ConvertM Env
envInsertAlias x y env = do
    z <- maybe (convertExpr env (Var y)) pure (envLookupAlias y env)
    pure env{envAliases = MS.insert x z (envAliases env)}

envLookupAlias :: Var -> Env -> Maybe LF.Expr
envLookupAlias x = MS.lookup x . envAliases

---------------------------------------------------------------------
-- CONVERSION

data ConversionError
  = ConversionError
     { errorFilePath :: !(Maybe FilePath)
     , errorRange :: !(Maybe Range)
     , errorMessage :: !String
     }
  deriving Show

data ConversionEnv = ConversionEnv
  { convModuleFilePath :: !(Maybe FilePath)
  , convRange :: !(Maybe SourceLoc)
  }

newtype ConvertM a = ConvertM (ReaderT ConversionEnv (Except Diagnostic) a)
  deriving (Functor, Applicative, Monad, MonadError Diagnostic, MonadReader ConversionEnv)

instance MonadFail ConvertM where
    fail = conversionError

runConvertM :: ConversionEnv -> ConvertM a -> Either Diagnostic a
runConvertM s (ConvertM a) = runExcept (runReaderT a s)

withRange :: Maybe SourceLoc -> ConvertM a -> ConvertM a
withRange r = local (\s -> s { convRange = r })

isBuiltinName :: NamedThing a => String -> a -> Bool
isBuiltinName expected a
  | getOccString a == expected
  , Just m <- nameModule_maybe (getName a)
  , GHC.moduleName m == mkModuleName "DA.Internal.Prelude" = True
  | otherwise = False

isBuiltinOptional :: NamedThing a => Env -> a -> Bool
isBuiltinOptional env a =
    supportsOptional (envLfVersion env) && isBuiltinName "Optional" a

isBuiltinSome :: NamedThing a => Env -> a -> Bool
isBuiltinSome env a =
    supportsOptional (envLfVersion env) && isBuiltinName "Some" a

isBuiltinNone :: NamedThing a => Env -> a -> Bool
isBuiltinNone env a =
    supportsOptional (envLfVersion env) && isBuiltinName "None" a

isBuiltinTextMap :: NamedThing a => Env -> a -> Bool
isBuiltinTextMap env a =
    supportsTextMap (envLfVersion env) && isBuiltinName "TextMap" a

toInt64 :: Integer -> Maybe Int64
toInt64 x
    | toInteger (minBound :: Int64) <= x && x <= toInteger (maxBound :: Int64) =
        Just $ fromInteger x
    | otherwise = Nothing

convertModule :: LF.Version -> MS.Map UnitId T.Text -> GhcModule -> Either Diagnostic LF.Module
convertModule lfVersion pkgMap mod0 = runConvertM (ConversionEnv (gmPath mod0) Nothing) $ do
    definitions <- concat <$> traverse (convertBind env) (cm_binds x)
    types <- concat <$> traverse (convertTypeDef env) (eltsUFM (cm_types x))
    pure (LF.moduleFromDefinitions lfModName (gmPath mod0) flags (types ++ definitions))
    where
        x = gmCore mod0
        ghcModName = GHC.moduleName $ cm_module x
        thisUnitId = GHC.moduleUnitId $ cm_module x
        lfModName = convertModuleName ghcModName
        flags = LF.daml12FeatureFlags
        binds = concat [case x' of NonRec a b -> [(a,b)]; Rec xs -> xs | x' <- cm_binds x]
        bindings = MS.fromList binds
        choices = MS.fromListWith (++)
          [(is x', [b])
          | (a,b) <- binds
          , DFunId _ <- [idDetails a]
          , TypeCon (Is "Choice") [TypeCon x' [],_,_] <- [varType a]
          ]
        keys = MS.fromListWith (++)
          [(is x', [b])
          | (a,b) <- binds
          , DFunId _ <- [idDetails a]
          , TypeCon (Is "Key") [TypeCon x' [],_] <- [varType a]
          ]
        defMeths = defaultMethods x
        env = Env
          { envLFModuleName = lfModName
          , envGHCModuleName = ghcModName
          , envModuleUnitId = thisUnitId
          , envBindings = bindings
          , envChoices = choices
          , envKeys = keys
          , envDefaultMethods = defMeths
          , envAliases = MS.empty
          , envPkgMap = pkgMap
          , envLfVersion = lfVersion
          }


convertTemplate :: Env -> GHC.Expr Var -> ConvertM [Definition]
convertTemplate env (VarIs "C:Template" `App` Type (TypeCon ty [])
        `App` ensure `App` signatories `App` observer `App` agreement `App` _create `App` _fetch `App` _archive)
    = do
    tplSignatories <- applyTplParam <$> convertExpr env signatories
    tplChoices <- fmap (\cs -> NM.fromList (archiveChoice tplSignatories : cs)) choices
    tplObservers <- applyTplParam <$> convertExpr env observer
    tplPrecondition <- applyTplParam <$> convertExpr env ensure
    tplAgreement <- applyTplParam <$> convertExpr env agreement
    tplKey <- keys >>= \case
      [] -> return Nothing
      [x] -> return (Just x)
      _:_ -> conversionError ("multiple keys found for template " ++ prettyPrint ty)
    pure [DTemplate Template{..}]
    where
        applyTplParam e = e `ETmApp` EVar tplParam
        choices = traverse (convertChoice env) $ envFindChoices env $ is ty
        keys = mapM (convertKey env) $ envFindKeys env $ is ty
        tplLocation = Nothing
        tplTypeCon = mkTypeCon [is ty]
        tplParam = mkVar "this"
convertTemplate _ x = unsupported "Template definition with unexpected form" x

archiveChoice :: LF.Expr -> TemplateChoice
archiveChoice signatories = TemplateChoice{..}
    where
        chcLocation = Nothing
        chcName = mkChoiceName "Archive"
        chcReturnType = TUnit
        chcConsuming = True
        chcControllers = signatories
        chcUpdate = EUpdate $ UPure TUnit mkEUnit
        chcSelfBinder = mkVar "self"
        chcArgBinder = (mkVar "arg", TUnit)

convertChoice :: Env -> GHC.Expr Var -> ConvertM TemplateChoice
convertChoice env (VarIs "C:Choice" `App` Type tmpl `App` Type (TypeCon chc []) `App` Type result `App` _templateDict
        `App` consuming `App` Var controller `App` choice `App` _exercise) = do
    chcConsuming <- f (10 :: Int) consuming
    argType <- convertType env $ TypeCon chc []
    let chcArgBinder = (mkVar "arg", argType)
    tmplType <- convertType env tmpl
    chcReturnType <- convertType env result
    controllerExpr <- envFindBind env controller >>= convertExpr env
    let chcControllers = case controllerExpr of
            -- NOTE(MH): We drop the second argument to `controllerExpr` when
            -- it is unused. This is necessary to make sure that a
            -- non-flexible controller expression does not mention the choice
            -- argument `argVar`.
            ETmLam thisBndr (ETmLam argBndr body)
              | fst argBndr `Set.notMember` cata freeVarsStep body ->
                ETmLam thisBndr body `ETmApp` thisVar
            _ ->
                controllerExpr `ETmApp` thisVar `ETmApp` argVar
    chcUpdate <- fmap (\u -> u `ETmApp` thisVar `ETmApp` selfVar `ETmApp` argVar) (convertExpr env choice)
    pure TemplateChoice{..}
    where
        chcLocation = Nothing
        chcName = mkChoiceName $ occNameString $ nameOccName $ getName chc
        chcSelfBinder = mkVar "self"
        thisVar = EVar (mkVar "this")
        selfVar = EVar (mkVar "self")
        argVar = EVar (mkVar "arg")

        f i (App a _) = f i a
        f i (VarIs "nonconsuming") = pure False
        f i (VarIs "$dmconsuming") = pure True -- the default is consuming
        f i (Var x) | i > 0 = f (i-1) =<< envFindBind env x -- only required to see through the automatic default
        f _ x = unsupported "Unexpected definition of 'consuming', expected either absent or 'nonconsuming'" x
convertChoice _ x = unhandled "Choice body" x

convertKey :: Env -> GHC.Expr Var -> ConvertM TemplateKey
convertKey env (VarIs "C:Key" `App` Type tmpl `App` Type keyType `App` _templateDict `App` Var key `App` keyMaintainers `App` _fetch `App` _lookup) = do
    proxyTyp <- qGHC_Types env (mkTypeCon ["Proxy"])
    tmpl' <- convertType env tmpl
    key <- envFindBind env key
    case key of
      Lam binder key -> do
        let env' = env{envAliases = MS.insert binder (EVar (mkVar "this")) (envAliases env)}
        TemplateKey
          <$> convertType env keyType
          <*> convertExpr env' key
          <*> ((`ETmApp` ERecCon (TypeConApp proxyTyp [tmpl']) []) <$> convertExpr env keyMaintainers)
      x -> unhandled "Key definition" x
convertKey _ x = unhandled "Key definition" x

convertTypeDef :: Env -> TyThing -> ConvertM [Definition]
convertTypeDef env (ATyCon t)
  | moduleNameString (GHC.moduleName (nameModule (getName t))) == "DA.Internal.LF"
  , is t `elem` internalTypes
  = pure []
convertTypeDef env o@(ATyCon t) = withRange (convNameLoc t) $
    case tyConFlavour t of
      fl | fl `elem` [ClassFlavour,DataTypeFlavour,NewtypeFlavour] -> convertCtors env =<< toCtors env t
      TypeSynonymFlavour -> pure []
      _ -> unsupported ("Data definition, of type " ++ prettyPrint (tyConFlavour t)) o
convertTypeDef env x = pure []


-- TODO(MH): There's some overlap between the `$ctor:Foo` functions generated
-- here and the `$WFoo` functions generated by GHC. At some point we need to
-- figure out which ones to keep and which ones to throw away.
convertCtors :: Env -> Ctors -> ConvertM [Definition]
convertCtors env (Ctors name tys [o@(Ctor ctor fldNames fldTys)])
  | isRecordCtor o
  = pure [defDataType tconName tys $ DataRecord flds
    ,defValue name (Tagged $ T.pack $ "$ctor:" ++ getOccString ctor, mkTForalls tys $ mkTFuns fldTys (typeConAppToType tcon)) expr
    ]
    where
        flds = zipExact fldNames fldTys
        tconName = mkTypeCon [getOccString name]
        tcon = TypeConApp (Qualified PRSelf (envLFModuleName env) tconName) $ map (TVar . fst) tys
        expr = mkETyLams tys $ mkETmLams (map (first retag) flds) $ ERecCon tcon [(l, EVar $ retag l) | l <- fldNames]
convertCtors env (Ctors name tys cs) = do
    (constrs, funs) <- unzip <$> traverse convertCtor cs
    pure $ [defDataType tconName tys $ DataVariant constrs] ++ concat funs
    where
      tconName = mkTypeCon [getOccString name]
      convertCtor :: Ctor -> ConvertM ((VariantConName, LF.Type), [Definition])
      convertCtor o@(Ctor ctor fldNames fldTys) =
        case (fldNames, fldTys) of
          ([], []) -> pure ((ctorName, TUnit), [ctorFun [] mkEUnit])
          ([], [typ]) -> pure ((ctorName, typ), [ctorFun [mkField "arg"] (EVar (mkVar "arg"))])
          ([], _:_:_) -> unsupported "Data constructor with multiple unnamed fields" (show name)
          (_:_, _) ->
            let recName = synthesizeVariantRecord ctorName tconName
                recData = defDataType recName tys $ DataRecord (zipExact fldNames fldTys)
                recTCon = TypeConApp (Qualified PRSelf (envLFModuleName env) recName) $ map (TVar . fst) tys
                recExpr = ERecCon recTCon [(f, EVar (retag f)) | f <- fldNames]
            in  pure ((ctorName, typeConAppToType recTCon), [recData, ctorFun fldNames recExpr])
          where
            ctorName = mkVariantCon (getOccString ctor)
            tcon = TypeConApp (Qualified PRSelf (envLFModuleName env) tconName) $ map (TVar . fst) tys
            tres = mkTForalls tys $ mkTFuns fldTys (typeConAppToType tcon)
            ctorFun fldNames' ctorArg =
              defValue ctor (Tagged $ T.pack $ "$ctor:" ++ getOccString ctor, tres) $
              mkETyLams tys $ mkETmLams (zipExact (map retag fldNames') fldTys) $ EVariantCon tcon ctorName ctorArg


convertBind :: Env -> CoreBind -> ConvertM [Definition]
convertBind env (NonRec name x)
    | DFunId _ <- idDetails name
    , TypeCon (Is "Template") [t] <- varType name
    = withRange (convNameLoc name) $ liftA2 (++) (convertTemplate env x) (convertBind2 env (NonRec name x))
convertBind env x = convertBind2 env x

convertBind2 :: Env -> CoreBind -> ConvertM [Definition]
convertBind2 env (NonRec name x)
    | Just internals <- MS.lookup (envGHCModuleName env) internalFunctions
    , is name `elem` internals
    = pure []
    -- NOTE(MH): String literals should never appear at the top level since
    -- they must be wrapped in either 'unpackCString#' or 'unpackCStringUtf8#'
    -- to decide how to decode them.
    | Lit LitString {} <- x
    = unhandled "String literal at top level" x
    | otherwise
    = withRange (convNameLoc name) $ do
    x' <- convertExpr env x
    let sanitize = case idDetails name of
          -- NOTE(MH): This is DICTIONARY SANITIZATION step (3).
          DFunId{} ->
            over (_ETyLams . _2 . _ETmLams . _2 . _ETmApps . _2 . each) (ETmLam (mkVar "_", TUnit))
          _ -> id
    name' <- convValWithType env name
    pure [defValue name name' (sanitize x')]
convertBind2 env (Rec xs) = concat <$> traverse (\(a, b) -> convertBind env (NonRec a b)) xs

-- NOTE(MH): These are the names of the builtin DAML-LF types whose Surface
-- DAML counterpart is not defined in 'GHC.Types'. They are all defined in
-- 'DA.Internal.LF' in terms of 'GHC.Types.Opaque'. We need to remove them
-- during conversion to DAML-LF together with their constructors since we
-- deliberately remove 'GHC.Types.Opaque' as well.
internalTypes :: [String]
internalTypes = ["Scenario","Update","ContractId","Time","Date","Party"]

internalFunctions :: MS.Map GHC.ModuleName [String]
internalFunctions = MS.fromList $ map (first mkModuleName)
    [ ("DA.Internal.Record",
        [ "getFieldPrim"
        , "setFieldPrim"
        ])
    , ("DA.Internal.Template", -- template funcs defined with magic
        [ "$dminternalExercise"
        , "$dminternalFetch"
        , "$dminternalCreate"
        , "$dminternalArchive"
        , "$dminternalFetchByKey"
        , "$dminternalLookupByKey"
        ])
    , ("DA.Internal.LF", map ("$W" ++) internalTypes)
    , ("GHC.Base",
        [ "getTag"
        ])
    ]


convertExpr :: Env -> GHC.Expr Var -> ConvertM LF.Expr
convertExpr env0 e = do
    (e, args) <- go env0 e []
    args <- mapM (convertArg env0) args
    pure $ mkEApps e args
  where
    -- NOTE(MH): We collect all arguments to functions in a list such that
    -- we have access to them when converting functions. This is necessary to
    -- inline fully applied record constructors instead of going through a
    -- record constructor function, which is required for contract keys.
    go :: Env -> GHC.Expr Var -> [GHC.Arg Var] -> ConvertM (LF.Expr, [GHC.Expr Var])
    go env (x `App` y) args
        = go env x (y : args)
    -- see through $ to give better matching power
    go env (VarIs "$") (Type _ : Type _ : x : y : args)
        = go env x (y : args)

    go env (VarIs "$dminternalCreate") (Type t@(TypeCon tmpl []) : _dict : args) = fmap (, args) $ do
        t' <- convertType env t
        tmpl' <- convertQualified env tmpl
        pure $ ETmLam (varV1, t') $ EUpdate $ UCreate tmpl' $ EVar varV1
    go env (VarIs "$dminternalFetch") (Type t@(TypeCon tmpl []) : _dict : args) = fmap (, args) $ do
        t' <- convertType env t
        tmpl' <- convertQualified env tmpl
        pure . ETmLam (varV1, TContractId t') $ EUpdate $ UFetch tmpl' $ EVar varV1
    go env (VarIs "$dminternalExercise") (Type t@(TypeCon tmpl []) : Type c@(TypeCon chc []) : Type _result : _dict : args) = fmap (, args) $ do
        t' <- convertType env t
        c' <- convertType env c
        tmpl' <- convertQualified env tmpl
        pure $
            ETmLam (varV1, TList TParty) $ ETmLam (varV2, TContractId t') $ ETmLam (varV3, c') $
            EUpdate $ UExercise tmpl' (mkChoiceName $ is chc) (EVar varV2) (EVar varV1) (EVar varV3)
    go env (VarIs "$dminternalArchive") (Type t@(TypeCon tmpl []) : _dict : args) = fmap (, args) $ do
        t' <- convertType env t
        tmpl' <- convertQualified env tmpl
        pure $
            ETmLam (varV1, TList TParty) $ ETmLam (varV2, TContractId t') $
            EUpdate $ UExercise tmpl' (mkChoiceName "Archive") (EVar varV2) (EVar varV1) mkEUnit
    go env (VarIs "$dminternalFetchByKey") (Type t@(TypeCon tmpl []) : Type key : _dict : args)
        = fmap (, args) $ do
        key' <- convertType env key
        tmpl' <- convertQualified env tmpl
        tuple2Typ <- qGHC_Tuple env (mkTypeCon ["Tuple2"])
        let contractType = TConApp tmpl' []
        let cidType = TContractId contractType
        pure $ ETmLam (varV1, key') $
          EUpdate $ UBind
            (Binding
              (varV2, TTuple [(Tagged "contractId", cidType), (Tagged "contract", contractType)])
              (EUpdate (UFetchByKey RetrieveByKey
                { retrieveByKeyTemplate = tmpl'
                , retrieveByKeyKey = EVar varV1
                })))
            (EUpdate (UPure (TConApp tuple2Typ [cidType, contractType]) (ERecCon
              (TypeConApp tuple2Typ [cidType, contractType])
              [ (Tagged "_1", ETupleProj (Tagged "contractId") (EVar varV2))
              , (Tagged "_2", ETupleProj (Tagged "contract") (EVar varV2))
              ])))
    go env (VarIs "$dminternalLookupByKey") (Type t@(TypeCon tmpl []) : Type key : _dict : args)
        = fmap (, args) $ do
        key' <- convertType env key
        tmpl' <- convertQualified env tmpl
        pure $ ETmLam (varV1, key') $
          EUpdate $ ULookupByKey RetrieveByKey
            { retrieveByKeyTemplate = tmpl'
            , retrieveByKeyKey = EVar varV1
            }
    go env (VarIs "primitive") (Type (isStrLitTy -> Just y) : Type t : args)
        = fmap (, args) $ convertPrim (envLfVersion env) (unpackFS y) <$> convertType env t
    go env (VarIs "getFieldPrim") (Type (isStrLitTy -> Just name) : Type record : Type _field : args) = fmap (, args) $ do
        record' <- convertType env record
        pure $ ETmLam (varV1, record') $ ERecProj (fromTCon record') (mkField $ unpackFS name) $ EVar varV1
    go env (VarIs "getField") (Type (isStrLitTy -> Just name) : Type recordType : Type _fieldType : _dict : record : args) = fmap (, args) $ do
            recordType <- convertType env recordType
            record <- convertExpr env record
            pure $ ERecProj (fromTCon recordType) (mkField $ unpackFS name) record
    go env (VarIs "setFieldPrim") (Type (isStrLitTy -> Just name) : Type record : Type field : args) = fmap (, args) $ do
        record' <- convertType env record
        field' <- convertType env field
        pure $
            ETmLam (varV1, field') $ ETmLam (varV2, record') $
            ERecUpd (fromTCon record') (mkField $ unpackFS name) (EVar varV2) (EVar varV1)
    go env (VarIs "fromRational") ((VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEDecimal $ fromRational $ top % bot
    go env (VarIs "negate") (tyInt : VarIs "$fAdditiveInt" : (VarIs "fromInteger" `App` Lit (LitNumber _ x _)) : args)
        | Just y <- toInt64 (negate x)
        = fmap (, args) $ pure $ EBuiltin $ BEInt64 y
    go env (VarIs "fromInteger") (Lit (LitNumber _ x _) : args)
        | Just y <- toInt64 x
        = fmap (, args) $ pure $ EBuiltin $ BEInt64 y
    go env (VarIs "fromString") (x : args)
        = fmap (, args) $ convertExpr env x
    go env (VarIs "unpackCString#") (Lit (LitString x) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEText $ T.decodeLatin1 x
    go env (VarIs "unpackCStringUtf8#") (Lit (LitString x) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEText $ T.decodeUtf8 x
    go env x@(Var f) (Type t1 : Type t2 : Lit (LitString s) : args)
        | Just m <- nameModule_maybe (getName f)
        , moduleNameString (GHC.moduleName m) == "Control.Exception.Base"
        = fmap (, args) $ do
        x' <- convertExpr env x
        t1' <- convertType env t1
        t2' <- convertType env t2
        pure (x' `ETyApp` t1' `ETyApp` t2' `ETmApp` EBuiltin (BEText (T.decodeUtf8 s)))

    -- conversion of bodies of $con2tag functions
    go env (VarIs "getTag") (Type (TypeCon t _) : x : args) = fmap (, args) $ do
        Ctors _ _ cs <- toCtors env t
        x' <- convertExpr env x
        t' <- convertQualified env t
        pure $ ECase x'
            [ CaseAlternative (CPVariant t' (mkVariantCon (getOccString variantName)) (mkVar "_")) (EBuiltin $ BEInt64 i)
            | (Ctor variantName _ _, i) <- zip cs [0..]
            ]
    go env (VarIs "tagToEnum#") (Type (TypeCon (Is "Bool") []) : (op0 `App` x `App` y) : args)
        | VarIs "==#" <- op0 = go BEEqual
        | VarIs "<#"  <- op0 = go BELess
        | VarIs ">#"  <- op0 = go BEGreater
        where
          go op1 = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              pure (EBuiltin (op1 BTInt64) `ETmApp` x' `ETmApp` y')
    go env (VarIs "tagToEnum#") (Type (TypeCon (Is "Bool") []) : x : args) = fmap (, args) $ do
        x' <- convertExpr env x
        pure $ EBuiltin (BEEqual BTInt64) `ETmApp` EBuiltin (BEInt64 1) `ETmApp` x'
    go env (VarIs "tagToEnum#") (Type tt@(TypeCon t _) : x : args) = fmap (, args) $ do
        -- FIXME: Should generate a binary tree of eq and compare
        Ctors _ _ cs@(c1:_) <- toCtors env t
        tt' <- convertType env tt
        x' <- convertExpr env x
        let mkCtor (Ctor c _ _) = EVariantCon (fromTCon tt') (mkVariantCon (getOccString c)) mkEUnit
            mkEqInt i = EBuiltin (BEEqual BTInt64) `ETmApp` x' `ETmApp` EBuiltin (BEInt64 i)
        pure (foldr ($) (mkCtor c1) [mkIf (mkEqInt i) (mkCtor c) | (i,c) <- zipFrom 0 cs])
    go env (Lit (LitNumber LitNumInt x _)) args
        | toInteger (minBound :: Int64) <= x && x <= toInteger (maxBound :: Int64)
        = fmap (, args) $ pure $ EBuiltin $ BEInt64 $ fromInteger x

    -- built ins because they are lazy
    go env (VarIs "ifThenElse") (Type tRes : cond : true : false : args)
        = fmap (, args) $ mkIf <$> convertExpr env cond <*> convertExpr env true <*> convertExpr env false
    go env (VarIs "||") (x : y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> pure ETrue <*> convertExpr env y
    go env (VarIs "&&") (x : y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> pure EFalse
    go env (VarIs "when") (Type monad : dict : x : y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> mkPure env monad dict TUnit EUnit
    go env (VarIs "unless") (Type monad : dict : x : y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> mkPure env monad dict TUnit EUnit <*> convertExpr env y
    go env (VarIs "submit") (Type typ : pty : upd : args) = fmap (, args) $ do
        pty' <- convertExpr env pty
        upd' <- convertExpr env upd
        typ' <- convertType env typ
        pure $
          EScenario (SCommit typ' pty' (EUpdate (UEmbedExpr typ' upd')))
    go env (VarIs "submitMustFail") (Type typ : pty : upd : args) = fmap (, args) $ do
        pty' <- convertExpr env pty
        upd' <- convertExpr env upd
        typ' <- convertType env typ
        pure $ EScenario (SMustFailAt typ' pty' (EUpdate (UEmbedExpr typ' upd')))

    -- custom conversion because they correspond to builtins in DAML-LF, so can make the output more readable
    go env (VarIs "pure") (Type monad : dict : Type t : x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env (VarIs "return") (Type monad : Type t : dict : x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        -- In all other cases, 'return' is rewritten to 'pure'.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env bind@(VarIs ">>=") allArgs@(Type monad : dict : Type _ : Type _ : x : lam@(Lam b y) : args) = do
        monad' <- convertType env monad
        case monad' of
          TBuiltin BTUpdate -> mkBind EUpdate UBind
          TBuiltin BTScenario -> mkBind EScenario SBind
          _ -> fmap (, allArgs) $ convertExpr env bind
        where
          mkBind inj bind = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              b' <- convVarWithType env b
              pure (inj (bind (Binding b' x') y'))
    go env semi@(VarIs ">>") allArgs@(Type monad : Type t : Type _ : _dict : x : y : args) = do
        monad' <- convertType env monad
        case monad' of
          TBuiltin BTUpdate -> mkSeq EUpdate UBind
          TBuiltin BTScenario -> mkSeq EScenario SBind
          _ -> fmap (, allArgs) $ convertExpr env semi
        where
          mkSeq inj bind = fmap (, args) $ do
              t' <- convertType env t
              x' <- convertExpr env x
              y' <- convertExpr env y
              pure $ inj (bind (Binding (mkVar "_", t') x') y')

    go env (VarIs "[]") (Type (TypeCon (Is "Char") []) : args)
        = fmap (, args) $ pure $ EBuiltin (BEText T.empty)
    go env (VarIs "[]") (Type t : args)
        = fmap (, args) $ ENil <$> convertType env t
    go env (VarIs "[]") args
        = fmap (, args) $ pure $ ETyLam varT1 $ ENil (TVar (fst varT1))
    -- TODO(MH): We should use the same technique as in GenDALF to avoid
    -- generating useless lambdas.
    go env (VarIs ":") (Type t : args) = fmap (, args) $ do
        t' <- convertType env t
        pure $ mkETmLams [(varV1, t'), (varV2, TList t')] $ ECons t' (EVar varV1) (EVar varV2)
    go env (VarIs ":") args
        = fmap (, args) $ do
            let t' = TVar (fst varT1)
            pure $ ETyLam varT1 $ mkETmLams [(varV1, t'), (varV2, TList t')] $ ECons t' (EVar varV1) (EVar varV2)
    go env (Var v) (Type t : args)
        | isBuiltinNone env v
        = fmap (, args) $ ENone <$> convertType env t
    go env (Var v) args
        | isBuiltinNone env v
        = fmap (, args) $ pure $ ETyLam varT1 $ ENone (TVar (fst varT1))
    go env (Var v) (Type t : args)
        | isBuiltinSome env v
        = fmap (, args) $ do
            t' <- convertType env t
            pure $ mkETmLams [(varV1, t')] $ ESome t' (EVar varV1)
    go env (Var v) args
        | isBuiltinSome env v
        = fmap (, args) $ do
            let t' = TVar (fst varT1)
            pure $ ETyLam varT1 $ mkETmLams [(varV1, t')] $ ESome t' (EVar varV1)
    go env (Var x) args
        | Just internals <- MS.lookup modName internalFunctions
        , is x `elem` internals
        = unsupported "Direct call to internal function" x
        | is x == "()" = fmap (, args) $ pure mkEUnit
        | is x == "True" = fmap (, args) $ pure $ mkBool True
        | is x == "False" = fmap (, args) $ pure $ mkBool False
        | is x == "I#" = fmap (, args) $ pure $ mkIdentity TInt64 -- we pretend Int and Int# are the same thing
        -- NOTE(MH): Handle data constructors. Fully applied record
        -- constructors are inlined. This is required for contract keys to
        -- work. Constructor workers are not handled (yet).
        | Just m <- nameModule_maybe $ varName x
        , Just con <- isDataConId_maybe x
        , not ("$W" `isPrefixOf` is x)
        = do
            unitId <- convertUnitId (envModuleUnitId env) (envPkgMap env) $ GHC.moduleUnitId m
            let qualify = Qualified unitId (convertModuleName $ GHC.moduleName m)
            ctor@(Ctor _ fldNames _) <- toCtor env con
            let renameTuple ('(':',':xs) | dropWhile (== ',') xs == ")" = "Tuple" ++ show (length xs + 1)
                renameTuple t = t
            -- NOTE(MH): The first case are fully applied record constructors,
            -- the second case is everything else.
            if  | let tycon = dataConTyCon con
                , tyConFlavour tycon /= ClassFlavour && isRecordCtor ctor && isSingleConType tycon
                , let n = length (dataConUnivTyVars con)
                , let (tyArgs, tmArgs) = splitAt n args
                , length tyArgs == n && length tmArgs == length fldNames
                , Just tyArgs <- mapM isType_maybe tyArgs
                , all (isNothing . isType_maybe) tmArgs
                -> fmap (, []) $ do
                    tyArgs <- mapM (convertType env) tyArgs
                    tmArgs <- mapM (convertExpr env) tmArgs
                    let tcon = TypeConApp (qualify (mkTypeCon [renameTuple (is (dataConTyCon con))])) tyArgs
                    pure $ ERecCon tcon (zip fldNames tmArgs)
                | otherwise
                -> fmap (, args) $ pure $ EVal $ qualify $ mkVal ("$ctor:" ++ renameTuple (is x))
        | Just m <- nameModule_maybe $ varName x = fmap (, args) $ do
            unitId <- convertUnitId (envModuleUnitId env) (envPkgMap env) $ GHC.moduleUnitId m
            pure $ EVal $
              Qualified
                unitId
                (convertModuleName $ GHC.moduleName m) $
              convVal x
        | isGlobalId x = fmap (, args) $ do
            pkgRef <- nameToPkgRef env $ varName x
            pure $ EVal $ Qualified pkgRef (envLFModuleName env) $ convVal x
            -- some things are global, but not with a module name, so give them the current one
        | Just y <- envLookupAlias x env = fmap (, args) $ pure y
        | otherwise = fmap (, args) $ pure $ EVar $ convVar x
        where
          modName = maybe (envGHCModuleName env) GHC.moduleName $ nameModule_maybe $ getName x

    go env (Lam name x) args
        | isTyVar name = fmap (, args) $ ETyLam <$> convTypeVar name <*> convertExpr env x
        | otherwise = fmap (, args) $ ETmLam <$> convVarWithType env name <*> convertExpr env x
    go env (Cast x co) args = fmap (, args) $ do
        x' <- convertExpr env x
        convertCast env x' co
    go env (Let (NonRec name x) y) args
        | Var v <- x
        = fmap (, args) $ do
             env' <- envInsertAlias name v env
             convertExpr env' y
        | otherwise
        = fmap (, args) $ do
             x' <- convertExpr env x
             y' <- convertExpr env y
             name' <- convVarWithType env name
             pure (ELet (Binding name' x') y')
    go env (Case scrutinee bind t [(DataAlt con, [v], x)]) args | is con == "I#" = fmap (, args) $ do
        -- we pretend Int and Int# are the same, so a case statement becomes a Let
        scrutinee' <- convertExpr env scrutinee
        x' <- convertExpr env x
        pure $
            ELet (Binding (convVar bind, TInt64) scrutinee') $
            ELet (Binding (convVar v, TInt64) (EVar (convVar bind))) x'
    -- NOTE(MH): This is DICTIONARY SANITIZATION step (2).
    go env (Case scrutinee bind _ [(DataAlt con, vs, Var x)]) args
        | tyConFlavour (dataConTyCon con) == ClassFlavour
        = fmap (, args) $ do
            scrutinee' <- convertExpr env scrutinee
            Ctor _ fldNames _ <- toCtor env con
            let fldIndex = fromJust (elemIndex x vs)
            let fldName = fldNames !! fldIndex
            recTyp <- convertType env (varType bind)
            pure $ ERecProj (fromTCon recTyp) fldName scrutinee' `ETmApp` EUnit
    go env o@(Case scrutinee bind _ [alt@(DataAlt con, vs, x)]) args = fmap (, args) $ do
        scrutinee' <- convertExpr env scrutinee
        convertType env (varType bind) >>= \case
          TText -> asLet
          TDecimal -> asLet
          TParty -> asLet
          TTimestamp -> asLet
          TDate -> asLet
          TContractId{} -> asLet
          TUpdate{} -> asLet
          TScenario{} -> asLet
          tcon0 -> do
              ctor <- toCtor env con
              if not (isRecordCtor ctor)
                then do
                  ty <- convertType env $ varType bind
                  alt' <- convertAlt env ty alt
                  bind' <- convVarWithType env bind
                  pure $
                    ELet (Binding bind' scrutinee') $
                    ECase (EVar $ convVar bind) [alt']
                else mkProj ctor tcon0 scrutinee'
      where
        asLet = do
            scrutinee' <- convertExpr env scrutinee
            x' <- convertExpr env x
            bind' <- convVarWithType env bind
            pure $ ELet (Binding bind' scrutinee') x'
        -- NOTE(MH): The 'zipExact' below fails for pattern matches on constructors with existentials.
        mkProj (Ctor _ fldNames fldTys) tcon bound = case zipExactMay vs (zipExact fldNames fldTys) of
          Nothing -> unsupported "Pattern match with existential type" alt
          Just vsFlds -> do
              x' <- convertExpr env x
              projBinds <- mkProjBindings env (convVar bind) (fromTCon tcon) vsFlds x'
              pure $
                ELet (Binding (convVar bind, tcon) bound) projBinds
    go env (Case scrutinee bind typ []) args = fmap (, args) $ do
        -- GHC only generates empty case alternatives if it is sure the scrutinee will fail, LF doesn't support empty alternatives
        scrutinee' <- convertExpr env scrutinee
        typ' <- convertType env typ
        bind' <- convVarWithType env bind
        pure $
          ELet (Binding bind' scrutinee') $
          ECase (EVar $ convVar bind) [CaseAlternative CPDefault $ EBuiltin BEError `ETyApp` typ' `ETmApp` EBuiltin (BEText $ T.pack "Unreachable")]
    go env (Case (Var v) bind _ [(DEFAULT, [], x)]) args = fmap (, args) $ do
        env' <- envInsertAlias bind v env
        convertExpr env' x
    go env (Case scrutinee bind _ [(DEFAULT, [], x)]) args = fmap (, args) $ do
        scrutinee' <- convertExpr env scrutinee
        x' <- convertExpr env x
        bind' <- convVarWithType env bind
        pure $ ELet (Binding bind' scrutinee') x'
    go env (Case scrutinee bind _ (defaultLast -> alts)) args = fmap (, args) $ do
        scrutinee' <- convertExpr env scrutinee
        bindTy <- convertType env $ varType bind
        alts' <- traverse (convertAlt env bindTy) alts
        bind' <- convVarWithType env bind
        if isDeadOcc (occInfo (idInfo bind))
          then pure $ ECase scrutinee' alts'
          else pure $
            ELet (Binding bind' scrutinee') $
            ECase (EVar $ convVar bind) alts'
    go env (Tick _ x) args = go env x args
    go env (Let (Rec xs) _) args = unsupported "Local variables defined recursively - recursion can only happen at the top level" $ map fst xs
    go env o@(Coercion _) args = unhandled "Coercion" o
    go _ x args = unhandled "Expression" x

    convertArg :: Env -> GHC.Arg Var -> ConvertM LF.Arg
    convertArg env = \case
        Type t -> TyArg <$> convertType env t
        e -> TmArg <$> convertExpr env e

-- | Convert ghc package unit id's to LF package references.
convertUnitId :: GHC.UnitId -> MS.Map GHC.UnitId T.Text -> UnitId -> ConvertM LF.PackageRef
convertUnitId thisUnitId _pkgMap unitId | unitId == thisUnitId = pure LF.PRSelf
convertUnitId _thisUnitId pkgMap unitId = case unitId of
  IndefiniteUnitId x -> unsupported "Indefinite unit id's" x
  DefiniteUnitId _ -> case MS.lookup unitId pkgMap of
    Just hash -> pure $ LF.PRImport $ Tagged hash
    Nothing -> unknown unitId pkgMap

convertAlt :: Env -> LF.Type -> Alt Var -> ConvertM CaseAlternative
convertAlt env ty (DEFAULT, [], x) = CaseAlternative CPDefault <$> convertExpr env x
convertAlt env ty (DataAlt con, [], x)
    | is con == "True" = CaseAlternative (CPEnumCon ECTrue) <$> convertExpr env x
    | is con == "False" = CaseAlternative (CPEnumCon ECFalse) <$> convertExpr env x
    | is con == "[]" = CaseAlternative CPNil <$> convertExpr env x
    | is con == "()" = CaseAlternative (CPEnumCon ECUnit) <$> convertExpr env x
    | isBuiltinNone env con
    = CaseAlternative CPNone <$> convertExpr env x
convertAlt env ty (DataAlt con, [a,b], x)
    | is con == ":" = CaseAlternative (CPCons (convVar a) (convVar b)) <$> convertExpr env x
convertAlt env ty (DataAlt con, [a], x)
    | isBuiltinSome env con
    = CaseAlternative (CPSome (convVar a)) <$> convertExpr env x
convertAlt env (TConApp tcon targs) alt@(DataAlt con, vs, x) = do
    Ctor (mkVariantCon . getOccString -> variantName) fldNames fldTys <- toCtor env con
    let patVariant = variantName
    if null fldNames
      then
        case zipExactMay vs fldTys of
          Nothing -> unsupported "Pattern match with existential type" alt
          Just [] ->
            let patBinder = vArg
            in  CaseAlternative CPVariant{..} <$> convertExpr env x
          Just [(v, _)] ->
            let patBinder = convVar v
            in  CaseAlternative CPVariant{..} <$> convertExpr env x
          Just (_:_:_) -> unsupported "Data constructor with multiple unnamed fields" alt
      else
        case zipExactMay vs (zipExact fldNames fldTys) of
          Nothing -> unsupported "Pattern match with existential type" alt
          Just vsFlds ->
            let patBinder = vArg
            in  do
              x' <- convertExpr env x
              projBinds <- mkProjBindings env vArg (TypeConApp (synthesizeVariantRecord variantName <$> tcon) targs) vsFlds x'
              pure $ CaseAlternative CPVariant{..} projBinds
    where
        -- TODO(MH): We need to generate fresh names.
        vArg = mkVar "$arg"
        patTypeCon = tcon
convertAlt _ _ x = unsupported "Case alternative of this form" x

mkProjBindings :: Env -> ExprVarName -> TypeConApp -> [(Var, (FieldName, LF.Type))] -> LF.Expr -> ConvertM LF.Expr
mkProjBindings env recVar recTyp vsFlds e =
  fmap (\bindings -> mkELets bindings e) $ sequence
    [ Binding <$> convVarWithType env v <*> pure (ERecProj recTyp fld (EVar recVar))
    | (v, (fld, _typ)) <- vsFlds
    , not (isDeadOcc (occInfo (idInfo v)))
    ]

-- | Convert casts induced by newtype definitions. The definition
--
-- > newtype T = MkT S
--
-- induces two coercion axioms:
--
-- (1) @S ~ T@ -- newtype construction
--
-- (2) @T ~ S@ -- newtype projection
--
-- In both cases, the newtype @T@ can also be the dictionary type of
-- a type class with a single method and no super classes.
--
-- Coercions induced by newtypes can also occur deeply nested in function
-- types or forall quantifications. We handle those cases by recursion into
-- all sub-coercions.
convertCast :: Env -> LF.Expr -> Coercion -> ConvertM LF.Expr
convertCast env expr0 co0 = evalStateT (go expr0 co0) 0
  where
    go :: LF.Expr -> Coercion -> StateT Int ConvertM LF.Expr
    go expr co
      | isReflCo co
      = pure expr

      | Just (xCo, yCo) <- splitFunCo_maybe  co
      = do
        let Pair _ t = coercionKind xCo
        n <- state (\k -> let l = k+1 in (l, l))
        let var = mkVar ("$cast" ++ show n)
        -- NOTE(MH): We need @mkSymCo xCo@ here because the arrow type is
        -- contravariant in its first argument but GHC's coercions
        -- don't reflect this fact.
        var' <- go (EVar var) (mkSymCo xCo)
        t' <- lift (convertType env t)
        ETmLam (var, t') <$> go (expr `ETmApp` var') yCo

      | Just (x, k_co, co') <- splitForAllCo_maybe co
      , isReflCo k_co
      = do
        (x', kind) <- lift $ convTypeVar x
        ETyLam (x', kind) <$> go (expr `ETyApp` TVar x') co'

      -- Case (1) & (2)
      | Pair s t <- coercionKind co
      = lift $ do
        mCase1 <- isSatNewTyCon s t
        mCase2 <- isSatNewTyCon t s
        case (mCase1, mCase2) of
          (Just (tcon, field, flv), _) ->
            let sanitize x
                  -- NOTE(MH): This is DICTIONARY SANITIZATION step (3).
                  | flv == ClassFlavour = ETmLam (mkVar "_", TUnit) x
                  | otherwise = x
            in pure $ ERecCon tcon [(field, sanitize expr)]
          (_, Just (tcon, field, flv)) ->
            let sanitize x
                  -- NOTE(MH): This is DICTIONARY SANITIZATION step (2).
                  | flv == ClassFlavour = x `ETmApp` EUnit
                  | otherwise = x
            in pure $ sanitize (ERecProj tcon field expr)
          _ -> unhandled "Coercion" co
    isSatNewTyCon :: GHC.Type -> GHC.Type -> ConvertM (Maybe (TypeConApp, FieldName, TyConFlavour))
    isSatNewTyCon s (TypeCon t ts)
      | Just data_con <- newTyConDataCon_maybe t
      , (tvs, rhs) <- newTyConRhs t
      , length ts == length tvs
      , applyTysX tvs rhs ts `eqType` s
      , [field] <- ctorLabels flv data_con
      = do
      ts' <- traverse (convertType env) ts
      t' <- convertQualified env t
      pure $ Just (TypeConApp t' ts', field, flv)
      where
        flv = tyConFlavour t
    isSatNewTyCon _ _
      = pure Nothing

convertModuleName :: GHC.ModuleName -> LF.ModuleName
convertModuleName (moduleNameString -> x)
    = mkModName $ splitOn "." x

qGHC_Tuple :: Env -> a -> ConvertM (Qualified a)
qGHC_Tuple env a = do
  pkgRef <- packageNameToPkgRef env "daml-prim"
  pure $ Qualified pkgRef (mkModName ["GHC", "Tuple"]) a

qGHC_Types :: Env -> a -> ConvertM (Qualified a)
qGHC_Types env a = do
  pkgRef <- packageNameToPkgRef env "daml-prim"
  pure $ Qualified pkgRef (mkModName ["GHC", "Types"]) a

convertQualified :: NamedThing a => Env -> a -> ConvertM (Qualified TypeConName)
convertQualified env x = do
  pkgRef <- nameToPkgRef env x
  pure $ Qualified
    pkgRef
    (convertModuleName $ GHC.moduleName $ nameModule $ getName x)
    (mkTypeCon [is x])

nameToPkgRef :: NamedThing a => Env -> a -> ConvertM LF.PackageRef
nameToPkgRef env x =
  maybe (pure LF.PRSelf) (convertUnitId thisUnitId pkgMap . moduleUnitId) $
  Name.nameModule_maybe $ getName x
  where
    thisUnitId = envModuleUnitId env
    pkgMap = envPkgMap env

packageNameToPkgRef :: Env -> String -> ConvertM LF.PackageRef
packageNameToPkgRef env =
  convertUnitId (envModuleUnitId env) (envPkgMap env) . GHC.stringToUnitId

convertTyCon :: Env -> TyCon -> ConvertM LF.Type
convertTyCon env t
    | t == unitTyCon = pure TUnit
    | isTupleTyCon t, arity >= 2 = TCon <$> qGHC_Tuple env (mkTypeCon ["Tuple" ++ show arity])
    | t == listTyCon = pure (TBuiltin BTList)
    | t == boolTyCon = pure TBool
    | t == intTyCon || t == intPrimTyCon = pure TInt64
    | t == charTyCon = unsupported "Type GHC.Types.Char" t
    | t == liftedRepDataConTyCon = pure TUnit
    | t == typeSymbolKindCon = unsupported "Type GHC.Types.Symbol" t
    | Just m <- nameModule_maybe (getName t), m == gHC_TYPES =
        case getOccString t of
            "Text" -> pure TText
            "Decimal" -> pure TDecimal
            _ -> defaultTyCon
    -- TODO(DEL-6953): We need to add a condition on the package name as well.
    | Just m <- nameModule_maybe (getName t), GHC.moduleName m == mkModuleName "DA.Internal.LF" =
        case getOccString t of
            "Scenario" -> pure (TBuiltin BTScenario)
            "ContractId" -> pure (TBuiltin BTContractId)
            "Update" -> pure (TBuiltin BTUpdate)
            "Party" -> pure TParty
            "Date" -> pure TDate
            "Time" -> pure TTimestamp
            _ -> defaultTyCon
    | isBuiltinOptional env t = pure (TBuiltin BTOptional)
    | isBuiltinTextMap env t = pure (TBuiltin BTMap)
    | otherwise = defaultTyCon
    where
        arity = tyConArity t
        defaultTyCon = TCon <$> convertQualified env t

convertType :: Env -> GHC.Type -> ConvertM LF.Type
convertType env o@(TypeCon t ts)
    | t == listTyCon, ts `eqTypes` [charTy] = pure TText
    | t == anyTyCon, [_] <- ts = pure TUnit -- used for type-zonking
    | t == funTyCon, _:_:ts' <- ts =
        if supportsArrowType (envLfVersion env) || length ts' == 2
          then foldl TApp TArrow <$> traverse (convertType env) ts'
          else unsupported "Partial application of (->)" o
    | tyConFlavour t == TypeSynonymFlavour = convertType env $ expandTypeSynonyms o
    | otherwise = mkTApps <$> convertTyCon env t <*> traverse (convertType env) ts
convertType env t | Just (v, t') <- splitForAllTy_maybe t
  = TForall <$> convTypeVar v <*> convertType env t'
convertType env t | Just t' <- getTyVar_maybe t
  = TVar . fst <$> convTypeVar t'
convertType env t | Just s <- isStrLitTy t
  = pure TUnit
convertType env t | Just (a,b) <- splitAppTy_maybe t
  = TApp <$> convertType env a <*> convertType env b
convertType env x
  = unhandled "Type" x


convertKind :: GHC.Kind -> ConvertM LF.Kind
convertKind x@(TypeCon t ts)
    | t == typeSymbolKindCon, null ts = pure KStar
    | t == tYPETyCon, [_] <- ts = pure KStar
    | t == runtimeRepTyCon, null ts = pure KStar
    | t == funTyCon, [_,_,t1,t2] <- ts = KArrow <$> convertKind t1 <*> convertKind t2
    | otherwise = unhandled "Kind" x

convNameLoc :: NamedThing a => a -> Maybe LF.SourceLoc
convNameLoc n = case nameSrcSpan (getName n) of
  -- NOTE(MH): Locations are 1-based in GHC and 0-based in DAML-LF.
  -- Hence all the @- 1@s below.
  RealSrcSpan srcSpan -> Just SourceLoc
    { slocModuleRef = Nothing
    , slocStartLine = srcSpanStartLine srcSpan - 1
    , slocStartCol = srcSpanStartCol srcSpan - 1
    , slocEndLine = srcSpanEndLine srcSpan - 1
    , slocEndCol = srcSpanEndCol srcSpan - 1
    }
  UnhelpfulSpan{} -> Nothing

---------------------------------------------------------------------
-- SMART CONSTRUCTORS

defDataType :: TypeConName -> [(TypeVarName, LF.Kind)] -> DataCons -> Definition
defDataType name params constrs =
  DDataType $ DefDataType Nothing name (IsSerializable False) params constrs

defValue :: NamedThing a => a -> (ExprValName, LF.Type) -> LF.Expr -> Definition
defValue loc binder@(name, lftype) body =
  DValue $ DefValue (convNameLoc loc) binder (HasNoPartyLiterals True) (IsTest isTest) body
  where
    isTest = case view _TForalls lftype of
      (_, LF.TScenario _)  -> True
      _ -> False

---------------------------------------------------------------------
-- UNPACK CONSTRUCTORS

data Ctors = Ctors Name [(TypeVarName, LF.Kind)] [Ctor] deriving Show
data Ctor = Ctor Name [FieldName] [LF.Type] deriving Show

toCtors :: Env -> GHC.TyCon -> ConvertM Ctors
toCtors env t = Ctors (getName t) <$> traverse convTypeVar (tyConTyVars t) <*> cs
    where
        cs = case algTyConRhs t of
                DataTyCon cs' _ _ -> traverse (toCtor env) cs'
                NewTyCon{..} -> sequence [toCtor env data_con]
                x -> unsupported "Data definition, with unexpected RHS" t

-- NOTE(MH):
--
-- * Dictionary types contain multiple unnamed fields in general. Thus, it is
--   necessary to give them synthetic names. This is not a problem at all
--   since they don't show up in user interfaces.
--
-- * We treat all newtypes like records. First of all, not doing
--   this would considerably complicate converting coercions. Second, it makes
--   unpacking them faster in the current intepreter. Since the constructor
--   names of newtypes are meant to be meaningless, this is acceptable.
ctorLabels :: TyConFlavour -> DataCon -> [FieldName]
ctorLabels flv con
  | flv `elem` [ClassFlavour, TupleFlavour Boxed] || isTupleDataCon con
    -- NOTE(MH): The line below is a workaround for ghc issue
    -- https://github.com/ghc/ghc/blob/ae4f1033cfe131fca9416e2993bda081e1f8c152/compiler/types/TyCon.hs#L2030
    || (is con == "Unit" && moduleNameString (GHC.moduleName (nameModule (getName con))) == "GHC.Tuple")
  = map (mkField . (:) '_' . show) [1..dataConSourceArity con]
  | flv == NewtypeFlavour && null lbls
  = [mkField "unpack"]
  | otherwise
  = map convFieldName lbls
  where lbls = dataConFieldLabels con

toCtor :: Env -> DataCon -> ConvertM Ctor
toCtor env con =
  let (_,_,tys,_) = dataConSig con
      flv = tyConFlavour (dataConTyCon con)
      sanitize ty
        -- NOTE(MH): This is DICTIONARY SANITIZATION step (1).
        | flv == ClassFlavour = TUnit :-> ty
        | otherwise = ty
  in Ctor (getName con) (ctorLabels flv con) <$> traverse (fmap sanitize . convertType env) tys

isRecordCtor :: Ctor -> Bool
isRecordCtor (Ctor _ fldNames fldTys) = not (null fldNames) || null fldTys


---------------------------------------------------------------------
-- SIMPLE WRAPPERS

convFieldName :: FieldLbl a -> FieldName
convFieldName = mkField . unpackFS . flLabel

convTypeVar :: Var -> ConvertM (TypeVarName, LF.Kind)
convTypeVar t = do
    k <- convertKind $ tyVarKind t
    pure (mkTypeVar $ varPrettyPrint t, k)

convVar :: Var -> ExprVarName
convVar = mkVar . varPrettyPrint

convVarWithType :: Env -> Var -> ConvertM (ExprVarName, LF.Type)
convVarWithType env v = (convVar v,) <$> convertType env (varType v)

convVal :: Var -> ExprValName
convVal = mkVal . varPrettyPrint

convValWithType :: Env -> Var -> ConvertM (ExprValName, LF.Type)
convValWithType env v = (convVal v,) <$> convertType env (varType v)

mkPure :: Env -> GHC.Type -> GHC.Expr Var -> LF.Type -> LF.Expr -> ConvertM LF.Expr
mkPure env monad dict t x = do
    monad' <- convertType env monad
    case monad' of
      TBuiltin BTUpdate -> pure $ EUpdate (UPure t x)
      TBuiltin BTScenario -> pure $ EScenario (SPure t x)
      _ -> do
        dict' <- convertExpr env dict
        pkgRef <- if envModuleUnitId env == stringToUnitId "daml-prim" && envLfVersion env <= LF.version1_3
                  -- TODO (drsk): Get rid of this when daml-lf tests can handle dars.
                  then pure LF.PRSelf
                  else packageNameToPkgRef env "daml-stdlib"
        pure $ EVal (Qualified pkgRef (mkModName ["DA", "Internal", "Prelude"]) (mkVal "pure"))
          `ETyApp` monad'
          `ETmApp` dict'
          `ETyApp` t
          `ETmApp` x

sourceLocToRange :: SourceLoc -> Range
sourceLocToRange (SourceLoc _ slin scol elin ecol) =
  Range (Position slin scol) (Position elin ecol)
