-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE PatternSynonyms     #-}
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
-- lazy. In contrast, DAML-LF is strict. This mismatch causes a few problems.
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
-- GHC produces a @newtype@ rather than a @data@ type for dictionary types of
-- type classes with a single method and no super classes. Since DAML-LF does
-- not support @newtype@, we could either treat them as some sort of type
-- synonym or translate them to a record type with a single field. We have
-- chosen to do the latter for the sake of uniformity among all dictionary
-- types. GHC Core contains neither constructor nor projection functions for
-- @newtype@s but uses coercions instead. We translate these coercions to
-- proper record construction/projection.

module DA.Daml.LFConversion
    ( convertModule
    , sourceLocToRange
    ) where

import           DA.Daml.LFConversion.Primitives
import           DA.Daml.LFConversion.UtilGHC
import           DA.Daml.LFConversion.UtilLF

import           Development.IDE.Types.Diagnostics
import           Development.IDE.Types.Location
import           Development.IDE.GHC.Util

import           Control.Lens
import           Control.Monad.Except
import           Control.Monad.Extra
import Control.Monad.Fail
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Ast.Numeric (numericFromDecimal)
import           Data.Data hiding (TyCon)
import           Data.Foldable (foldlM)
import           Data.Int
import           Data.List.Extra
import qualified Data.Map.Strict as MS
import           Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import           Data.Tuple.Extra
import           Data.Ratio
import           "ghc-lib" GHC
import           "ghc-lib" GhcPlugins as GHC hiding ((<>))
import           "ghc-lib-parser" Pair hiding (swap)
import           "ghc-lib-parser" PrelNames
import           "ghc-lib-parser" TysPrim
import           "ghc-lib-parser" TyCoRep
import qualified "ghc-lib-parser" Name
import           Safe.Exact (zipExact, zipExactMay)
import           SdkVersion

---------------------------------------------------------------------
-- FAILURE REPORTING

conversionError :: String -> ConvertM e
conversionError msg = do
  ConversionEnv{..} <- ask
  throwError $ (convModuleFilePath,) Diagnostic
      { _range = maybe noRange sourceLocToRange convRange
      , _severity = Just DsError
      , _source = Just "Core to DAML-LF"
      , _message = T.pack msg
      , _code = Nothing
      , _relatedInformation = Nothing
      } where

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
    ,envAliases :: MS.Map Var LF.Expr
    ,envPkgMap :: MS.Map GHC.UnitId T.Text
    ,envLfVersion :: LF.Version
    ,envNewtypes :: [(GHC.Type, (TyCon, Coercion))]
    }

-- v is an alias for x
envInsertAlias :: Var -> LF.Expr -> Env -> Env
envInsertAlias v x env = env{envAliases = MS.insert v x (envAliases env)}

envLookupAlias :: Var -> Env -> Maybe LF.Expr
envLookupAlias x = MS.lookup x . envAliases

---------------------------------------------------------------------
-- CONVERSION

data ConversionError
  = ConversionError
     { errorFilePath :: !NormalizedFilePath
     , errorRange :: !(Maybe Range)
     , errorMessage :: !String
     }
  deriving Show

data ConversionEnv = ConversionEnv
  { convModuleFilePath :: !NormalizedFilePath
  , convRange :: !(Maybe SourceLoc)
  }

newtype ConvertM a = ConvertM (ReaderT ConversionEnv (Except FileDiagnostic) a)
  deriving (Functor, Applicative, Monad, MonadError FileDiagnostic, MonadReader ConversionEnv)

instance MonadFail ConvertM where
    fail = conversionError

runConvertM :: ConversionEnv -> ConvertM a -> Either FileDiagnostic a
runConvertM s (ConvertM a) = runExcept (runReaderT a s)

withRange :: Maybe SourceLoc -> ConvertM a -> ConvertM a
withRange r = local (\s -> s { convRange = r })

isBuiltinName :: NamedThing a => FastString -> a -> Bool
isBuiltinName expected a
  | getOccFS a == expected
  , Just m <- nameModule_maybe (getName a)
  , GHC.moduleName m == mkModuleName "DA.Internal.Prelude" = True
  | otherwise = False

convertInt64 :: Integer -> ConvertM LF.Expr
convertInt64 x
    | toInteger (minBound :: Int64) <= x && x <= toInteger (maxBound :: Int64) =
        pure $ EBuiltin $ BEInt64 (fromInteger x)
    | otherwise =
        unsupported "Int literal out of bounds" (negate x)

convertRational :: Env -> Integer -> Integer -> ConvertM LF.Expr
convertRational env num denom
 =
    -- the denominator needs to be a divisor of 10^10.
    -- num % denom * 10^10 needs to fit within a 128bit signed number.
    -- note that we can also get negative rationals here, hence we ask for upperBound128Bit - 1 as
    -- upper limit.
    if | 10 ^ maxPrecision `mod` denom == 0 &&
             abs (r * 10 ^ maxPrecision) <= upperBound128Bit - 1 ->
            pure $ EBuiltin $
            if envLfVersion env `supports` featureNumeric
                then BENumeric $ numericFromDecimal $ fromRational r
                else BEDecimal $ fromRational r
       | otherwise ->
           unsupported
               ("Rational is out of bounds: " ++
                show ((fromInteger num / fromInteger denom) :: Double) ++
                ".  Maximal supported precision is e^-10, maximal range after multiplying with 10^10 is [10^38 -1, -10^38 + 1]")
               (num, denom)
  where
    r = num % denom
    upperBound128Bit = 10 ^ (38 :: Integer)
    maxPrecision = 10 :: Integer

convertModule :: LF.Version -> MS.Map UnitId T.Text -> NormalizedFilePath -> CoreModule -> Either FileDiagnostic LF.Module
convertModule lfVersion pkgMap file x = runConvertM (ConversionEnv file Nothing) $ do
    definitions <- concatMapM (convertBind env) binds
    types <- concatMapM (convertTypeDef env) (eltsUFM (cm_types x))
    pure (LF.moduleFromDefinitions lfModName (Just $ fromNormalizedFilePath file) flags (types ++ definitions))
    where
        ghcModName = GHC.moduleName $ cm_module x
        thisUnitId = GHC.moduleUnitId $ cm_module x
        lfModName = convertModuleName ghcModName
        flags = LF.daml12FeatureFlags
        binds =
          [ bind
          | bindGroup <- cm_binds x
          , bind <- case bindGroup of
              NonRec name body
                -- NOTE(MH): We can't cope with the generated Typeable stuff, so remove those bindings
                | any (`T.isPrefixOf` getOccText name) ["$krep", "$tc", "$trModule"] -> []
                | otherwise -> [(name, body)]
              Rec binds -> binds
          ]
        newtypes =
          [ (wrappedT, (t, mkUnbranchedAxInstCo Representational co [] []))
          | ATyCon t <- eltsUFM (cm_types x)
          , Just ([], wrappedT, co) <- [unwrapNewTyCon_maybe t]
          ]
        env = Env
          { envLFModuleName = lfModName
          , envGHCModuleName = ghcModName
          , envModuleUnitId = thisUnitId
          , envAliases = MS.empty
          , envPkgMap = pkgMap
          , envLfVersion = lfVersion
          , envNewtypes = newtypes
          }

-- TODO(MH): We should run this on an `LF.Expr` instead of a `GHC.Expr`.
-- This will avoid a fair bit of repetition.
convertGenericTemplate :: Env -> GHC.Expr Var -> ConvertM (Template, LF.Expr)
convertGenericTemplate env x
    | (Var dictCon, args) <- collectArgs x
    , Just m <- nameModule_maybe $ varName dictCon
    , Just dictCon <- isDataConId_maybe dictCon
    , (tyArgs, args) <- span isTypeArg args
    , Just tyArgs <- mapM isType_maybe tyArgs
    , Just (superClassDicts, signatories : observers : ensure : agreement : create : _fetch : archive : keyAndChoices) <- span isSuperClassDict <$> mapM isVar_maybe (dropWhile isTypeArg args)
    , Just (polyType, _) <- splitFunTy_maybe (varType create)
    , Just (monoTyCon, unwrapCo) <- findMonoTyp polyType
    = do
        let tplLocation = convNameLoc monoTyCon
        polyType <- convertType env polyType
        monoType@(TCon monoTyCon) <- convertTyCon env monoTyCon
        (unwrapTpl, wrapTpl) <- convertCoercion env unwrapCo
        let (unwrapCid, wrapCid)
                | isReflCo unwrapCo = (id, id)
                | otherwise =
                    ( ETmApp $ mkETyApps (EBuiltin BECoerceContractId) [monoType, polyType]
                    , ETmApp $ mkETyApps (EBuiltin BECoerceContractId) [polyType, monoType]
                    )
        let tplTypeCon = qualObject monoTyCon
        let tplParam = this
        let applyThis e = ETmApp e $ unwrapTpl $ EVar this
        tplSignatories <- applyThis <$> convertExpr env (Var signatories)
        tplObservers <- applyThis <$> convertExpr env (Var observers)
        tplPrecondition <- applyThis <$> convertExpr env (Var ensure)
        tplAgreement <- applyThis <$> convertExpr env (Var agreement)
        archive <- convertExpr env (Var archive)
        (tplKey, key, choices) <- case keyAndChoices of
                hasKey : key : maintainers : _fetchByKey : _lookupByKey : choices
                    | TypeCon (Is "HasKey") _ <- varType hasKey -> do
                        _ :-> keyType <- convertType env (varType key)
                        hasKey <- convertExpr env (Var hasKey)
                        key <- convertExpr env (Var key)
                        maintainers <- convertExpr env (Var maintainers)
                        let selfField = FieldName "contractId"
                        let thisField = FieldName "contract"
                        tupleTyCon <- qDA_Types env $ mkTypeCon ["Tuple2"]
                        let tupleType = TypeConApp tupleTyCon [TContractId polyType, polyType]
                        let fetchByKey =
                                ETmLam (mkVar "key", keyType) $
                                EUpdate $ UBind (Binding (res, TTuple [(selfField, TContractId monoType), (thisField, monoType)]) $ EUpdate $ UFetchByKey $ RetrieveByKey monoTyCon $ EVar $ mkVar "key") $
                                EUpdate $ UPure (typeConAppToType tupleType) $ ERecCon tupleType
                                    [ (FieldName "_1", unwrapCid $ ETupleProj selfField $ EVar res)
                                    , (FieldName "_2", unwrapTpl $ ETupleProj thisField $ EVar res)
                                    ]
                        let lookupByKey =
                                ETmLam (mkVar "key", keyType) $
                                EUpdate $ UBind (Binding (res, TOptional (TContractId monoType)) $ EUpdate $ ULookupByKey $ RetrieveByKey monoTyCon $ EVar $ mkVar "key") $
                                EUpdate $ UPure (TOptional (TContractId polyType)) $ ECase (EVar res)
                                    [ CaseAlternative CPNone $ ENone (TContractId polyType)
                                    , CaseAlternative (CPSome self) $ ESome (TContractId polyType) $ unwrapCid $ EVar self
                                    ]
                        pure (Just $ TemplateKey keyType (applyThis key) (ETmApp maintainers hasKey), [hasKey, key, maintainers, fetchByKey, lookupByKey], choices)
                choices -> pure (Nothing, [], choices)
        let convertGenericChoice :: [Var] -> ConvertM (TemplateChoice, [LF.Expr])
            convertGenericChoice [consumption, controllers, action, _exercise] = do
                TContractId _ :-> _ :-> argType@(TConApp argTCon _) :-> TUpdate resType <- convertType env (varType action)
                let chcLocation = Nothing
                let chcName = ChoiceName $ T.intercalate "." $ unTypeConName $ qualObject argTCon
                consumptionType <- case varType consumption of
                    TypeCon (Is "NonConsuming") _ -> pure NonConsuming
                    TypeCon (Is "PreConsuming") _ -> pure PreConsuming
                    TypeCon (Is "PostConsuming") _ -> pure PostConsuming
                    t -> unhandled "choice consumption type" t
                let chcConsuming = consumptionType == PreConsuming
                let chcSelfBinder = self
                let applySelf e = ETmApp e $ unwrapCid $ EVar self
                let chcArgBinder = (arg, argType)
                let applyArg e = e `ETmApp` EVar (fst chcArgBinder)
                let chcReturnType = resType
                consumption <- convertExpr env (Var consumption)
                chcControllers <- applyArg . applyThis <$> convertExpr env (Var controllers)
                update <- applyArg . applyThis . applySelf <$> convertExpr env (Var action)
                let chcUpdate
                      | consumptionType /= PostConsuming = update
                      | otherwise =
                        EUpdate $ UBind (Binding (res, resType) update) $
                        EUpdate $ UBind (Binding (mkVar "_", TUnit) $ ETmApp archive $ EVar self) $
                        EUpdate $ UPure resType $ EVar res
                controllers <- convertExpr env (Var controllers)
                action <- convertExpr env (Var action)
                let exercise =
                        mkETmLams [(self, TContractId polyType), (arg, argType)] $
                          EUpdate $ UExercise monoTyCon chcName (wrapCid $ EVar self) Nothing (EVar arg)
                pure (TemplateChoice{..}, [consumption, controllers, action, exercise])
            convertGenericChoice es = unhandled "generic choice" es
        (tplChoices, choices) <- first NM.fromList . unzip <$> mapM convertGenericChoice (chunksOf 4 choices)
        superClassDicts <- mapM (convertExpr env . Var) superClassDicts
        signatories <- convertExpr env (Var signatories)
        observers <- convertExpr env (Var observers)
        ensure <- convertExpr env (Var ensure)
        agreement <- convertExpr env (Var agreement)
        let create = ETmLam (this, polyType) $ EUpdate $ UBind (Binding (self, TContractId monoType) $ EUpdate $ UCreate monoTyCon $ wrapTpl $ EVar this) $ EUpdate $ UPure (TContractId polyType) $ unwrapCid $ EVar self
        let fetch = ETmLam (self, TContractId polyType) $ EUpdate $ UBind (Binding (this, monoType) $ EUpdate $ UFetch monoTyCon $ wrapCid $ EVar self) $ EUpdate $ UPure polyType $ unwrapTpl $ EVar this
        tyArgs <- mapM (convertType env) tyArgs
        -- NOTE(MH): The additional lambda is DICTIONARY SANITIZATION step (3).
        let tmArgs = map (ETmLam (mkVar "_", TUnit)) $ superClassDicts ++ [signatories, observers, ensure, agreement, create, fetch, archive] ++ key ++ concat choices
        qTCon <- qualify env m  $ mkTypeCon [getOccText $ dataConTyCon dictCon]
        let tcon = TypeConApp qTCon tyArgs
        Ctor _ fldNames _ <- toCtor env dictCon
        let dict = ERecCon tcon (zip fldNames tmArgs)
        pure (Template{..}, dict)
  where
    isVar_maybe :: GHC.Expr Var -> Maybe Var
    isVar_maybe = \case
        Var v -> Just v
        _ -> Nothing
    isSuperClassDict :: Var -> Bool
    -- NOTE(MH): We need the `$f` case since GHC inlines super class
    -- dictionaries without running the simplifier under some circumstances.
    isSuperClassDict v = any (`T.isPrefixOf` getOccText v) ["$cp", "$f"]
    findMonoTyp :: GHC.Type -> Maybe (TyCon, Coercion)
    findMonoTyp t = case t of
        TypeCon tcon [] -> Just (tcon, mkNomReflCo t)
        t -> snd <$> find (eqType t . fst) (envNewtypes env)
    this = mkVar "this"
    self = mkVar "self"
    arg = mkVar "arg"
    res = mkVar "res"
convertGenericTemplate env x = unhandled "generic template" x

data Consuming = PreConsuming
               | NonConsuming
               | PostConsuming
               deriving (Eq)

convertTypeDef :: Env -> TyThing -> ConvertM [Definition]
convertTypeDef env (ATyCon t)
  | GHC.moduleNameFS (GHC.moduleName (nameModule (getName t))) == "DA.Internal.LF"
  , getOccFS t `elementOfUniqSet` internalTypes
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
convertCtors env (Ctors name flavour tys [o@(Ctor ctor fldNames fldTys)])
  | isRecordCtor o
  = pure $ [defDataType tconName tys $ DataRecord flds] ++
      [ defValue name (mkVal $ "$W" <> getOccText ctor, mkTForalls tys $ mkTFuns fldTys (typeConAppToType tcon)) expr
      | flavour == NewtypeFlavour
      ]
    where
        flds = zipExact fldNames fldTys
        tconName = mkTypeCon [getOccText name]
        tcon = TypeConApp (Qualified PRSelf (envLFModuleName env) tconName) $ map (TVar . fst) tys
        expr = mkETyLams tys $ mkETmLams (map (first fieldToVar ) flds) $ ERecCon tcon [(l, EVar $ fieldToVar l) | l <- fldNames]
convertCtors env o@(Ctors name _ _ cs) | isEnumCtors o = do
    let ctorNames = map (\(Ctor ctor _ _) -> mkVariantCon $ getOccText ctor) cs
    pure [defDataType tconName [] $ DataEnum ctorNames]
  where
    tconName = mkTypeCon [getOccText name]
convertCtors env (Ctors name _ tys cs) = do
    (constrs, funs) <- mapAndUnzipM convertCtor cs
    pure $ [defDataType tconName tys $ DataVariant constrs] ++ concat funs
    where
      tconName = mkTypeCon [getOccText name]
      convertCtor :: Ctor -> ConvertM ((VariantConName, LF.Type), [Definition])
      convertCtor o@(Ctor ctor fldNames fldTys) =
        case (fldNames, fldTys) of
          ([], []) -> pure ((ctorName, TUnit), [ctorFun [] EUnit])
          ([], [typ]) -> pure ((ctorName, typ), [ctorFun [mkField "arg"] (EVar (mkVar "arg"))])
          ([], _:_:_) -> unsupported "Data constructor with multiple unnamed fields" (prettyPrint name)
          (_:_, _) ->
            let recName = synthesizeVariantRecord ctorName tconName
                recData = defDataType recName tys $ DataRecord (zipExact fldNames fldTys)
                recTCon = TypeConApp (Qualified PRSelf (envLFModuleName env) recName) $ map (TVar . fst) tys
                recExpr = ERecCon recTCon [(f, EVar (fieldToVar f)) | f <- fldNames]
            in  pure ((ctorName, typeConAppToType recTCon), [recData, ctorFun fldNames recExpr])
          where
            ctorName = mkVariantCon (getOccText ctor)
            tcon = TypeConApp (Qualified PRSelf (envLFModuleName env) tconName) $ map (TVar . fst) tys
            tres = mkTForalls tys $ mkTFuns fldTys (typeConAppToType tcon)
            ctorFun fldNames' ctorArg =
              defValue ctor (mkVal $ "$ctor:" <> getOccText ctor, tres) $
              mkETyLams tys $ mkETmLams (zipExact (map fieldToVar fldNames') fldTys) $ EVariantCon tcon ctorName ctorArg


convertBind :: Env -> (Var, GHC.Expr Var) -> ConvertM [Definition]
convertBind env (name, x)
    | DFunId _ <- idDetails name
    , TypeCon (Is tplInst) _ <- varType name
    , "Instance" `T.isSuffixOf` fsToText tplInst
    = withRange (convNameLoc name) $ do
        (tmpl, dict) <- convertGenericTemplate env x
        name' <- convValWithType env name
        pure [DTemplate tmpl, defValue name name' dict]
    | Just internals <- lookupUFM internalFunctions (envGHCModuleName env)
    , getOccFS name `elementOfUniqSet` internals
    = pure []
    -- NOTE(MH): Desugaring `template X` will result in a type class
    -- `XInstance` which has methods `_createX`, `_fetchX`, `_exerciseXY`,
    -- `_fetchByKeyX` and `_lookupByKeyX`
    -- (among others). The implementations of these methods are replaced
    -- with DAML-LF primitives in `convertGenericChoice` below. As part of
    -- this rewriting we also need to erase the default implementations of
    -- these methods.
    --
    -- TODO(MH): The check is an approximation which will fail when users
    -- start the name of their own methods with, say, `_exercise`.
    | any (`T.isPrefixOf` getOccText name) [ "$" <> prefix <> "_" <> method | prefix <- ["dm", "c"], method <- ["create", "fetch", "exercise", "fetchByKey", "lookupByKey"] ]
    = pure []
    -- NOTE(MH): Our inline return type syntax produces a local letrec for
    -- recursive functions. We currently don't support local letrecs.
    -- However, we can work around this issue by rewriting
    -- > name = \@a_1 ... @a_n -> letrec f = \v -> y in f  -- (n >= 0)
    -- into
    -- > name = \@a_1 ... @a_n -> \v -> let f = name @a_1 ... @a_n in y
    -- We need to make sure `f` is a term lambda here since we'd end up with a
    -- recursive value otherwise.
    -- (This rewriting can be regarded as a very limited form of lambda
    -- lifting where the lifted version of `f` happens to be `name`.)
    -- This workaround should be removed once we either have a proper lambda
    -- lifter or DAML-LF supports local recursion.
    | (as, Let (Rec [(f, Lam v y)]) (Var f')) <- collectBinders x, f == f'
    = convertBind env $ (,) name $ mkLams as $ Lam v $ Let (NonRec f $ mkVarApps (Var name) as) y
    | otherwise
    = withRange (convNameLoc name) $ do
    x' <- convertExpr env x
    let sanitize = case idDetails name of
          -- NOTE(MH): This is DICTIONARY SANITIZATION step (3).
          -- NOTE (drsk): We only want to do the sanitization for non-newtypes. The sanitization
          -- step 3 for newtypes is done in the convertCoercion function.
          DFunId False ->
            over (_ETyLams . _2 . _ETmLams . _2 . _ERecCon . _2 . each . _2) (ETmLam (mkVar "_", TUnit))
          _ -> id
    name' <- convValWithType env name
    pure [defValue name name' (sanitize x')]

-- NOTE(MH): These are the names of the builtin DAML-LF types whose Surface
-- DAML counterpart is not defined in 'GHC.Types'. They are all defined in
-- 'DA.Internal.LF' in terms of 'GHC.Types.Opaque'. We need to remove them
-- during conversion to DAML-LF together with their constructors since we
-- deliberately remove 'GHC.Types.Opaque' as well.
internalTypes :: UniqSet FastString
internalTypes = mkUniqSet ["Scenario","Update","ContractId","Time","Date","Party","Pair", "TextMap"]

internalFunctions :: UniqFM (UniqSet FastString)
internalFunctions = listToUFM $ map (bimap mkModuleNameFS mkUniqSet)
    [ ("DA.Internal.Record",
        [ "getFieldPrim"
        , "setFieldPrim"
        ])
    , ("DA.Internal.LF", "unpackPair" : map ("$W" <>) (nonDetEltsUniqSet internalTypes))
    , ("GHC.Base",
        [ "getTag"
        ])
    ]


convertExpr :: Env -> GHC.Expr Var -> ConvertM LF.Expr
convertExpr env0 e = do
    (e, args) <- go env0 e []
    let appArg e (mbSrcSpan, arg) =
            mkEApp (maybe id (ELocation . convRealSrcSpan) mbSrcSpan e) <$> convertArg env0 arg
    foldlM appArg e args
  where
    -- NOTE(MH): We collect all arguments to functions in a list such that
    -- we have access to them when converting functions. This is necessary to
    -- inline fully applied record constructors instead of going through a
    -- record constructor function, which is required for contract keys.
    go :: Env -> GHC.Expr Var -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])
    go env (Tick (SourceNote srcSpan _) x) args = case args of
        [] -> do
            x <- convertExpr env x
            pure (ELocation (convRealSrcSpan srcSpan) x, [])
        (_, arg):args -> go env x ((Just srcSpan, arg):args)
    go env (Tick _ x) args = go env x args
    go env (x `App` y) args
        = go env x ((Nothing, y) : args)
    -- see through $ to give better matching power
    go env (VarIs "$") (LType _ : LType _ : LExpr x : y : args)
        = go env x (y : args)

    go env (VarIs "unpackPair") (LType (StrLitTy f1) : LType (StrLitTy f2) : LType t1 : LType t2 : args)
        = fmap (, args) $ do
            t1 <- convertType env t1
            t2 <- convertType env t2
            let fields = [(mkField f1, t1), (mkField f2, t2)]
            tupleTyCon <- qDA_Types env $ mkTypeCon ["Tuple" <> T.pack (show $ length fields)]
            let tupleType = TypeConApp tupleTyCon (map snd fields)
            pure $ ETmLam (varV1, TTuple fields) $ ERecCon tupleType $ zipWithFrom mkFieldProj (1 :: Int) fields
        where
            mkFieldProj i (name, _typ) = (mkField ("_" <> T.pack (show i)), ETupleProj name (EVar varV1))
    go env (VarIs "primitive") (LType (isStrLitTy -> Just y) : LType t : args)
        = fmap (, args) $ convertPrim (envLfVersion env) (unpackFS y) <$> convertType env t
    go env (VarIs "getFieldPrim") (LType (isStrLitTy -> Just name) : LType record : LType _field : args) = do
        record' <- convertType env record
        withTmArg env (varV1, record') args $ \x args ->
            pure (ERecProj (fromTCon record') (mkField $ fsToText name) x, args)
    -- NOTE(MH): We only inline `getField` for record types. This is required
    -- for contract keys. Projections on sum-of-records types have to through
    -- the type class for `getField`.
    go env (VarIs "getField") (LType (isStrLitTy -> Just name) : LType recordType@(TypeCon recordTyCon _) : LType _fieldType : _dict : LExpr record : args)
        | isSingleConType recordTyCon = fmap (, args) $ do
            recordType <- convertType env recordType
            record <- convertExpr env record
            pure $ ERecProj (fromTCon recordType) (mkField $ fsToText name) record
    go env (VarIs "setFieldPrim") (LType (isStrLitTy -> Just name) : LType record : LType field : args) = do
        record' <- convertType env record
        field' <- convertType env field
        withTmArg env (varV1, field') args $ \x1 args ->
            withTmArg env (varV2, record') args $ \x2 args ->
                pure (ERecUpd (fromTCon record') (mkField $ fsToText name) x2 x1, args)
    go env (VarIs "fromRational") (LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRational env top bot
    go env (VarIs "negate") (tyInt : LExpr (VarIs "$fAdditiveInt") : LExpr (untick -> VarIs "fromInteger" `App` Lit (LitNumber _ x _)) : args)
        = fmap (, args) $ convertInt64 (negate x)
    go env (VarIs "fromInteger") (LExpr (Lit (LitNumber _ x _)) : args)
        = fmap (, args) $ convertInt64 x
    go env (Lit (LitNumber LitNumInt x _)) args
        = fmap (, args) $ convertInt64 x
    go env (VarIs "fromString") (LExpr x : args)
        = fmap (, args) $ convertExpr env x
    go env (VarIs "unpackCString#") (LExpr (Lit (LitString x)) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEText $ unpackCString x
    go env (VarIs "unpackCStringUtf8#") (LExpr (Lit (LitString x)) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEText $ unpackCStringUtf8 x
    go env x@(Var f) (LType t1 : LType t2 : LExpr (untick -> Lit (LitString s)) : args)
        | Just m <- nameModule_maybe (getName f)
        , GHC.moduleNameString (GHC.moduleName m) == "Control.Exception.Base"
        = fmap (, args) $ do
        x' <- convertExpr env x
        t1' <- convertType env t1
        t2' <- convertType env t2
        pure (x' `ETyApp` t1' `ETyApp` t2' `ETmApp` EBuiltin (BEText (unpackCStringUtf8 s)))

    -- conversion of bodies of $con2tag functions
    go env (VarIs "getTag") (LType (TypeCon t _) : LExpr x : args) = fmap (, args) $ do
        ctors@(Ctors _ _ _ cs) <- toCtors env t
        x' <- convertExpr env x
        t' <- convertQualified env t
        let mkCasePattern con
                -- Note that tagToEnum# can also be used on non-enum types, i.e.,
                -- types where not all constructors are nullary.
                | isEnumCtors ctors = CPEnum t' con
                | otherwise = CPVariant t' con (mkVar "_")
        pure $ ECase x'
            [ CaseAlternative (mkCasePattern (mkVariantCon (getOccText variantName))) (EBuiltin $ BEInt64 i)
            | (Ctor variantName _ _, i) <- zip cs [0..]
            ]
    go env (VarIs "tagToEnum#") (LType (TypeCon (Is "Bool") []) : LExpr (op0 `App` x `App` y) : args)
        | VarIs "==#" <- op0 = go BEEqual
        | VarIs "<#"  <- op0 = go BELess
        | VarIs ">#"  <- op0 = go BEGreater
        where
          go op1 = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              pure (EBuiltin (op1 BTInt64) `ETmApp` x' `ETmApp` y')
    go env (VarIs "tagToEnum#") (LType (TypeCon (Is "Bool") []) : LExpr x : args) = fmap (, args) $ do
        x' <- convertExpr env x
        pure $ EBuiltin (BEEqual BTInt64) `ETmApp` EBuiltin (BEInt64 1) `ETmApp` x'
    go env (VarIs "tagToEnum#") (LType tt@(TypeCon t _) : LExpr x : args) = fmap (, args) $ do
        -- FIXME: Should generate a binary tree of eq and compare
        ctors@(Ctors _ _ _ cs@(c1:_)) <- toCtors env t
        tt' <- convertType env tt
        x' <- convertExpr env x
        let mkCtor (Ctor c _ _)
              -- Note that tagToEnum# can also be used on non-enum types, i.e.,
              -- types where not all constructors are nullary.
              | isEnumCtors ctors
              = EEnumCon (tcaTypeCon (fromTCon tt')) (mkVariantCon (getOccText c))
              | otherwise
              = EVariantCon (fromTCon tt') (mkVariantCon (getOccText c)) EUnit
            mkEqInt i = EBuiltin (BEEqual BTInt64) `ETmApp` x' `ETmApp` EBuiltin (BEInt64 i)
        pure (foldr ($) (mkCtor c1) [mkIf (mkEqInt i) (mkCtor c) | (i,c) <- zipFrom 0 cs])

    -- built ins because they are lazy
    go env (VarIs "ifThenElse") (LType tRes : LExpr cond : LExpr true : LExpr false : args)
        = fmap (, args) $ mkIf <$> convertExpr env cond <*> convertExpr env true <*> convertExpr env false
    go env (VarIs "||") (LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> pure ETrue <*> convertExpr env y
    go env (VarIs "&&") (LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> pure EFalse
    go env (VarIs "when") (LType monad : LExpr dict : LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> mkPure env monad dict TUnit EUnit
    go env (VarIs "unless") (LType monad : LExpr dict : LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> mkPure env monad dict TUnit EUnit <*> convertExpr env y
    go env (VarIs "submit") (LType typ : LExpr pty : LExpr upd : args) = fmap (, args) $ do
        pty' <- convertExpr env pty
        upd' <- convertExpr env upd
        typ' <- convertType env typ
        pure $
          EScenario (SCommit typ' pty' (EUpdate (UEmbedExpr typ' upd')))
    go env (VarIs "submitMustFail") (LType typ : LExpr pty : LExpr upd : args) = fmap (, args) $ do
        pty' <- convertExpr env pty
        upd' <- convertExpr env upd
        typ' <- convertType env typ
        pure $ EScenario (SMustFailAt typ' pty' (EUpdate (UEmbedExpr typ' upd')))

    -- custom conversion because they correspond to builtins in DAML-LF, so can make the output more readable
    go env (VarIs "pure") (LType monad : LExpr dict : LType t : LExpr x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env (VarIs "return") (LType monad : LType t : LExpr dict : LExpr x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        -- In all other cases, 'return' is rewritten to 'pure'.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env bind@(VarIs ">>=") allArgs@(LType monad : LExpr dict : LType _ : LType _ : LExpr x : LExpr lam@(Lam b y) : args) = do
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
    go env semi@(VarIs ">>") allArgs@(LType monad : LType t : LType _ : LExpr _dict : LExpr x : LExpr y : args) = do
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

    go env (VarIs "[]") (LType (TypeCon (Is "Char") []) : args)
        = fmap (, args) $ pure $ EBuiltin (BEText T.empty)
    go env (VarIs "[]") args
        = withTyArg env varT1 args $ \t args -> pure (ENil t, args)
    go env (VarIs ":") args =
        withTyArg env varT1 args $ \t args ->
        withTmArg env (varV1, t) args $ \x args ->
        withTmArg env (varV2, TList t) args $ \y args ->
          pure (ECons t x y, args)
    go env (Var v) args
        | isBuiltinName "None" v || isBuiltinName "$WNone" v
        = withTyArg env varT1 args $ \t args -> pure (ENone t, args)
    go env (Var v) args
        | isBuiltinName "Some" v || isBuiltinName "$WSome" v
        = withTyArg env varT1 args $ \t args ->
          withTmArg env (varV1, t) args $ \x args ->
            pure (ESome t x, args)
    go env (Var x) args
        | Just internals <- lookupUFM internalFunctions modName
        , getOccFS x `elementOfUniqSet` internals
        = unsupported "Direct call to internal function" x
        | getOccFS x == "()" = fmap (, args) $ pure EUnit
        | getOccFS x == "True" = fmap (, args) $ pure $ mkBool True
        | getOccFS x == "False" = fmap (, args) $ pure $ mkBool False
        | getOccFS x == "I#" = fmap (, args) $ pure $ mkIdentity TInt64 -- we pretend Int and Int# are the same thing
        -- NOTE(MH): Handle data constructors. Fully applied record
        -- constructors are inlined. This is required for contract keys to
        -- work. Constructor workers are not handled (yet).
        | Just m <- nameModule_maybe $ varName x
        , Just con <- isDataConId_maybe x
        = do
            let qual f t
                    | Just xs <- T.stripPrefix "(," t
                    , T.dropWhile (== ',') xs == ")" = qDA_Types env $ f $ "Tuple" <> T.pack (show $ T.length xs + 1)
                    | Just t' <- T.stripPrefix "$W" t = qualify env m $ f t'
                    | otherwise = qualify env m $ f t
            ctor@(Ctor _ fldNames _) <- toCtor env con
            let tycon = dataConTyCon con
            -- NOTE(MH): The first case are fully applied record constructors,
            -- the second case is everything else.
            if  | isRecordCtor ctor && isSingleConType tycon
                , let n = length (dataConUnivTyVars con)
                , let (tyArgs, tmArgs) = splitAt n (map snd args)
                , length tyArgs == n && length tmArgs == length fldNames
                , Just tyArgs <- mapM isType_maybe tyArgs
                , all (isNothing . isType_maybe) tmArgs
                -> fmap (, []) $ do
                    tyArgs <- mapM (convertType env) tyArgs
                    tmArgs <- mapM (convertExpr env) tmArgs
                    qTCon <- qual (mkTypeCon . pure) $ getOccText (dataConTyCon con)
                    let tcon = TypeConApp qTCon tyArgs
                    pure $ ERecCon tcon (zip fldNames tmArgs)
                | isRecordCtor ctor && isSingleConType tycon
                -> fmap (, args) $ fmap EVal $ qual (\x -> mkVal $ "$W" <> x) $ getOccText x
                | isEnumerationTyCon tycon && tyConArity tycon == 0
                -> fmap (, []) $ do
                    unless (null args) $ unhandled "enum constructor with args" (x, args)
                    tcon <- qualify env m $ mkTypeCon [getOccText tycon]
                    pure $ EEnumCon tcon $ mkVariantCon $ getOccText x
                | otherwise
                -> fmap (, args) $ fmap EVal $ qual (\x -> mkVal $ "$ctor:" <> x) $ getOccText x
        | Just m <- nameModule_maybe $ varName x = fmap (, args) $
            fmap EVal $ qualify env m $ convVal x
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
        (to, _from) <- convertCoercion env co
        pure $ to x'
    go env (Let (NonRec name x) y) args =
        fmap (, args) $ convertLet env name x (\env -> convertExpr env y)
    go env (Case scrutinee bind _ [(DEFAULT, [], x)]) args =
        go env (Let (NonRec bind scrutinee) x) args
    go env (Case scrutinee bind t [(DataAlt con, [v], x)]) args | getOccFS con == "I#" = do
        -- We pretend Int and Int# are the same, so a case-expression becomes a let-expression.
        let letExpr = Let (NonRec bind scrutinee) $ Let (NonRec v (Var bind)) x
        go env letExpr args
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
        convertType env (varType bind) >>= \case
          TText -> asLet
          TDecimal -> asLet
          TNumeric _ -> asLet
          TParty -> asLet
          TTimestamp -> asLet
          TDate -> asLet
          TContractId{} -> asLet
          TUpdate{} -> asLet
          TScenario{} -> asLet
          tcon -> do
              ctor@(Ctor _ fldNames fldTys) <- toCtor env con
              if not (isRecordCtor ctor)
                then convertLet env bind scrutinee $ \env -> do
                  bind' <- convertExpr env (Var bind)
                  ty <- convertType env $ varType bind
                  alt' <- convertAlt env ty alt
                  pure $ ECase bind' [alt']
                else case zipExactMay vs (zipExact fldNames fldTys) of
                    Nothing -> unsupported "Pattern match with existential type" alt
                    Just vsFlds -> convertLet env bind scrutinee $ \env -> do
                        bindRef <- convertExpr env (Var bind)
                        x' <- convertExpr env x
                        projBinds <- mkProjBindings env bindRef (fromTCon tcon) vsFlds x'
                        pure projBinds
      where
        asLet = convertLet env bind scrutinee $ \env -> convertExpr env x
    go env (Case scrutinee bind typ []) args = fmap (, args) $ do
        -- GHC only generates empty case alternatives if it is sure the scrutinee will fail, LF doesn't support empty alternatives
        scrutinee' <- convertExpr env scrutinee
        typ' <- convertType env typ
        bind' <- convVarWithType env bind
        pure $
          ELet (Binding bind' scrutinee') $
          ECase (EVar $ convVar bind) [CaseAlternative CPDefault $ EBuiltin BEError `ETyApp` typ' `ETmApp` EBuiltin (BEText "Unreachable")]
    go env (Case scrutinee bind _ (defaultLast -> alts)) args = fmap (, args) $ do
        scrutinee' <- convertExpr env scrutinee
        bindTy <- convertType env $ varType bind
        alts' <- mapM (convertAlt env bindTy) alts
        bind' <- convVarWithType env bind
        if isDeadOcc (occInfo (idInfo bind))
          then pure $ ECase scrutinee' alts'
          else pure $
            ELet (Binding bind' scrutinee') $
            ECase (EVar $ convVar bind) alts'
    go env (Let (Rec xs) _) args = unsupported "Local variables defined recursively - recursion can only happen at the top level" $ map fst xs
    go env o@(Coercion _) args = unhandled "Coercion" o
    go _ x args = unhandled "Expression" x

convertArg :: Env -> GHC.Arg Var -> ConvertM LF.Arg
convertArg env = \case
    Type t -> TyArg <$> convertType env t
    e -> TmArg <$> convertExpr env e

withTyArg :: Env -> (LF.TypeVarName, LF.Kind) -> [LArg Var] -> (LF.Type -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])) -> ConvertM (LF.Expr, [LArg Var])
withTyArg env _ (LType t:args) cont = do
    t <- convertType env t
    cont t args
withTyArg env (v, k) args cont = do
    (x, args) <- cont (TVar v) args
    pure (ETyLam (v, k) x, args)

withTmArg :: Env -> (LF.ExprVarName, LF.Type) -> [LArg Var] -> (LF.Expr-> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])) -> ConvertM (LF.Expr, [LArg Var])
withTmArg env _ (LExpr x:args) cont = do
    x <- convertExpr env x
    cont x args
withTmArg env (v, t) args cont = do
    (x, args) <- cont (EVar v) args
    pure (ETmLam (v, t) x, args)

convertLet :: Env -> Var -> GHC.Expr Var -> (Env -> ConvertM LF.Expr) -> ConvertM LF.Expr
convertLet env binder bound mkBody = do
    bound <- convertExpr env bound
    case bound of
        EVar{} -> mkBody (envInsertAlias binder bound env)
        _ -> do
            binder <- convVarWithType env binder
            body <- mkBody env
            pure $ ELet (Binding binder bound) body

-- | Convert ghc package unit id's to LF package references.
convertUnitId :: GHC.UnitId -> MS.Map GHC.UnitId T.Text -> UnitId -> ConvertM LF.PackageRef
convertUnitId thisUnitId _pkgMap unitId | unitId == thisUnitId = pure LF.PRSelf
convertUnitId _thisUnitId pkgMap unitId = case unitId of
  IndefiniteUnitId x -> unsupported "Indefinite unit id's" x
  DefiniteUnitId _ -> case MS.lookup unitId pkgMap of
    Just hash -> pure $ LF.PRImport $ PackageId hash
    Nothing -> unknown unitId pkgMap

convertAlt :: Env -> LF.Type -> Alt Var -> ConvertM CaseAlternative
convertAlt env ty (DEFAULT, [], x) = CaseAlternative CPDefault <$> convertExpr env x
convertAlt env ty (DataAlt con, [], x)
    | getOccFS con == "True" = CaseAlternative (CPBool True) <$> convertExpr env x
    | getOccFS con == "False" = CaseAlternative (CPBool False) <$> convertExpr env x
    | getOccFS con == "[]" = CaseAlternative CPNil <$> convertExpr env x
    | getOccFS con == "()" = CaseAlternative CPUnit <$> convertExpr env x
    | isBuiltinName "None" con
    = CaseAlternative CPNone <$> convertExpr env x
convertAlt env ty (DataAlt con, [a,b], x)
    | getOccFS con == ":" = CaseAlternative (CPCons (convVar a) (convVar b)) <$> convertExpr env x
convertAlt env ty (DataAlt con, [a], x)
    | isBuiltinName "Some" con
    = CaseAlternative (CPSome (convVar a)) <$> convertExpr env x
convertAlt env (TConApp tcon targs) alt@(DataAlt con, vs, x) = do
    ctors <- toCtors env $ dataConTyCon con
    Ctor (mkVariantCon . getOccText -> variantName) fldNames fldTys <- toCtor env con
    let patVariant = variantName
    if
      | isEnumCtors ctors ->
        CaseAlternative (CPEnum patTypeCon patVariant) <$> convertExpr env x
      | null fldNames ->
        case zipExactMay vs fldTys of
          Nothing -> unsupported "Pattern match with existential type" alt
          Just [] ->
            let patBinder = vArg
            in  CaseAlternative CPVariant{..} <$> convertExpr env x
          Just [(v, _)] ->
            let patBinder = convVar v
            in  CaseAlternative CPVariant{..} <$> convertExpr env x
          Just (_:_:_) -> unsupported "Data constructor with multiple unnamed fields" alt
      | otherwise ->
        case zipExactMay vs (zipExact fldNames fldTys) of
          Nothing -> unsupported "Pattern match with existential type" alt
          Just vsFlds ->
            let patBinder = vArg
            in  do
              x' <- convertExpr env x
              projBinds <- mkProjBindings env (EVar vArg) (TypeConApp (synthesizeVariantRecord variantName <$> tcon) targs) vsFlds x'
              pure $ CaseAlternative CPVariant{..} projBinds
    where
        -- TODO(MH): We need to generate fresh names.
        vArg = mkVar "$arg"
        patTypeCon = tcon
convertAlt _ _ x = unsupported "Case alternative of this form" x

mkProjBindings :: Env -> LF.Expr -> TypeConApp -> [(Var, (FieldName, LF.Type))] -> LF.Expr -> ConvertM LF.Expr
mkProjBindings env recExpr recTyp vsFlds e =
  fmap (\bindings -> mkELets bindings e) $ sequence
    [ Binding <$> convVarWithType env v <*> pure (ERecProj recTyp fld recExpr)
    | (v, (fld, _typ)) <- vsFlds
    , not (isDeadOcc (occInfo (idInfo v)))
    ]

-- Convert a coercion @S ~ T@ to a pair of lambdas
-- @(to :: S -> T, from :: T -> S)@ in higher-order abstract syntax style.
--
-- The definition
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
convertCoercion :: Env -> Coercion -> ConvertM (LF.Expr -> LF.Expr, LF.Expr -> LF.Expr)
convertCoercion env co = evalStateT (go env co) 0
  where
    go :: Env -> Coercion -> StateT Int ConvertM (LF.Expr -> LF.Expr, LF.Expr -> LF.Expr)
    go env co@(coercionKind -> Pair s t)
        | isReflCo co = pure (id, id)
        | Just (aCo, bCo) <- splitFunCo_maybe co = do
            let Pair a a' = coercionKind aCo
            (aTo, aFrom) <- go env aCo
            (bTo, bFrom) <- go env bCo
            a <- lift $ convertType env a
            a' <- lift $ convertType env a'
            x <- mkLamBinder
            let to expr = ETmLam (x, a') $ bTo $ ETmApp expr $ aFrom $ EVar x
            let from expr = ETmLam (x, a) $ bFrom $ ETmApp expr $ aTo $ EVar x
            pure (to, from)
        -- NOTE(MH): This case is commented out because we don't know how to trigger
        -- it in a test case yet. In theory it should do the right thing though.
        -- | Just (a, k_co, co') <- splitForAllCo_maybe co
        -- , isReflCo k_co
        -- = do
        --     (a, k) <- lift $ convTypeVar a
        --     (to', from') <- go env co'
        --     let to expr = ETyLam (a, k) $ to' $ ETyApp expr $ TVar a
        --     let from expr = ETyLam (a, k) $ from' $ ETyApp expr $ TVar a
        --     pure (to, from)
        -- Case (1) & (2)
        | Just (tCon, ts, field, flv) <- isSatNewTyCon s t = newtypeCoercion tCon ts field flv
        | Just (tCon, ts, field, flv) <- isSatNewTyCon t s = swap <$> newtypeCoercion tCon ts field flv
        | SymCo co' <- co = swap <$> go env co'
        | SubCo co' <- co = go env co'
        | otherwise = lift $ unhandled "Coercion" co

    newtypeCoercion tCon ts field flv = do
        ts' <- lift $ mapM (convertType env) ts
        t' <- lift $ convertQualified env tCon
        let tcon = TypeConApp t' ts'
        let sanitizeTo x
                -- NOTE(MH): This is DICTIONARY SANITIZATION step (3).
                | flv == ClassFlavour = ETmLam (mkVar "_", TUnit) x
                | otherwise = x
            sanitizeFrom x
                -- NOTE(MH): This is DICTIONARY SANITIZATION step (2).
                | flv == ClassFlavour = x `ETmApp` EUnit
                | otherwise = x
        let to expr = ERecCon tcon [(field, sanitizeTo expr)]
        let from expr = sanitizeFrom $ ERecProj tcon field expr
        pure (to, from)

    mkLamBinder = do
        n <- state (dupe . succ)
        pure $ mkVar $ "$cast" <> T.pack (show n)

    isSatNewTyCon :: GHC.Type -> GHC.Type -> Maybe (TyCon, [TyCoRep.Type], FieldName, TyConFlavour)
    isSatNewTyCon s (TypeCon t ts)
        | Just data_con <- newTyConDataCon_maybe t
        , (tvs, rhs) <- newTyConRhs t
        , length ts == length tvs
        , applyTysX tvs rhs ts `eqType` s
        , [field] <- ctorLabels data_con = Just (t, ts, field, flv)
      where
        flv = tyConFlavour t
    isSatNewTyCon _ _ = Nothing

convertModuleName :: GHC.ModuleName -> LF.ModuleName
convertModuleName =
    ModuleName . T.split (== '.') . fsToText . moduleNameFS

qualify :: Env -> GHC.Module -> a -> ConvertM (Qualified a)
qualify env m x = do
    unitId <- convertUnitId (envModuleUnitId env) (envPkgMap env) $ GHC.moduleUnitId m
    pure $ Qualified unitId (convertModuleName $ GHC.moduleName m) x

qDA_Types :: Env -> a -> ConvertM (Qualified a)
qDA_Types env a = do
  pkgRef <- packageNameToPkgRef env "daml-prim"
  pure $ Qualified pkgRef (mkModName ["DA", "Types"]) a

convertQualified :: NamedThing a => Env -> a -> ConvertM (Qualified TypeConName)
convertQualified env x = do
  pkgRef <- nameToPkgRef env x
  pure $ Qualified
    pkgRef
    (convertModuleName $ GHC.moduleName $ nameModule $ getName x)
    (mkTypeCon [getOccText x])

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
    | isTupleTyCon t, arity >= 2 = TCon <$> qDA_Types env (mkTypeCon ["Tuple" <> T.pack (show arity)])
    | t == listTyCon = pure (TBuiltin BTList)
    | t == boolTyCon = pure TBool
    | t == intTyCon || t == intPrimTyCon = pure TInt64
    | t == charTyCon = unsupported "Type GHC.Types.Char" t
    | t == liftedRepDataConTyCon = pure TUnit
    | t == typeSymbolKindCon = pure TUnit
    | Just m <- nameModule_maybe (getName t), m == gHC_TYPES =
        case getOccText t of
            "Text" -> pure TText
            "Decimal" ->
                if envLfVersion env `supports` featureNumeric
                    then pure (TNumeric (TNat 10))
                    else pure TDecimal
            _ -> defaultTyCon
    -- TODO(DEL-6953): We need to add a condition on the package name as well.
    | Just m <- nameModule_maybe (getName t), GHC.moduleName m == mkModuleName "DA.Internal.LF" =
        case getOccText t of
            "Scenario" -> pure (TBuiltin BTScenario)
            "ContractId" -> pure (TBuiltin BTContractId)
            "Update" -> pure (TBuiltin BTUpdate)
            "Party" -> pure TParty
            "Date" -> pure TDate
            "Time" -> pure TTimestamp
            "TextMap" -> pure (TBuiltin BTMap)
            _ -> defaultTyCon
    | isBuiltinName "Optional" t = pure (TBuiltin BTOptional)
    | otherwise = defaultTyCon
    where
        arity = tyConArity t
        defaultTyCon = TCon <$> convertQualified env t

metadataTys :: UniqSet FastString
metadataTys = mkUniqSet ["MetaData", "MetaCons", "MetaSel"]

convertType :: Env -> GHC.Type -> ConvertM LF.Type
convertType env o@(TypeCon t ts)
    | t == listTyCon, ts `eqTypes` [charTy] = pure TText
    -- TODO (drsk) we need to check that 'MetaData', 'MetaCons', 'MetaSel' are coming from the
    -- module GHC.Generics.
    | getOccFS t `elementOfUniqSet` metadataTys, [_] <- ts = pure TUnit
    | t == anyTyCon, [_] <- ts = pure TUnit -- used for type-zonking
    | t == funTyCon, _:_:ts' <- ts =
        foldl TApp TArrow <$> mapM (convertType env) ts'
    | Just m <- nameModule_maybe (getName t)
    , GHC.moduleName m == mkModuleName "DA.Internal.LF"
    , getOccText t == "Pair"
    , [StrLitTy f1, StrLitTy f2, t1, t2] <- ts = do
        t1 <- convertType env t1
        t2 <- convertType env t2
        pure $ TTuple [(mkField f1, t1), (mkField f2, t2)]
    | tyConFlavour t == TypeSynonymFlavour = convertType env $ expandTypeSynonyms o
    | otherwise = mkTApps <$> convertTyCon env t <*> mapM (convertType env) ts
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
    -- TODO (drsk): We want to check that the 'Meta' constructor really comes from GHC.Generics.
    | getOccFS t == "Meta", null ts = pure KStar
    | t == funTyCon, [_,_,t1,t2] <- ts = KArrow <$> convertKind t1 <*> convertKind t2
convertKind (TyVarTy x) = convertKind $ tyVarKind x
convertKind x = unhandled "Kind" x

convNameLoc :: NamedThing a => a -> Maybe LF.SourceLoc
convNameLoc n = case nameSrcSpan (getName n) of
  -- NOTE(MH): Locations are 1-based in GHC and 0-based in DAML-LF.
  -- Hence all the @- 1@s below.
  RealSrcSpan srcSpan -> Just (convRealSrcSpan srcSpan)
  UnhelpfulSpan{} -> Nothing

convRealSrcSpan :: RealSrcSpan -> LF.SourceLoc
convRealSrcSpan srcSpan = SourceLoc
    { slocModuleRef = Nothing
    , slocStartLine = srcSpanStartLine srcSpan - 1
    , slocStartCol = srcSpanStartCol srcSpan - 1
    , slocEndLine = srcSpanEndLine srcSpan - 1
    , slocEndCol = srcSpanEndCol srcSpan - 1
    }

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

data Ctors = Ctors
    { _cTypeName :: Name
    , _cFlavour :: TyConFlavour
    , _cParams :: [(TypeVarName, LF.Kind)]
    , _cCtors :: [Ctor]
    }
data Ctor = Ctor Name [FieldName] [LF.Type]

toCtors :: Env -> GHC.TyCon -> ConvertM Ctors
toCtors env t = Ctors (getName t) (tyConFlavour t) <$> mapM convTypeVar (tyConTyVars t) <*> cs
    where
        cs = case algTyConRhs t of
                DataTyCon cs' _ _ -> mapM (toCtor env) cs'
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
--
-- * We add a field for every constraint containt in the thetas.
ctorLabels :: DataCon -> [FieldName]
ctorLabels con =
    [mkField $ "$dict" <> T.pack (show i) | i <- [1 .. length thetas]] ++ conFields
  where
  thetas = dataConTheta con
  conFields
    | flv `elem` [ClassFlavour, TupleFlavour Boxed] || isTupleDataCon con
      -- NOTE(MH): The line below is a workaround for ghc issue
      -- https://github.com/ghc/ghc/blob/ae4f1033cfe131fca9416e2993bda081e1f8c152/compiler/types/TyCon.hs#L2030
      -- If we omit this workaround, `GHC.Tuple.Unit` gets translated into a
      -- variant rather than a record and the `SugarUnit` test will fail.
      || (getOccFS con == "Unit" && nameModule (getName con) == gHC_TUPLE)
    = map (mkField . T.cons '_' . T.pack . show) [1..dataConSourceArity con]
    | flv == NewtypeFlavour && null lbls
    = [mkField "unpack"]
    | otherwise
    = map convFieldName lbls
  flv = tyConFlavour (dataConTyCon con)
  lbls = dataConFieldLabels con

toCtor :: Env -> DataCon -> ConvertM Ctor
toCtor env con =
  let (_, thetas, tys,_) = dataConSig con
      flv = tyConFlavour (dataConTyCon con)
      sanitize ty
        -- NOTE(MH): This is DICTIONARY SANITIZATION step (1).
        | flv == ClassFlavour = TUnit :-> ty
        | otherwise = ty
  in Ctor (getName con) (ctorLabels con) <$> mapM (fmap sanitize . convertType env) (thetas ++ tys)

isRecordCtor :: Ctor -> Bool
isRecordCtor (Ctor _ fldNames fldTys) = not (null fldNames) || null fldTys

isEnumCtors :: Ctors -> Bool
isEnumCtors (Ctors _ _ params ctors) = null params && all (\(Ctor _ _ tys) -> null tys) ctors

---------------------------------------------------------------------
-- SIMPLE WRAPPERS

convFieldName :: FieldLbl a -> FieldName
convFieldName = mkField . fsToText . flLabel

convTypeVar :: Var -> ConvertM (TypeVarName, LF.Kind)
convTypeVar t = do
    k <- convertKind $ tyVarKind t
    pure (mkTypeVar $ T.pack $ show (varUnique t), k)

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
        pkgRef <- packageNameToPkgRef env damlStdlib
        pure $ EVal (Qualified pkgRef (mkModName ["DA", "Internal", "Prelude"]) (mkVal "pure"))
          `ETyApp` monad'
          `ETmApp` dict'
          `ETyApp` t
          `ETmApp` x
