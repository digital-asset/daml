-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE PatternSynonyms #-}
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
import           DA.Daml.LF.Ast.Type as LF
import           DA.Daml.LF.Ast.Numeric
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
import           "ghc-lib" GhcPlugins as GHC hiding ((<>), notNull)
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
      }

unsupported :: (HasCallStack, Outputable a) => String -> a -> ConvertM e
unsupported typ x = conversionError errMsg
    where
         errMsg =
             "Failure to process DAML program, this feature is not currently supported.\n" ++
             typ ++ ".\n" ++
             prettyPrint x

unknown :: HasCallStack => GHC.UnitId -> MS.Map GHC.UnitId DalfPackage -> ConvertM e
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
    ,envPkgMap :: MS.Map GHC.UnitId LF.DalfPackage
    ,envLfVersion :: LF.Version
    ,envTypeSynonyms :: [(GHC.Type, TyCon)]
    ,envInstances :: [(TyCon, [GHC.Type])]
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

convertInt64 :: Integer -> ConvertM LF.Expr
convertInt64 x
    | toInteger (minBound :: Int64) <= x && x <= toInteger (maxBound :: Int64) =
        pure $ EBuiltin $ BEInt64 (fromInteger x)
    | otherwise =
        unsupported "Int literal out of bounds" (negate x)

-- | Convert a rational number into a (legacy) Decimal literal.
convertRationalDecimal :: Env -> Integer -> Integer -> ConvertM LF.Expr
convertRationalDecimal env num denom
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

-- | Convert a rational number into a fixed scale Numeric literal. We check
-- that the scale is in bound, and the number can be represented without
-- overflow or loss of precision.
convertRationalNumericMono :: Env -> Integer -> Integer -> Integer -> ConvertM LF.Expr
convertRationalNumericMono env scale num denom
    | scale < 0 || scale > fromIntegral numericMaxScale =
        unsupported
            ("Tried to construct value of type Numeric " ++ show scale ++ ", but scale is out of bounds. Scale must be between 0 through 37, not " ++ show scale)
            scale

    | abs (rational * 10 ^ scale) >= 10 ^ numericMaxPrecision =
        unsupported
            ("Rational is out of bounds: " ++ show double ++ ". The Numeric " ++ show scale ++ " type can only represent numbers greater than -10^" ++ show maxPower ++ " and smaller than 10^" ++ show maxPower)
            (num, denom)

    | (num * 10^scale) `mod` denom /= 0 =
        unsupported
            ("Rational is out of bounds: " ++ show double ++ ". It cannot be represented without loss of precision. Maximum precision for the Numeric " ++ show scale ++ " type is 10^-" ++ show scale)
            (num, denom)

    | otherwise =
        pure $ EBuiltin $ BENumeric $
            numeric (fromIntegral scale)
                    ((num * 10^scale) `div` denom)

    where
        rational = num % denom
        double = (fromInteger num / fromInteger denom) :: Double
        maxPower = fromIntegral numericMaxPrecision - scale

convertModule
    :: LF.Version
    -> MS.Map UnitId DalfPackage
    -> NormalizedFilePath
    -> CoreModule
    -> Either FileDiagnostic LF.Module
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
        typeSynonyms =
          [ (wrappedT, t)
          | ATyCon t <- eltsUFM (cm_types x)
          , Just ([], wrappedT) <- [synTyConDefn_maybe t]
          ]
        instances =
            [ (c, ts)
            | (name, _) <- binds
            , DFunId _ <- [idDetails name]
            , TypeCon c ts <- [varType name]
            ]
        env = Env
          { envLFModuleName = lfModName
          , envGHCModuleName = ghcModName
          , envModuleUnitId = thisUnitId
          , envAliases = MS.empty
          , envPkgMap = pkgMap
          , envLfVersion = lfVersion
          , envTypeSynonyms = typeSynonyms
          , envInstances = instances
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
    , (superClassDicts, args) <- span isSuperClassDict args
    , Just (signatories : observers : ensure : agreement : create : _fetch : archive : _toAnyTemplate : _fromAnyTemplate : _templateTypeRep : keyAndChoices) <- mapM isVar_maybe args
    , Just (polyType@(TypeCon polyTyCon _), _) <- splitFunTy_maybe (varType create)
    , Just monoTyCon <- findMonoTyp polyType
    = do
        let tplLocation = convNameLoc monoTyCon
            fields = ctorLabels (tyConSingleDataCon polyTyCon)
        polyType@(TConApp polyTyCon polyTyArgs) <- convertType env polyType
        let polyTCA = TypeConApp polyTyCon polyTyArgs
        monoType@(TCon monoTyCon) <- convertTyCon env monoTyCon
        let monoTCA = TypeConApp monoTyCon []
        let coerceRec fromType toType fromExpr =
                ELet (Binding (rec, typeConAppToType fromType) fromExpr) $
                ERecCon toType $ map (\field -> (field, ERecProj fromType field (EVar rec))) fields
        let (unwrapTpl, wrapTpl, unwrapCid, wrapCid)
                | null polyTyArgs = (id, id, id, id)
                | otherwise =
                    ( coerceRec monoTCA polyTCA
                    , coerceRec polyTCA monoTCA
                    , ETmApp $ mkETyApps (EBuiltin BECoerceContractId) [monoType, polyType]
                    , ETmApp $ mkETyApps (EBuiltin BECoerceContractId) [polyType, monoType]
                    )
        stdlibRef <- packageNameToPkgRef env damlStdlib
        let tplTypeCon = qualObject monoTyCon
        let tplParam = this
        let applyThis e = ETmApp e $ unwrapTpl $ EVar this
        tplSignatories <- applyThis <$> convertExpr env (Var signatories)
        tplObservers <- applyThis <$> convertExpr env (Var observers)
        tplPrecondition <- applyThis <$> convertExpr env (Var ensure)
        tplAgreement <- applyThis <$> convertExpr env (Var agreement)
        archive <- convertExpr env (Var archive)
        (tplKey, key, choices) <- case keyAndChoices of
                hasKey : key : maintainers : _fetchByKey : _lookupByKey : _toAnyContractKey : _fromAnyContractKey : choices
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
                        let toAnyContractKey =
                                if envLfVersion env `supports` featureAnyType
                                  then ETyLam
                                         (mkTypeVar "proxy", KArrow KStar KStar)
                                         (ETmLam
                                            (mkVar "_", TApp (TVar $ mkTypeVar "proxy") polyType)
                                            (ETmLam (mkVar "key", keyType) $ EToAny keyType $ EVar $ mkVar "key"))
                                  else EBuiltin BEError `ETyApp`
                                       TForall (mkTypeVar "proxy", KArrow KStar KStar) (TApp (TVar $ mkTypeVar "proxy") polyType :-> keyType :-> TUnit) `ETmApp`
                                       EBuiltin (BEText "toAnyContractKey is not supported in this DAML-LF version")
                        let fromAnyContractKey =
                                if envLfVersion env `supports` featureAnyType
                                  then ETyLam
                                         (mkTypeVar "proxy", KArrow KStar KStar)
                                         (ETmLam
                                            (mkVar "_", TApp (TVar $ mkTypeVar "proxy") polyType)
                                            (ETmLam (mkVar "any", TAny) $ EFromAny keyType $ EVar $ mkVar "any"))
                                  else EBuiltin BEError `ETyApp`
                                       TForall (mkTypeVar "proxy", KArrow KStar KStar) (TApp (TVar $ mkTypeVar "proxy") polyType :-> TUnit :-> TOptional keyType) `ETmApp`
                                       EBuiltin (BEText "fromAnyContractKey is not supported in this DAML-LF version")
                        pure (Just $ TemplateKey keyType (applyThis key) (ETmApp maintainers hasKey), [hasKey, key, maintainers, fetchByKey, lookupByKey, toAnyContractKey, fromAnyContractKey], choices)
                choices -> pure (Nothing, [], choices)
        let convertGenericChoice :: [Var] -> ConvertM (TemplateChoice, [LF.Expr])
            convertGenericChoice [consumption, controllers, action, _exercise, _toAnyChoice, _fromAnyChoice] = do
                (argType, argTCon, resType) <- convertType env (varType action) >>= \case
                    TContractId _ :-> _ :-> argType@(TConApp argTCon _) :-> TUpdate resType -> pure (argType, argTCon, resType)
                    t -> unhandled "Choice action type" (varType action)
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
                let toAnyChoice =
                        if envLfVersion env `supports` featureAnyType
                          then ETyLam
                                 (mkTypeVar "proxy", KArrow KStar KStar)
                                 (ETmLam
                                    (mkVar "_", TApp (TVar $ mkTypeVar "proxy") polyType)
                                    (ETmLam chcArgBinder $ EToAny argType $ EVar arg))
                          else EBuiltin BEError `ETyApp`
                               TForall (mkTypeVar "proxy", KArrow KStar KStar) (TApp (TVar $ mkTypeVar "proxy") polyType :-> argType :-> TUnit) `ETmApp`
                               EBuiltin (BEText "toAnyChoice is not supported in this DAML-LF version")
                let fromAnyChoice =
                        if envLfVersion env `supports` featureAnyType
                          then ETyLam
                                 (mkTypeVar "proxy", KArrow KStar KStar)
                                 (ETmLam
                                    (mkVar "_", TApp (TVar $ mkTypeVar "proxy") polyType)
                                    (ETmLam (mkVar "any", TAny) $ EFromAny argType $ EVar $ mkVar "any"))
                          else EBuiltin BEError `ETyApp`
                               TForall (mkTypeVar "proxy", KArrow KStar KStar) (TApp (TVar $ mkTypeVar "proxy") polyType :-> TUnit :-> TOptional argType) `ETmApp`
                               EBuiltin (BEText "fromAnyChoice is not supported in this DAML-LF version")
                pure (TemplateChoice{..}, [consumption, controllers, action, exercise, toAnyChoice, fromAnyChoice])
            convertGenericChoice es = unhandled "generic choice" es
        (tplChoices, choices) <- first NM.fromList . unzip <$> mapM convertGenericChoice (chunksOf 6 choices)
        superClassDicts <- mapM (convertExpr env) superClassDicts
        signatories <- convertExpr env (Var signatories)
        observers <- convertExpr env (Var observers)
        ensure <- convertExpr env (Var ensure)
        agreement <- convertExpr env (Var agreement)
        let create = ETmLam (this, polyType) $ EUpdate $ UBind (Binding (self, TContractId monoType) $ EUpdate $ UCreate monoTyCon $ wrapTpl $ EVar this) $ EUpdate $ UPure (TContractId polyType) $ unwrapCid $ EVar self
        let fetch = ETmLam (self, TContractId polyType) $ EUpdate $ UBind (Binding (this, monoType) $ EUpdate $ UFetch monoTyCon $ wrapCid $ EVar self) $ EUpdate $ UPure polyType $ unwrapTpl $ EVar this
        let anyTemplateTy = anyTemplateTyFromStdlib stdlibRef
        let anyTemplateField = mkField "getAnyTemplate"
        let toAnyTemplate =
                if envLfVersion env `supports` featureAnyType
                  then ETmLam (this, polyType) $ ERecCon anyTemplateTy [(anyTemplateField, EToAny (TCon monoTyCon) (wrapTpl $ EVar this))]
                  else EBuiltin BEError `ETyApp` (polyType :-> typeConAppToType anyTemplateTy) `ETmApp` EBuiltin (BEText "toAnyTemplate is not supported in this DAML-LF version")
        let fromAnyTemplate =
                if envLfVersion env `supports` featureAnyType
                    then ETmLam (anyTpl, typeConAppToType anyTemplateTy) $
                         ECase (EFromAny (TCon monoTyCon) (ERecProj anyTemplateTy anyTemplateField (EVar anyTpl)))
                             [ CaseAlternative CPNone $ ENone polyType
                             , CaseAlternative (CPSome self) $ ESome polyType $ unwrapTpl $ EVar self
                             ]
                    else EBuiltin BEError `ETyApp` (typeConAppToType anyTemplateTy :-> TOptional polyType) `ETmApp` EBuiltin (BEText "fromAnyTemplate is not supported in this DAML-LF version")
        let templateTypeRep =
                let proxyName = mkTypeVar "proxy"
                    argType = TVar proxyName `TApp` polyType
                    resType = TypeConApp (Qualified stdlibRef (mkModName ["DA", "Internal", "LF"]) (mkTypeCon ["TemplateTypeRep"])) []
                    resField = mkField "getTemplateTypeRep"
                in
                ETyLam (proxyName, KStar `KArrow` KStar) $!
                    if envLfVersion env `supports` featureTypeRep
                        then ETmLam (arg, argType) $ ERecCon resType [(resField, ETypeRep (TCon monoTyCon))]
                        else EBuiltin BEError `ETyApp` (argType :-> typeConAppToType resType) `ETmApp` EBuiltin (BEText "templateTypeRep is not supported in this DAML-LF version")
        tyArgs <- mapM (convertType env) tyArgs
        -- NOTE(MH): The additional lambda is DICTIONARY SANITIZATION step (3).
        let tmArgs = map (ETmLam (mkVar "_", TUnit)) $ superClassDicts ++ [signatories, observers, ensure, agreement, create, fetch, archive, toAnyTemplate, fromAnyTemplate, templateTypeRep] ++ key ++ concat choices
        qTCon <- qualify env m  $ mkTypeCon [getOccText $ dataConTyCon dictCon]
        let tcon = TypeConApp qTCon tyArgs
        let fldNames = ctorLabels dictCon
        let dict = ERecCon tcon (zip fldNames tmArgs)
        pure (Template{..}, dict)
  where
    isVar_maybe :: GHC.Expr Var -> Maybe Var
    isVar_maybe = \case
        Var v -> Just v
        _ -> Nothing
    isSuperClassDict :: GHC.Expr Var -> Bool
    -- NOTE(MH): We need the `$f` and `$d` cases since GHC inlines super class
    -- dictionaries without running the simplifier under some circumstances.
    isSuperClassDict (Var v) = any (`T.isPrefixOf` getOccText v) ["$cp", "$f", "$d"]
    -- For things like Numeric 10 we can end up with superclass dictionaries
    -- of the form `dict @10`.
    isSuperClassDict (App t _) = isSuperClassDict t
    isSuperClassDict _ = False
    findMonoTyp :: GHC.Type -> Maybe TyCon
    findMonoTyp t = case t of
        TypeCon tcon [] -> Just tcon
        t -> snd <$> find (eqType t . fst) (envTypeSynonyms env)
    this = mkVar "this"
    self = mkVar "self"
    arg = mkVar "arg"
    res = mkVar "res"
    rec = mkVar "rec"
convertGenericTemplate env x = unhandled "generic template" x

data Consuming = PreConsuming
               | NonConsuming
               | PostConsuming
               deriving (Eq)

convertTypeDef :: Env -> TyThing -> ConvertM [Definition]
convertTypeDef env o@(ATyCon t) = withRange (convNameLoc t) $ if
    -- Internal types (i.e. already defined in LF)
    | NameIn DA_Internal_LF n <- t
    , n `elementOfUniqSet` internalTypes
    -> pure []

    -- NOTE(MH): We detect type synonyms produced by the desugaring
    -- of `template instance` declarations and inline the record definition
    -- of the generic template.
    --
    -- TODO(FM): Precompute a map of possible template instances in Env
    -- instead of checking every closed type synonym against every class
    -- instance (or improve this some other way to subquadratic time).
    | Just ([], TypeCon tpl args) <- synTyConDefn_maybe t
    , any (\(c, args') -> getOccFS c == getOccFS tpl <> "Instance" && eqTypes args args') $ envInstances env
    -> convertTemplateInstanceDef env (getName t) tpl args

    -- Type synonyms get expanded out during conversion (see 'convertType').
    | isTypeSynonymTyCon t
    -> pure []

    -- Enum types. These are algebraic types without any type arguments,
    -- with two or more constructors that have no arguments.
    | isEnumTyCon t
    -> convertEnumDef env t

    -- Simple record types. This includes newtypes, typeclasses, and
    -- single constructor algebraic types with no fields or with
    -- labelled fields.
    | isSimpleRecordTyCon t
    -> convertSimpleRecordDef env t

    -- Variants are algebraic types that are not enums and not simple
    -- record types. This includes most 'data' types.
    | isVariantTyCon t
    -> convertVariantDef env t

    | otherwise
    -> unsupported ("Data definition, of type " ++ prettyPrint (tyConFlavour t)) o

convertTypeDef env x = pure []

convertEnumDef :: Env -> TyCon -> ConvertM [Definition]
convertEnumDef env t =
    pure [defDataType tconName [] $ DataEnum ctorNames]
  where
    tconName = mkTypeCon [getOccText t]
    ctorNames = map (mkVariantCon . getOccText) (tyConDataCons t)

convertSimpleRecordDef :: Env -> TyCon -> ConvertM [Definition]
convertSimpleRecordDef env tycon = do
    let con = tyConSingleDataCon tycon
        flavour = tyConFlavour tycon
        sanitize -- DICTIONARY SANITIZATION step (1)
            | flavour == ClassFlavour = (TUnit :->)
            | otherwise = id
    tyVars <- mapM convTypeVar (tyConTyVars tycon)
    fields <- convertRecordFields env con sanitize
    let tconName = mkTypeCon [getOccText tycon]
        typeDef = defDataType tconName tyVars (DataRecord fields)
        workerDef = defNewtypeWorker env tycon tconName con tyVars fields
    pure $ typeDef : [workerDef | flavour == NewtypeFlavour]

defNewtypeWorker :: NamedThing a => Env -> a -> TypeConName -> DataCon
    -> [(TypeVarName, LF.Kind)] -> [(FieldName, LF.Type)] -> Definition
defNewtypeWorker env loc tconName con tyVars fields =
    let tcon = TypeConApp
            (Qualified PRSelf (envLFModuleName env) tconName)
            (map (TVar . fst) tyVars)
        workerName = mkWorkerName (getOccText con)
        workerType = mkTForalls tyVars $ mkTFuns (map snd fields) $ typeConAppToType tcon
        workerBody = mkETyLams tyVars $ mkETmLams (map (first fieldToVar) fields) $
            ERecCon tcon [(label, EVar (fieldToVar label)) | (label,_) <- fields]
    in defValue loc (workerName, workerType) workerBody

convertRecordFields :: Env -> DataCon -> (LF.Type -> t) -> ConvertM [(FieldName, t)]
convertRecordFields env con wrap = do
    let labels = ctorLabels con
        (_, theta, args, _) = dataConSig con
    types <- mapM (convertType env) (theta ++ args)
    pure $ zipExact labels (map wrap types)

convertVariantDef :: Env -> TyCon -> ConvertM [Definition]
convertVariantDef env tycon = do
    tyVars <- mapM convTypeVar (tyConTyVars tycon)
    (constrs, moreDefs) <- mapAndUnzipM
        (convertVariantConDef env tycon tyVars)
        (tyConDataCons tycon)
    let tconName = mkTypeCon [getOccText tycon]
        typeDef = defDataType tconName tyVars (DataVariant constrs)
    pure $ [typeDef] ++ concat moreDefs

convertVariantConDef :: Env -> TyCon -> [(TypeVarName, LF.Kind)] -> DataCon -> ConvertM ((VariantConName, LF.Type), [Definition])
convertVariantConDef env tycon tyVars con =
    case (ctorLabels con, dataConOrigArgTys con) of
        ([], []) ->
            pure ((ctorName, TUnit), [])
        ([], [argTy]) -> do
            argTy' <- convertType env argTy
            pure ((ctorName, argTy'), [])
        ([], _:_:_) ->
            unsupported "Data constructor with multiple unnamed fields" (prettyPrint (getName tycon))
        (labels, args) -> do
            fields <- zipExact labels <$> mapM (convertType env) args
            let recName = synthesizeVariantRecord ctorName tconName
                recDef = defDataType recName tyVars (DataRecord fields)
                recType = TConApp
                    (Qualified PRSelf (envLFModuleName env) recName)
                    (map (TVar . fst) tyVars)
            pure ((ctorName, recType), [recDef])
    where
        tconName = mkTypeCon [getOccText tycon]
        ctorName = mkVariantCon (getOccText con)

-- | Instantiate and inline the generic template record definition
-- for a template instance.
convertTemplateInstanceDef :: Env -> Name -> TyCon -> [GHC.Type] -> ConvertM [Definition]
convertTemplateInstanceDef env tname templateTyCon args = do
    when (tyConFlavour templateTyCon /= DataTypeFlavour) $
        unhandled "template type with unexpected flavour"
            (prettyPrint $ tyConFlavour templateTyCon)
    lfArgs <- mapM (convertType env) args
    let templateCon = tyConSingleDataCon templateTyCon
        tyVarNames = map convTypeVarName (tyConTyVars templateTyCon)
        subst = MS.fromList (zipExact tyVarNames lfArgs)
    fields <- convertRecordFields env templateCon (LF.substitute subst)
    let tconName = mkTypeCon [getOccText tname]
        typeDef = defDataType tconName [] (DataRecord fields)
    pure [typeDef]

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
    | any (`T.isPrefixOf` getOccText name) [ "$" <> prefix <> "_" <> method | prefix <- ["dm", "c"], method <- ["create", "fetch", "exercise", "toAnyTemplate", "fromAnyTemplate", "_templateTypeRep", "fetchByKey", "lookupByKey", "toAnyChoice", "fromAnyChoice", "toAnyContractKey", "fromAnyContractKey"] ]
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
internalTypes = mkUniqSet ["Scenario","Update","ContractId","Time","Date","Party","Pair", "TextMap", "Any", "TypeRep"]

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
    go env (VarIn DA_Internal_Prelude "$") (LType _ : LType _ : LExpr x : y : args)
        = go env x (y : args)
    go env (VarIn DA_Internal_LF "unpackPair") (LType (StrLitTy f1) : LType (StrLitTy f2) : LType t1 : LType t2 : args)
        = fmap (, args) $ do
            t1 <- convertType env t1
            t2 <- convertType env t2
            let fields = [(mkField f1, t1), (mkField f2, t2)]
            tupleTyCon <- qDA_Types env $ mkTypeCon ["Tuple" <> T.pack (show $ length fields)]
            let tupleType = TypeConApp tupleTyCon (map snd fields)
            pure $ ETmLam (varV1, TTuple fields) $ ERecCon tupleType $ zipWithFrom mkFieldProj (1 :: Int) fields
        where
            mkFieldProj i (name, _typ) = (mkField ("_" <> T.pack (show i)), ETupleProj name (EVar varV1))
    go env (VarIn GHC_Types "primitive") (LType (isStrLitTy -> Just y) : LType t : args)
        = fmap (, args) $ convertPrim (envLfVersion env) (unpackFS y) <$> convertType env t
    go env (VarIn GHC_Types "external") (LType (isStrLitTy -> Just y) : LType t : args)
        = do
            stdlibRef <- packageNameToPkgRef env damlStdlib
            fmap (, args) $ convertExternal env stdlibRef (unpackFS y) <$> convertType env t
    go env (VarIs "getFieldPrim") (LType (isStrLitTy -> Just name) : LType record : LType _field : args) = do
        record' <- convertType env record
        withTmArg env (varV1, record') args $ \x args ->
            pure (ERecProj (fromTCon record') (mkField $ fsToText name) x, args)
    -- NOTE(MH): We only inline `getField` for record types. This is required
    -- for contract keys. Projections on sum-of-records types have to through
    -- the type class for `getField`.
    go env (VarIn DA_Internal_Record "getField") (LType (isStrLitTy -> Just name) : LType recordType@(TypeCon recordTyCon _) : LType _fieldType : _dict : LExpr record : args)
        | isSingleConType recordTyCon = fmap (, args) $ do
            recordType <- convertType env recordType
            record <- convertExpr env record
            pure $ ERecProj (fromTCon recordType) (mkField $ fsToText name) record
    go env (VarIn DA_Internal_Record "setFieldPrim") (LType (isStrLitTy -> Just name) : LType record : LType field : args) = do
        record' <- convertType env record
        field' <- convertType env field
        withTmArg env (varV1, field') args $ \x1 args ->
            withTmArg env (varV2, record') args $ \x2 args ->
                pure (ERecUpd (fromTCon record') (mkField $ fsToText name) x2 x1, args)
    go env (VarIn GHC_Real "fromRational") (LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRationalDecimal env top bot
    go env (VarIn GHC_Real "fromRational") (LType (isNumLitTy -> Just n) : _ : LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = fmap (, args) $ convertRationalNumericMono env n top bot
    go env (VarIn GHC_Real "fromRational") (LType scaleTyCoRep : _ : LExpr (VarIs ":%" `App` tyInteger `App` Lit (LitNumber _ top _) `App` Lit (LitNumber _ bot _)) : args)
        = unsupported "Polymorphic numeric literal. Specify a fixed scale by giving the type, e.g. (1.2345 : Numeric 10)" ()
    go env (VarIn GHC_Num "negate") (tyInt : LExpr (VarIs "$fAdditiveInt") : LExpr (untick -> VarIs "fromInteger" `App` Lit (LitNumber _ x _)) : args)
        = fmap (, args) $ convertInt64 (negate x)
    go env (VarIn GHC_Integer_Type "fromInteger") (LExpr (Lit (LitNumber _ x _)) : args)
        = fmap (, args) $ convertInt64 x
    go env (Lit (LitNumber LitNumInt x _)) args
        = fmap (, args) $ convertInt64 x
    go env (VarIn Data_String "fromString") (LExpr x : args) -- referenced by deriving Enum
        = fmap (, args) $ convertExpr env x
    go env (VarIn GHC_CString "fromString") (LExpr x : args)
        = fmap (, args) $ convertExpr env x
    go env (VarIn GHC_CString "unpackCString#") (LExpr (Lit (LitString x)) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEText $ unpackCString x
    go env (VarIn GHC_CString "unpackCStringUtf8#") (LExpr (Lit (LitString x)) : args)
        = fmap (, args) $ pure $ EBuiltin $ BEText $ unpackCStringUtf8 x
    go env x@(VarIn Control_Exception_Base _) (LType t1 : LType t2 : LExpr (untick -> Lit (LitString s)) : args)
        = fmap (, args) $ do
        x' <- convertExpr env x
        t1' <- convertType env t1
        t2' <- convertType env t2
        pure (x' `ETyApp` t1' `ETyApp` t2' `ETmApp` EBuiltin (BEText (unpackCStringUtf8 s)))

    -- conversion of bodies of $con2tag functions
    go env (VarIn GHC_Base "getTag") (LType (TypeCon t _) : LExpr x : args) = fmap (, args) $ do
        x' <- convertExpr env x
        t' <- convertQualified env t
        let mkCasePattern con
                -- Note that tagToEnum# can also be used on non-enum types, i.e.,
                -- types where not all constructors are nullary.
                | isEnumTyCon t = CPEnum t' con
                | otherwise = CPVariant t' con (mkVar "_")
        pure $ ECase x'
            [ CaseAlternative
                (mkCasePattern (mkVariantCon (getOccText con)))
                (EBuiltin $ BEInt64 i)
            | (con, i) <- zip (tyConDataCons t) [0..]
            ]
    go env (VarIn GHC_Prim "tagToEnum#") (LType (TypeCon (Is "Bool") []) : LExpr (op0 `App` x `App` y) : args)
        | VarIn GHC_Prim "==#" <- op0 = go BEEqual
        | VarIn GHC_Prim "<#"  <- op0 = go BELess
        | VarIn GHC_Prim ">#"  <- op0 = go BEGreater
        where
          go op1 = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              pure (EBuiltin (op1 BTInt64) `ETmApp` x' `ETmApp` y')
    go env (VarIn GHC_Prim "tagToEnum#") (LType (TypeCon (Is "Bool") []) : LExpr x : args) = fmap (, args) $ do
        x' <- convertExpr env x
        pure $ EBuiltin (BEEqual BTInt64) `ETmApp` EBuiltin (BEInt64 1) `ETmApp` x'
    go env (VarIn GHC_Prim "tagToEnum#") (LType tt@(TypeCon t _) : LExpr x : args) = fmap (, args) $ do
        -- FIXME: Should generate a binary tree of eq and compare
        tt' <- convertType env tt
        x' <- convertExpr env x
        let cs = tyConDataCons t
            c1 = head cs -- FIXME: handle the empty variant more gracefully.
            mkCtor con
              -- Note that tagToEnum# can also be used on non-enum types, i.e.,
              -- types where not all constructors are nullary.
              | isEnumTyCon t
              = EEnumCon (tcaTypeCon (fromTCon tt')) (mkVariantCon (getOccText con))
              | otherwise
              = EVariantCon (fromTCon tt') (mkVariantCon (getOccText con)) EUnit
            mkEqInt i = EBuiltin (BEEqual BTInt64) `ETmApp` x' `ETmApp` EBuiltin (BEInt64 i)
        pure (foldr ($) (mkCtor c1) [mkIf (mkEqInt i) (mkCtor c) | (i,c) <- zipFrom 0 cs])

    -- built ins because they are lazy
    go env (VarIn GHC_Types "ifThenElse") (LType tRes : LExpr cond : LExpr true : LExpr false : args)
        = fmap (, args) $ mkIf <$> convertExpr env cond <*> convertExpr env true <*> convertExpr env false
    go env (VarIn GHC_Classes "||") (LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> pure ETrue <*> convertExpr env y
    go env (VarIn GHC_Classes "&&") (LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> pure EFalse
    go env (VarIn DA_Action "when") (LType monad : LExpr dict : LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> convertExpr env y <*> mkPure env monad dict TUnit EUnit
    go env (VarIn DA_Action "unless") (LType monad : LExpr dict : LExpr x : LExpr y : args)
        = fmap (, args) $ mkIf <$> convertExpr env x <*> mkPure env monad dict TUnit EUnit <*> convertExpr env y
    go env (VarIn DA_Internal_LF "submit") (LType typ : LExpr pty : LExpr upd : args) = fmap (, args) $ do
        pty' <- convertExpr env pty
        upd' <- convertExpr env upd
        typ' <- convertType env typ
        pure $
          EScenario (SCommit typ' pty' (EUpdate (UEmbedExpr typ' upd')))
    go env (VarIn DA_Internal_LF "submitMustFail") (LType typ : LExpr pty : LExpr upd : args) = fmap (, args) $ do
        pty' <- convertExpr env pty
        upd' <- convertExpr env upd
        typ' <- convertType env typ
        pure $ EScenario (SMustFailAt typ' pty' (EUpdate (UEmbedExpr typ' upd')))

    -- custom conversion because they correspond to builtins in DAML-LF, so can make the output more readable
    go env (VarIn DA_Internal_Prelude "pure") (LType monad : LExpr dict : LType t : LExpr x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env (VarIn DA_Internal_Prelude "return") (LType monad : LType t : LExpr dict : LExpr x : args)
        -- This is generating the special UPure/SPure nodes when the monad is Update/Scenario.
        -- In all other cases, 'return' is rewritten to 'pure'.
        = fmap (, args) $ join $ mkPure env monad dict <$> convertType env t <*> convertExpr env x
    go env bind@(VarIn DA_Internal_Prelude ">>=") allArgs@(LType monad : LExpr dict : LType _ : LType _ : LExpr x : LExpr lam@(Lam b y) : args) = do
        monad' <- convertType env monad
        case monad' of
          TBuiltin BTUpdate -> mkBind EUpdate UBind
          TBuiltin BTScenario -> mkBind EScenario SBind
          _ -> fmap (, allArgs) $ convertExpr env bind
        where
          mkBind :: (m -> LF.Expr) -> (Binding -> LF.Expr -> m) -> ConvertM (LF.Expr, [LArg Var])
          mkBind inj bind = fmap (, args) $ do
              x' <- convertExpr env x
              y' <- convertExpr env y
              b' <- convVarWithType env b
              pure (inj (bind (Binding b' x') y'))
    go env semi@(VarIn DA_Internal_Prelude ">>") allArgs@(LType monad : LType t : LType _ : LExpr _dict : LExpr x : LExpr y : args) = do
        monad' <- convertType env monad
        case monad' of
          TBuiltin BTUpdate -> mkSeq EUpdate UBind
          TBuiltin BTScenario -> mkSeq EScenario SBind
          _ -> fmap (, allArgs) $ convertExpr env semi
        where
          mkSeq :: (m -> LF.Expr) -> (Binding -> LF.Expr -> m) -> ConvertM (LF.Expr, [LArg Var])
          mkSeq inj bind = fmap (, args) $ do
              t' <- convertType env t
              x' <- convertExpr env x
              y' <- convertExpr env y
              pure $ inj (bind (Binding (mkVar "_", t') x') y')

    go env (VarIn GHC_Types "[]") (LType (TypeCon (Is "Char") []) : args)
        = fmap (, args) $ pure $ EBuiltin (BEText T.empty)
    go env (VarIn GHC_Types "[]") args
        = withTyArg env varT1 args $ \t args -> pure (ENil t, args)
    go env (VarIn GHC_Types ":") args =
        withTyArg env varT1 args $ \t args ->
        withTmArg env (varV1, t) args $ \x args ->
        withTmArg env (varV2, TList t) args $ \y args ->
          pure (ECons t x y, args)
    go env (VarIn DA_Internal_Prelude (IgnoreWorkerPrefixFS "None")) args
        = withTyArg env varT1 args $ \t args -> pure (ENone t, args)
    go env (VarIn DA_Internal_Prelude (IgnoreWorkerPrefixFS "Some")) args
        = withTyArg env varT1 args $ \t args ->
          withTmArg env (varV1, t) args $ \x args ->
            pure (ESome t x, args)

    go env (VarIn GHC_Tuple "()") args = fmap (, args) $ pure EUnit
    go env (VarIn GHC_Types "True") args = fmap (, args) $ pure $ mkBool True
    go env (VarIn GHC_Types "False") args = fmap (, args) $ pure $ mkBool False
    go env (VarIn GHC_Types "I#") args = fmap (, args) $ pure $ mkIdentity TInt64
        -- we pretend Int and Int# are the same thing

    go env (Var x) args
        | Just internals <- lookupUFM internalFunctions modName
        , getOccFS x `elementOfUniqSet` internals
        = unsupported "Direct call to internal function" x
        where
            modName = maybe (envGHCModuleName env) GHC.moduleName $ nameModule_maybe $ getName x

    go env (Var x) args
        | Just m <- nameModule_maybe $ varName x
        , Just con <- isDataConId_maybe x
        = convertDataCon env m con args
        | Just m <- nameModule_maybe $ varName x =
            fmap ((, args) . EVal) $ qualify env m $ convVal x
        | isGlobalId x = fmap (, args) $ do
            pkgRef <- nameToPkgRef env $ varName x
            pure $ EVal $ Qualified pkgRef (envLFModuleName env) $ convVal x
            -- some things are global, but not with a module name, so give them the current one
        | Just y <- envLookupAlias x env = fmap (, args) $ pure y
        | otherwise = fmap (, args) $ pure $ EVar $ convVar x

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
    go env (Case scrutinee bind t [(DataAlt (NameIn GHC_Types "I#"), [v], x)]) args = do
        -- We pretend Int and Int# are the same, so a case-expression becomes a let-expression.
        let letExpr = Let (NonRec bind scrutinee) $ Let (NonRec v (Var bind)) x
        go env letExpr args
    -- NOTE(MH): This is DICTIONARY SANITIZATION step (2).
    go env (Case scrutinee bind _ [(DataAlt con, vs, Var x)]) args
        | tyConFlavour (dataConTyCon con) == ClassFlavour
        = fmap (, args) $ do
            scrutinee' <- convertExpr env scrutinee
            let fldNames = ctorLabels con
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
            TAny{} -> asLet
            tcon | isSimpleRecordCon con -> do
                fields <- convertRecordFields env con id
                case zipExactMay vs fields of
                    Nothing -> unsupported "Pattern match with existential type" alt
                    Just vsFields -> convertLet env bind scrutinee $ \env -> do
                        bindRef <- convertExpr env (Var bind)
                        x' <- convertExpr env x
                        mkProjBindings env bindRef (fromTCon tcon) vsFields x'
            _ ->
                convertLet env bind scrutinee $ \env -> do
                    bind' <- convertExpr env (Var bind)
                    ty <- convertType env $ varType bind
                    alt' <- convertAlt env ty alt
                    pure $ ECase bind' [alt']

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

-- | Is this an enum type?
isEnumTyCon :: TyCon -> Bool
isEnumTyCon tycon =
    isEnumerationTyCon tycon
    && (tyConArity tycon == 0)
    && ((length (tyConDataCons tycon) >= 2)
       || hasDamlEnumCtx tycon)

-- | Is this a simple record type?
isSimpleRecordTyCon :: TyCon -> Bool
isSimpleRecordTyCon tycon =
    maybe False isSimpleRecordCon (tyConSingleDataCon_maybe tycon)

-- | Is this a variant type?
isVariantTyCon :: TyCon -> Bool
isVariantTyCon tycon =
    (tyConFlavour tycon == DataTypeFlavour)
    && not (isEnumTyCon tycon)
    && not (isSimpleRecordTyCon tycon)

conIsSingle :: DataCon -> Bool
conIsSingle = isSingleConType . dataConTyCon

conHasNoArgs :: DataCon -> Bool
conHasNoArgs = null . dataConOrigArgTys

conHasLabels :: DataCon -> Bool
conHasLabels = notNull . ctorLabels

isEnumCon :: DataCon -> Bool
isEnumCon = isEnumTyCon . dataConTyCon

isSimpleRecordCon :: DataCon -> Bool
isSimpleRecordCon con =
    (conHasLabels con || conHasNoArgs con)
    && conIsSingle con
    && not (isEnumCon con)

isVariantRecordCon :: DataCon -> Bool
isVariantRecordCon con = conHasLabels con && not (conIsSingle con)

-- | The different classes of data cons with respect to LF conversion.
data DataConClass
    = EnumCon -- ^ constructor for an enum type
    | SimpleRecordCon -- ^ constructor for a record type
    | SimpleVariantCon -- ^ constructor for a variant type with no synthetic record type
    | VariantRecordCon -- ^ constructor for a variant type with a synthetic record type
    deriving (Eq, Show)

classifyDataCon :: DataCon -> DataConClass
classifyDataCon con
    | isEnumCon con = EnumCon
    | isSimpleRecordCon con = SimpleRecordCon
    | isVariantRecordCon con = VariantRecordCon
    | otherwise = SimpleVariantCon
        -- in which case, daml-preprocessor ensures that the
        -- constructor cannot have more than one argument

-- | Split args into type args and non-type args of the expected length
-- for a particular DataCon.
splitConArgs_maybe :: DataCon -> [LArg Var] -> Maybe ([GHC.Type], [GHC.Arg Var])
splitConArgs_maybe con args = do
    let (conTypes, conTheta, conArgs, _) = dataConSig con
        numTypes = length conTypes
        numVals = length conTheta + length conArgs
        (typeArgs, valArgs) = splitAt numTypes (map snd args)
    guard (length typeArgs == numTypes)
    guard (length valArgs == numVals)
    typeArgs <- mapM isType_maybe typeArgs
    guard (all (isNothing . isType_maybe) valArgs)
    Just (typeArgs, valArgs)

-- NOTE(MH): Handle data constructors. Fully applied record
-- constructors are inlined. This is required for contract keys to
-- work. Constructor workers are not handled (yet).
convertDataCon :: Env -> GHC.Module -> DataCon -> [LArg Var] -> ConvertM (LF.Expr, [LArg Var])
convertDataCon env m con args
    -- Fully applied
    | Just (tyArgs, tmArgs) <- splitConArgs_maybe con args = do
        tyArgs <- mapM (convertType env) tyArgs
        tmArgs <- mapM (convertExpr env) tmArgs
        let tycon = dataConTyCon con
        qTCon <- qual (\x -> mkTypeCon [x]) (getOccText tycon)
        let tcon = TypeConApp qTCon tyArgs
            ctorName = mkVariantCon (getOccText con)
            fldNames = ctorLabels con
            xargs = (dataConName con, args)

        fmap (, []) $ case classifyDataCon con of
            EnumCon -> do
                unless (null args) $ unhandled "enum constructor with arguments" xargs
                pure $ EEnumCon qTCon ctorName

            SimpleVariantCon ->
                fmap (EVariantCon tcon ctorName) $ case tmArgs of
                    [] -> pure EUnit
                    [tmArg] -> pure tmArg
                    _ -> unhandled "constructor with more than two unnamed arguments" xargs

            SimpleRecordCon ->
                pure $ ERecCon tcon (zipExact fldNames tmArgs)

            VariantRecordCon -> do
                let recTCon = fmap (synthesizeVariantRecord ctorName) qTCon
                pure $
                    EVariantCon tcon ctorName $
                    ERecCon (TypeConApp recTCon tyArgs) (zipExact fldNames tmArgs)

    -- Partially applied
    | otherwise = do
        fmap (\op -> (EVal op, args)) (qual mkWorkerName (getOccText con))

    where

        qual :: (T.Text -> n) -> T.Text -> ConvertM (Qualified n)
        qual f t
            | Just xs <- T.stripPrefix "(," t
            , T.dropWhile (== ',') xs == ")" = qDA_Types env $ f $ "Tuple" <> T.pack (show $ T.length xs + 1)
            | IgnoreWorkerPrefix t' <- t = qualify env m $ f t'

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
convertUnitId :: GHC.UnitId -> MS.Map GHC.UnitId DalfPackage -> UnitId -> ConvertM LF.PackageRef
convertUnitId thisUnitId _pkgMap unitId | unitId == thisUnitId = pure LF.PRSelf
convertUnitId _thisUnitId pkgMap unitId = case unitId of
  IndefiniteUnitId x -> unsupported "Indefinite unit id's" x
  DefiniteUnitId _ -> case MS.lookup unitId pkgMap of
    Just DalfPackage{..} -> pure $ LF.PRImport dalfPackageId
    Nothing -> unknown unitId pkgMap

convertAlt :: Env -> LF.Type -> Alt Var -> ConvertM CaseAlternative
convertAlt env ty (DEFAULT, [], x) = CaseAlternative CPDefault <$> convertExpr env x
convertAlt env ty (DataAlt con, [], x)
    | NameIn GHC_Types "True" <- con = CaseAlternative (CPBool True) <$> convertExpr env x
    | NameIn GHC_Types "False" <- con = CaseAlternative (CPBool False) <$> convertExpr env x
    | NameIn GHC_Types "[]" <- con = CaseAlternative CPNil <$> convertExpr env x
    | NameIn GHC_Tuple "()" <- con = CaseAlternative CPUnit <$> convertExpr env x
    | NameIn DA_Internal_Prelude "None" <- con
    = CaseAlternative CPNone <$> convertExpr env x
convertAlt env ty (DataAlt con, [a,b], x)
    | NameIn GHC_Types ":" <- con = CaseAlternative (CPCons (convVar a) (convVar b)) <$> convertExpr env x
convertAlt env ty (DataAlt con, [a], x)
    | NameIn DA_Internal_Prelude "Some" <- con
    = CaseAlternative (CPSome (convVar a)) <$> convertExpr env x

convertAlt env (TConApp tcon targs) alt@(DataAlt con, vs, x) = do
    let patTypeCon = tcon
        patVariant = mkVariantCon (getOccText con)
        vArg = mkVar "$arg"

    case classifyDataCon con of
        EnumCon ->
            CaseAlternative (CPEnum patTypeCon patVariant) <$> convertExpr env x

        SimpleVariantCon -> do
            when (length vs /= dataConRepArity con) $
                unsupported "Pattern match with existential type" alt
            when (length vs >= 2) $
                unsupported "Data constructor with multiple unnamed fields" alt

            let patBinder = maybe vArg convVar (listToMaybe vs)
            CaseAlternative CPVariant{..} <$> convertExpr env x

        SimpleRecordCon ->
            unhandled "unreachable case -- convertAlt with simple record constructor" ()

        VariantRecordCon -> do
            fields <- convertRecordFields env con id
            let patBinder = vArg
            case zipExactMay vs fields of
                Nothing -> unsupported "Pattern match with existential type" alt
                Just vsFlds -> do
                    x' <- convertExpr env x
                    projBinds <- mkProjBindings env (EVar vArg) (TypeConApp (synthesizeVariantRecord patVariant <$> tcon) targs) vsFlds x'
                    pure $ CaseAlternative CPVariant{..} projBinds

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
    | NameIn GHC_Types n <- t =
        case n of
            "Text" -> pure TText
            "Numeric" -> pure (TBuiltin BTNumeric)
            "Decimal" ->
                if envLfVersion env `supports` featureNumeric
                    then pure TNumeric10
                    else pure TDecimal
            _ -> defaultTyCon
    -- TODO(DEL-6953): We need to add a condition on the package name as well.
    | NameIn DA_Internal_LF n <- t =
        case n of
            "Scenario" -> pure (TBuiltin BTScenario)
            "ContractId" -> pure (TBuiltin BTContractId)
            "Update" -> pure (TBuiltin BTUpdate)
            "Party" -> pure TParty
            "Date" -> pure TDate
            "Time" -> pure TTimestamp
            "TextMap" -> pure (TBuiltin BTTextMap)
            "Any" ->
                -- We just translate this to TUnit when it is not supported.
                -- We can’t get rid of it completely since the template desugaring uses
                -- this and we do not want to make that dependent on the DAML-LF version.
                pure $ if envLfVersion env `supports` featureAnyType
                    then TAny
                    else TUnit
            "TypeRep" ->
                -- We just translate this to TUnit when it is not supported.
                -- We can’t get rid of it completely since the template desugaring uses
                -- this and we do not want to make that dependent on the DAML-LF version.
                pure $ if envLfVersion env `supports` featureTypeRep
                    then TTypeRep
                    else TUnit
            _ -> defaultTyCon
    | NameIn DA_Internal_Prelude "Optional" <- t = pure (TBuiltin BTOptional)
    | otherwise = defaultTyCon
    where
        arity = tyConArity t
        defaultTyCon = TCon <$> convertQualified env t

metadataTys :: UniqSet FastString
metadataTys = mkUniqSet ["MetaData", "MetaCons", "MetaSel"]

convertType :: Env -> GHC.Type -> ConvertM LF.Type
convertType env o@(TypeCon t ts)
    | t == listTyCon, ts `eqTypes` [charTy] = pure TText
    | NameIn DA_Generics n <- t, n `elementOfUniqSet` metadataTys, [_] <- ts = pure TUnit
    | t == anyTyCon, [_] <- ts = pure TUnit -- used for type-zonking
    | t == funTyCon, _:_:ts' <- ts =
        foldl TApp TArrow <$> mapM (convertType env) ts'
    | NameIn DA_Internal_LF "Pair" <- t
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
convertType env t | Just m <- isNumLitTy t
  = case typeLevelNatE m of
        Left TLNEOutOfBounds ->
            unsupported "type-level natural outside of supported range [0, 37]" m
        Right n ->
            pure (TNat n)
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
    | Just m <- nameModule_maybe (getName t)
    , GHC.moduleName m == mkModuleName "GHC.Types"
    , getOccFS t == "Nat", null ts = pure KNat
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

------------------------------------------------------------------------------
-- EXTERNAL PACKAGES

-- External instance methods
convertExternal :: Env -> LF.PackageRef -> String -> LF.Type -> LF.Expr
convertExternal env stdlibRef primId lfType
    | [pkgId, modStr, templName, method] <- splitOn ":" primId
    , Just LF.Template {..} <- lookup pkgId modStr templName =
        let pkgRef = PRImport $ PackageId $ T.pack pkgId
            mod = ModuleName $ map T.pack $ splitOn "." modStr
            qualify tconName =
                Qualified
                    { qualPackage = pkgRef
                    , qualModule = mod
                    , qualObject = tconName
                    }
            templateDataType = TCon . qualify
         in case method of
                "signatory" ->
                    ETmLam
                        (tplParam, templateDataType tplTypeCon)
                        tplSignatories
                "observer" ->
                    ETmLam (tplParam, templateDataType tplTypeCon) tplObservers
                "agreement" ->
                    ETmLam (tplParam, templateDataType tplTypeCon) tplAgreement
                "ensure" ->
                    ETmLam
                        (tplParam, templateDataType tplTypeCon)
                        tplPrecondition
                "create" ->
                    ETmLam
                        (tplParam, templateDataType tplTypeCon)
                        (EUpdate $ UCreate (qualify tplTypeCon) (EVar tplParam))
                "fetch" ->
                    let coid = mkVar "$coid"
                     in ETmLam
                            ( coid
                            , TApp
                                  (TBuiltin BTContractId)
                                  (templateDataType tplTypeCon))
                            (EUpdate $ UFetch (qualify tplTypeCon) (EVar coid))
                "archive" ->
                    let archiveChoice = ChoiceName "Archive"
                     in case NM.lookup archiveChoice tplChoices of
                            Nothing ->
                                EBuiltin BEError `ETyApp` lfType `ETmApp`
                                EBuiltin
                                    (BEText $
                                     "convertExternal: archive is not implemented in external package")
                            Just TemplateChoice {..} ->
                                case chcArgBinder of
                                    (_, LF.TCon tcon) ->
                                        let coid = mkVar "$coid"
                                            archiveChoiceArg =
                                                LF.ERecCon
                                                    { LF.recTypeCon =
                                                          LF.TypeConApp tcon []
                                                    , LF.recFields = []
                                                    }
                                         in ETmLam
                                                ( coid
                                                , TApp
                                                      (TBuiltin BTContractId)
                                                      (templateDataType
                                                           tplTypeCon))
                                                (EUpdate $
                                                 UExercise
                                                     (qualify tplTypeCon)
                                                     archiveChoice
                                                     (EVar coid)
                                                     Nothing
                                                     archiveChoiceArg)
                                    otherwise ->
                                        error
                                            "convertExternal: Archive choice exists but has the wrong type."
                "toAnyTemplate"
                    | envLfVersion env `supports` featureAnyType ->
                        ETmLam (tplParam, templateDataType tplTypeCon) $
                        ERecCon
                            anyTemplateTy
                            [ ( anyTemplateField
                              , EToAny
                                    (templateDataType tplTypeCon)
                                    (EVar tplParam))
                            ]
                "toAnyTemplate"
                    | otherwise ->
                        EBuiltin BEError `ETyApp` lfType `ETmApp`
                        EBuiltin
                            (BEText
                                 "toAnyTemplate is not supported in this DAML-LF version")
                "fromAnyTemplate"
                    | envLfVersion env `supports` featureAnyType ->
                        ETmLam (anyTpl, typeConAppToType anyTemplateTy) $
                        ECase
                            (EFromAny
                                 (templateDataType tplTypeCon)
                                 (ERecProj
                                      anyTemplateTy
                                      anyTemplateField
                                      (EVar anyTpl)))
                            [ CaseAlternative CPNone $
                              ENone $ templateDataType tplTypeCon
                            , CaseAlternative (CPSome tplParam) $
                              ESome (templateDataType tplTypeCon) $
                              EVar tplParam
                            ]
                    | otherwise ->
                        EBuiltin BEError `ETyApp` lfType `ETmApp`
                        EBuiltin
                            (BEText
                                 "fromAnyTemplate is not supported in this DAML-LF version")
                "_templateTypeRep"
                    | envLfVersion env `supports` featureTypeRep ->
                        let resType =
                                TypeConApp
                                    (Qualified
                                         stdlibRef
                                         (mkModName ["DA", "Internal", "LF"])
                                         (mkTypeCon ["TemplateTypeRep"]))
                                    []
                            resField = mkField "getTemplateTypeRep"
                         in ERecCon
                                resType
                                [ ( resField
                                  , ETypeRep $ templateDataType tplTypeCon)
                                ]
                    | otherwise ->
                        EBuiltin BEError `ETyApp` lfType `ETmApp`
                        EBuiltin
                            (BEText
                                 "templateTypeRep is not supported in this DAML-LF version")
                other -> error "convertExternal: Unknown external method"
    | [pkgId, modStr, templName, method] <- splitOn ":" primId
    , Nothing <- lookup pkgId modStr templName =
        error $ "convertExternal: external template not found " <> primId
    | [pkgId, modStr, templName, choiceName, method] <- splitOn ":" primId
    , Just LF.Template {tplTypeCon,tplChoices} <- lookup pkgId modStr templName
    , choice <- ChoiceName (T.pack choiceName)
    , Just TemplateChoice {chcSelfBinder,chcArgBinder} <- NM.lookup choice tplChoices = do
        let pkgRef = PRImport $ PackageId $ T.pack pkgId
        let mod = ModuleName $ map T.pack $ splitOn "." modStr
        let qualify tconName = Qualified { qualPackage = pkgRef , qualModule = mod , qualObject = tconName}
        let templateDataType = TCon . qualify
        let (choiceArg, _) = chcArgBinder
        case method of
          "exercise" -> do
            ETmLam (chcSelfBinder, TApp (TBuiltin BTContractId) (templateDataType tplTypeCon)) $
              ETmLam chcArgBinder $
                EUpdate $ UExercise (qualify tplTypeCon) choice (EVar chcSelfBinder) Nothing (EVar choiceArg)
          "_toAnyChoice" ->
            -- TODO: envLfVersion env `supports` featureAnyType
            EBuiltin BEError `ETyApp` lfType `ETmApp`
            EBuiltin (BEText "toAnyChoice is not supported in this DAML-LF version")
          "_fromAnyChoice" ->
            -- TODO: envLfVersion env `supports` featureAnyType
            EBuiltin BEError `ETyApp` lfType `ETmApp`
            EBuiltin (BEText "fromAnyChoice is not supported in this DAML-LF version")
          _ -> error $ "convertExternal: Unknown external Choice method, " <> method
    | [pkgId, modStr, templName, choiceName, method] <- splitOn ":" primId
    , Nothing <- lookup pkgId modStr templName =
        error $ "convertExternal: external choice not found " <> primId
    | otherwise =
        error $ "convertExternal: malformed external string" <> primId
  where
    anyTemplateTy = anyTemplateTyFromStdlib stdlibRef
    lookup pId modName temName = do
        mods <- MS.lookup pId pkgIdToModules
        mod <- NM.lookup (LF.ModuleName $ map T.pack $ splitOn "." modName) mods
        NM.lookup (LF.TypeConName [T.pack temName]) $ LF.moduleTemplates mod
    pkgIdToModules =
        MS.fromList
            [ (T.unpack $ LF.unPackageId dalfPackageId, LF.packageModules pkg)
            | (_uId, DalfPackage {..}) <- MS.toList $ envPkgMap env
            , let ExternalPackage _pid pkg = dalfPackagePkg
            ]

-----------------------------
-- AnyTemplate constant names

anyTemplateTyFromStdlib :: PackageRef -> TypeConApp
anyTemplateTyFromStdlib stdlibRef =
    TypeConApp
        (Qualified
             stdlibRef
             (mkModName ["DA", "Internal", "LF"])
             (mkTypeCon ["AnyTemplate"]))
        []

anyTemplateField :: FieldName
anyTemplateField = mkField "getAnyTemplate"

anyTpl :: ExprVarName
anyTpl = mkVar "anyTpl"

---------------------------------------------------------------------
-- SIMPLE WRAPPERS

convFieldName :: FieldLbl a -> FieldName
convFieldName = mkField . fsToText . flLabel

convTypeVar :: Var -> ConvertM (TypeVarName, LF.Kind)
convTypeVar t = do
    k <- convertKind $ tyVarKind t
    pure (convTypeVarName t, k)

convTypeVarName :: Var -> TypeVarName
convTypeVarName = mkTypeVar . T.pack . show . varUnique

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
