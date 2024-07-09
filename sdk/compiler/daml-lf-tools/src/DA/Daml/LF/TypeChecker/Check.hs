-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module contains the Daml-LF type checker.
--
-- Some notes:
--
-- * We only do type checking and kind checking, i.e., check that all type
--   constructors are used with the correct number of arguments. We do not check
--   that template and choice parameters are serializable nor do we check for
--   template coherence.
--
-- * Shadowing of type variables is forbidden in type abstractions. Handling it
--   is far from trivial. To see this, consider handle consider the following
--   example:
--
--   > f : ∀β. β → β → T
--   > Λα. λ(x:α). Λα. λ(y:α). f α x y
--
--   When we enter the scope of the inner α, we need to replace either all α's
--   in its body or all α's in the environment with a fresh variable name.
--
-- * Shadowing of term variables is /currently/ allowed. We might need to
--   reconsider this decision.
--
-- * FIXME(MH): The @actor@ parameter of a 'UFetch' is /not/ checked. This is a
--   temporary measure to circumvent some issues with the translation from the
--   Renamer AST.
module DA.Daml.LF.TypeChecker.Check
    ( checkModule
    , expandTypeSynonyms
    , expandSynApp
    , typeOf'
    ) where

import Data.Hashable
import           Control.Lens hiding (Context, MethodName, para)
import           Control.Monad.Extra
import           Data.Either.Combinators (whenLeft)
import           Data.Foldable
import           Data.Functor
import           Data.List.Extended
import Data.Generics.Uniplate.Data (para)
import qualified Data.Set as S
import qualified Data.HashSet as HS
import           Data.Maybe (listToMaybe)
import qualified Data.Map.Strict as Map
import qualified Data.NameMap as NM
import qualified Data.IntSet as IntSet
import qualified Data.Text as T
import           Safe.Exact (zipExactMay)

import           DA.Daml.LF.Ast
import           DA.Daml.LF.Ast.Optics (dataConsType)
import           DA.Daml.LF.Ast.Type
import           DA.Daml.LF.Ast.Alpha
import           DA.Daml.LF.Ast.Numeric
import qualified DA.Daml.LF.TemplateOrInterface as TemplateOrInterface
import           DA.Daml.LF.TypeChecker.Env
import           DA.Daml.LF.TypeChecker.Error


-- | Check that a list does /not/ contain duplicate elements.
checkUnique :: (MonadGamma m, Eq a, Hashable a) => (a -> Error) -> [a] -> m ()
checkUnique mkDuplicateError xs = void (foldlM step HS.empty xs)
  where
    step acc x
      | x `HS.member` acc = throwWithContext (mkDuplicateError x)
      | otherwise         = pure (HS.insert x acc)

-- | Check that a record type is well-formed, i.e., no field name is repeated
-- and all field types are well-formed.
checkRecordType :: MonadGamma m => [(FieldName, Type)] -> m ()
checkRecordType (unzip -> (names, types)) = do
  checkUnique EDuplicateField names
  traverse_ (`checkType` KStar) types

-- TODO(MH): Defer the instantiation of the type parameters to the call sites of
-- this function.
-- | Check that a type constructor application is well-formed, i.e., the number
-- of arguments matches the number of parameters and each argument is
-- well-formed itself. This function assumes that the data type referenced by
-- the 'TypeConApp' has already passed 'checkDefDataType'.
--
-- For convenience, we return the 'DataCons'tructors with the type parameters
-- instantiated to the type arguments of the application.
checkTypeConApp :: MonadGamma m => TypeConApp -> m DataCons
checkTypeConApp tapp@(TypeConApp tcon targs) = do
  -- NOTE(MH): Since we're assuming that the data type has already passed
  -- 'checkDefDataType', the elements of @tparams@ are mutually distinct and
  -- contain all type variables which are free in @dataCons@. Thus, it is safe
  -- to call 'substitute'.
  DefDataType _loc _tcon _serializable tparams dataCons <- inWorld (lookupDataType tcon)
  subst0 <- match _Just (ETypeConAppWrongArity tapp) (zipExactMay tparams targs)
  for_ subst0 $ \((_, kind), typ) -> checkType typ kind
  let subst1 = map (\((v, _kind), typ) -> (v, typ)) subst0
  pure (over dataConsType (substitute (Map.fromList subst1)) dataCons)

-- | Check that a type is well-formed, that is:
--
-- (1) Each free type variable is in the environment 'Gamma'.
--
-- (2) Each type constructor is known and applied to the right number of
--     arguments.
--
-- (3) 'BTContractId' is only applied to type constructors which originate from
--     a template.
checkType :: MonadGamma m => Type -> Kind -> m ()
checkType typ kind = do
  typKind <- kindOf typ
  unless (typKind == kind) $
    throwWithContext EKindMismatch{foundKind = typKind, expectedKind = kind}

kindOfDataType :: DefDataType -> Kind
kindOfDataType = foldr (KArrow . snd) KStar . dataParams

kindOfBuiltin :: BuiltinType -> Kind
kindOfBuiltin = \case
  BTInt64 -> KStar
  BTNumeric -> KNat `KArrow` KStar
  BTText -> KStar
  BTTimestamp -> KStar
  BTParty -> KStar
  BTUnit -> KStar
  BTBool -> KStar
  BTDate -> KStar
  BTList -> KStar `KArrow` KStar
  BTUpdate -> KStar `KArrow` KStar
  BTScenario -> KStar `KArrow` KStar
  BTContractId -> KStar `KArrow` KStar
  BTOptional -> KStar `KArrow` KStar
  BTTextMap -> KStar `KArrow` KStar
  BTGenMap -> KStar `KArrow` KStar `KArrow` KStar
  BTArrow -> KStar `KArrow` KStar `KArrow` KStar
  BTAny -> KStar
  BTTypeRep -> KStar
  BTRoundingMode -> KStar
  BTBigNumeric -> KStar
  BTAnyException -> KStar

checkKind :: MonadGamma m => Kind -> m ()
checkKind = \case
  KNat -> pure ()
  KStar -> pure ()
  k@(KArrow _ KNat) ->
    throwWithContext (ENatKindRightOfArrow k)
  KArrow a b -> do
    checkKind a
    checkKind b

kindOf :: MonadGamma m => Type -> m Kind
kindOf = \case
  TVar v -> lookupTypeVar v
  TCon tcon -> kindOfDataType <$> inWorld (lookupDataType tcon)
  TSynApp tsyn args -> do
    ty <- expandSynApp tsyn args
    checkType ty KStar
    pure KStar
  -- NOTE(MH): Types of the form `(forall f. f) a` are only relevant for
  -- impredicative polymorphism, which we don't support. Since this type
  -- cannot be encoded into a protobuf anyway, we fail here with more context
  -- rather than only in the serializer.
  t@(TApp TForall{} _) ->
    throwWithContext (EImpredicativePolymorphism t)
  TApp tfun targ -> do
    kind <- kindOf tfun
    (argKind, resKind) <- match _KArrow (EExpectedHigherKind kind) kind
    checkType targ argKind
    pure resKind
  TBuiltin btype -> pure (kindOfBuiltin btype)
  TForall (v, k) t1 -> do
    checkKind k
    introTypeVar v k $ checkType t1 KStar $> KStar
  TStruct recordType -> checkRecordType recordType $> KStar
  TNat _ -> pure KNat

expandTypeSynonyms :: MonadGamma m => Type -> m Type
expandTypeSynonyms = expand where
  expand = \case
    TVar v -> return $ TVar v
    TCon tcon -> return $ TCon tcon
    TSynApp tsyn args -> expandSynApp tsyn args >>= expand
    TApp tfun targ -> do
      tfun' <- expand tfun
      targ' <- expand targ
      return $ TApp tfun' targ'
    TBuiltin btype -> return $ TBuiltin btype
    TForall (v, k) t1 -> do
      t1' <- introTypeVar v k $ expand t1
      return $ TForall (v, k) t1'
    TStruct recordType -> do
      recordType' <- mapM (\(n,t) -> do t' <- expand t; return (n,t')) recordType
      return $ TStruct recordType'
    TNat typeLevelNat -> return $ TNat typeLevelNat

expandSynApp :: MonadGamma m => Qualified TypeSynName -> [Type] -> m Type
expandSynApp tsyn args = do
  def@DefTypeSyn{synParams,synType} <- inWorld (lookupTypeSyn tsyn)
  subst0 <- match _Just (ESynAppWrongArity def args) (zipExactMay synParams args)
  for_ subst0 $ \((_, kind), typ) -> checkType typ kind
  let subst1 = map (\((v, _kind), typ) -> (v, typ)) subst0
  -- NOTE(NIC): Since we're assuming that the DefTypeSyn has been checked
  -- the elements of @synParams@ are mutually distinct and
  -- contain all type variables which are free in @synType@. Thus, it is safe
  -- to call 'substitute'.
  return $ substitute (Map.fromList subst1) synType

typeOfBuiltin :: MonadGamma m => BuiltinExpr -> m Type
typeOfBuiltin = \case
  BEInt64 _          -> pure TInt64
  BENumeric n        -> pure (TNumeric (TNat (typeLevelNat (numericScale n))))
  BEText    _        -> pure TText
  BETimestamp _      -> pure TTimestamp
  BEDate _           -> pure TDate
  BEUnit             -> pure TUnit
  BEBool _           -> pure TBool
  BERoundingMode _   -> pure TRoundingMode
  BEError            -> pure $ TForall (alpha, KStar) (TText :-> tAlpha)
  BEAnyExceptionMessage -> pure $ TAnyException :-> TText
  BEEqual     -> pure $ TForall (alpha, KStar) (tAlpha :-> tAlpha :-> TBool)
  BELess      -> pure $ TForall (alpha, KStar) (tAlpha :-> tAlpha :-> TBool)
  BELessEq    -> pure $ TForall (alpha, KStar) (tAlpha :-> tAlpha :-> TBool)
  BEGreater   -> pure $ TForall (alpha, KStar) (tAlpha :-> tAlpha :-> TBool)
  BEGreaterEq -> pure $ TForall (alpha, KStar) (tAlpha :-> tAlpha :-> TBool)
  BEToText    btype  -> pure $ TBuiltin btype :-> TText
  BEContractIdToText -> pure $ TForall (alpha, KStar) $ TContractId tAlpha :-> TOptional TText
  BECodePointsToText -> pure $ TList TInt64 :-> TText
  BETextToParty    -> pure $ TText :-> TOptional TParty
  BETextToInt64    -> pure $ TText :-> TOptional TInt64
  BETextToCodePoints -> pure $ TText :-> TList TInt64
  BEAddNumeric -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TNumeric tAlpha :-> TNumeric tAlpha
  BESubNumeric -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TNumeric tAlpha :-> TNumeric tAlpha
  BEMulNumeric       -> pure $ TForall (alpha, KNat) $ TForall (beta, KNat) $ TForall (gamma, KNat) $ TNumeric tGamma :-> TNumeric tAlpha :-> TNumeric tBeta :-> TNumeric tGamma
  BEDivNumeric       -> pure $ TForall (alpha, KNat) $ TForall (beta, KNat) $ TForall (gamma, KNat) $ TNumeric tGamma :-> TNumeric tAlpha :-> TNumeric tBeta :-> TNumeric tGamma
  BERoundNumeric -> pure $ TForall (alpha, KNat) $ TInt64 :-> TNumeric tAlpha :-> TNumeric tAlpha
  BECastNumeric      -> pure $ TForall (alpha, KNat) $ TForall (beta, KNat) $ TNumeric tBeta :-> TNumeric tAlpha :-> TNumeric tBeta
  BEShiftNumeric     -> pure $ TForall (alpha, KNat) $ TForall (beta, KNat) $ TNumeric tBeta :-> TNumeric tAlpha :-> TNumeric tBeta
  BEInt64ToNumeric   -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TInt64 :-> TNumeric tAlpha
  BENumericToInt64 -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TInt64
  BENumericToText -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TText
  BETextToNumeric    -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TText :-> TOptional (TNumeric tAlpha)

  BEScaleBigNumeric -> pure $ TBigNumeric :-> TInt64
  BEPrecisionBigNumeric -> pure $ TBigNumeric :-> TInt64
  BEAddBigNumeric -> pure $ TBigNumeric :-> TBigNumeric :-> TBigNumeric
  BESubBigNumeric -> pure $ TBigNumeric :-> TBigNumeric :-> TBigNumeric
  BEMulBigNumeric -> pure $ TBigNumeric :-> TBigNumeric :-> TBigNumeric
  BEDivBigNumeric -> pure $ TInt64 :-> TRoundingMode :-> TBigNumeric :-> TBigNumeric :-> TBigNumeric
  BEShiftRightBigNumeric -> pure $ TInt64 :-> TBigNumeric :-> TBigNumeric
  BEBigNumericToNumeric -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TBigNumeric :-> TNumeric tAlpha
  BENumericToBigNumeric -> pure $ TForall (alpha, KNat) $ TNumeric tAlpha :-> TBigNumeric

  BEAddInt64         -> pure $ tBinop TInt64
  BESubInt64         -> pure $ tBinop TInt64
  BEMulInt64         -> pure $ tBinop TInt64
  BEDivInt64         -> pure $ tBinop TInt64
  BEModInt64         -> pure $ tBinop TInt64
  BEExpInt64         -> pure $ tBinop TInt64
  BEExplodeText      -> pure $ TText :-> TList TText
  BEAppendText       -> pure $ tBinop TText
  BEImplodeText      -> pure $ TList TText :-> TText
  BESha256Text       -> pure $ TText :-> TText
  BEFoldl -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $
             (tBeta :-> tAlpha :-> tBeta) :-> tBeta :-> TList tAlpha :-> tBeta
  BEFoldr -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $
             (tAlpha :-> tBeta :-> tBeta) :-> tBeta :-> TList tAlpha :-> tBeta

  BETextMapEmpty  -> pure $ TForall (alpha, KStar) $ TTextMap tAlpha
  BETextMapInsert -> pure $ TForall (alpha, KStar) $ TText :-> tAlpha :-> TTextMap tAlpha :-> TTextMap tAlpha
  BETextMapLookup -> pure $ TForall (alpha, KStar) $ TText :-> TTextMap tAlpha :-> TOptional tAlpha
  BETextMapDelete -> pure $ TForall (alpha, KStar) $ TText :-> TTextMap tAlpha :-> TTextMap tAlpha
  BETextMapToList -> pure $ TForall (alpha, KStar) $ TTextMap tAlpha :-> TList (TTextMapEntry tAlpha)
  BETextMapSize   -> pure $ TForall (alpha, KStar) $ TTextMap tAlpha :-> TInt64
  BEGenMapEmpty -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ TGenMap tAlpha tBeta
  BEGenMapInsert -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ tAlpha :-> tBeta :-> TGenMap tAlpha tBeta :-> TGenMap tAlpha tBeta
  BEGenMapLookup -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ tAlpha :-> TGenMap tAlpha tBeta :-> TOptional tBeta
  BEGenMapDelete -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ tAlpha :-> TGenMap tAlpha tBeta :-> TGenMap tAlpha tBeta
  BEGenMapKeys -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ TGenMap tAlpha tBeta :-> TList tAlpha
  BEGenMapValues -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ TGenMap tAlpha tBeta :-> TList tBeta
  BEGenMapSize -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ TGenMap tAlpha tBeta :-> TInt64

  BEEqualList -> pure $
    TForall (alpha, KStar) $
    (tAlpha :-> tAlpha :-> TBool) :-> TList tAlpha :-> TList tAlpha :-> TBool
  BETimestampToUnixMicroseconds -> pure $ TTimestamp :-> TInt64
  BEUnixMicrosecondsToTimestamp -> pure $ TInt64 :-> TTimestamp
  BEDateToUnixDays -> pure $ TDate :-> TInt64
  BEUnixDaysToDate -> pure $ TInt64 :-> TDate
  BETrace -> pure $ TForall (alpha, KStar) $ TText :-> tAlpha :-> tAlpha
  BECoerceContractId -> do
    pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $ TContractId tAlpha :-> TContractId tBeta

  BETypeRepTyConName -> pure (TTypeRep :-> TOptional TText)

  where
    tBinop typ = typ :-> typ :-> typ

checkRecCon :: MonadGamma m => TypeConApp -> [(FieldName, Expr)] -> m ()
checkRecCon typ recordExpr = do
  dataCons <- checkTypeConApp typ
  recordType <- match _DataRecord (EExpectedRecordType typ) dataCons
  let (exprFieldNames, fieldExprs) = unzip recordExpr
  let (typeFieldNames, fieldTypes) = unzip recordType
  unless (exprFieldNames == typeFieldNames) $
    throwWithContext (EFieldMismatch typ recordExpr)
  zipWithM_ checkExpr fieldExprs fieldTypes

checkVariantCon :: MonadGamma m => TypeConApp -> VariantConName -> Expr -> m ()
checkVariantCon typ@(TypeConApp tcon _) con conArg = do
  dataCons <- checkTypeConApp typ
  variantType <- match _DataVariant (EExpectedVariantType tcon) dataCons
  conArgType <- match _Just (EUnknownDataCon con) (con `lookup` variantType)
  checkExpr conArg conArgType

checkEnumCon :: MonadGamma m => Qualified TypeConName -> VariantConName -> m ()
checkEnumCon tcon con = do
    defDataType <- inWorld (lookupDataType tcon)
    enumCons <- match _DataEnum (EExpectedEnumType tcon) (dataCons defDataType)
    unless (con `elem` enumCons) $ throwWithContext (EUnknownDataCon con)

typeOfRecProj :: MonadGamma m => TypeConApp -> FieldName -> Expr -> m Type
typeOfRecProj typ0 field record = do
  dataCons <- checkTypeConApp typ0
  recordType <- match _DataRecord (EExpectedRecordType typ0) dataCons
  let typ1 = typeConAppToType typ0
  fieldType <- match _Just (EUnknownField field typ1) (lookup field recordType)
  checkExpr record typ1
  pure fieldType

typeOfRecUpd :: MonadGamma m => TypeConApp -> FieldName -> Expr -> Expr -> m Type
typeOfRecUpd typ0 field record update = do
  dataCons <- checkTypeConApp typ0
  recordType <- match _DataRecord (EExpectedRecordType typ0) dataCons
  let typ1 = typeConAppToType typ0
  fieldType <- match _Just (EUnknownField field typ1) (lookup field recordType)
  checkExpr record typ1
  catchAndRethrow
    (\case
      ETypeMismatch { foundType, expectedType, expr } ->
        EFieldTypeMismatch
          { targetRecord = typ1
          , fieldName = field
          , foundType, expectedType, expr
          }
      e -> e)
    (checkExpr update fieldType)
  pure typ1

typeOfStructCon :: MonadGamma m => [(FieldName, Expr)] -> m Type
typeOfStructCon recordExpr = do
  checkUnique EDuplicateField (map fst recordExpr)
  TStruct <$> (traverse . _2) typeOf recordExpr

typeOfStructProj :: MonadGamma m => FieldName -> Expr -> m Type
typeOfStructProj field expr = do
  typ <- typeOf expr
  structType <- match _TStruct (EExpectedStructType typ) typ
  match _Just (EUnknownField field typ) (lookup field structType)

typeOfStructUpd :: MonadGamma m => FieldName -> Expr -> Expr -> m Type
typeOfStructUpd field struct update = do
  typ <- typeOf struct
  structType <- match _TStruct (EExpectedStructType typ) typ
  fieldType <- match _Just (EUnknownField field typ) (lookup field structType)
  checkExpr update fieldType
  pure typ

typeOfTmApp :: MonadGamma m => Expr -> Expr -> m Type
typeOfTmApp fun arg = do
  typ <- typeOf fun
  case typ of
    argType :-> resType -> do
      checkExpr arg argType
      pure resType
    _ -> throwWithContext (EExpectedFunctionType typ)

typeOfTyApp :: MonadGamma m => Expr -> Type -> m Type
typeOfTyApp expr typ = do
  exprType <- typeOf expr
  ((tvar, kind), typeBody) <- match _TForall (EExpectedUniversalType exprType) exprType
  checkType typ kind
  -- NOTE(MH): Calling 'substitute' is safe since @typ@ and @typeBody@ live in
  -- the same context.
  pure (substitute (Map.singleton tvar typ) typeBody)

typeOfTmLam :: MonadGamma m => (ExprVarName, Type) -> Expr -> m Type
typeOfTmLam (var, typ) body = do
  checkType typ KStar
  bodyType <- introExprVar var typ (typeOf body)
  pure (typ :-> bodyType)

typeOfTyLam :: MonadGamma m => (TypeVarName, Kind) -> Expr -> m Type
typeOfTyLam (tvar, kind) expr = do
    checkKind kind
    TForall (tvar, kind) <$>
        introTypeVar tvar kind (typeOf expr)

-- | Type to track which constructor ranks have be covered in a pattern matching.
data MatchedRanks = AllRanks | SomeRanks !IntSet.IntSet

emptyMR :: MatchedRanks
emptyMR = SomeRanks IntSet.empty

singletonMR :: Int -> MatchedRanks
singletonMR k = SomeRanks (IntSet.singleton k)

unionMR :: MatchedRanks -> MatchedRanks -> MatchedRanks
unionMR AllRanks _ = AllRanks
unionMR _ AllRanks = AllRanks
unionMR (SomeRanks ks) (SomeRanks ls) = SomeRanks (ks `IntSet.union` ls)

unionsMR :: [MatchedRanks] -> MatchedRanks
unionsMR = foldl' unionMR emptyMR

missingMR :: MatchedRanks -> Int -> Maybe Int
missingMR AllRanks _ = Nothing
missingMR (SomeRanks ks) n
    | IntSet.size ks == n = Nothing
    | otherwise = IntSet.lookupGE 0 (IntSet.fromDistinctAscList [0..n-1] `IntSet.difference` ks)

typeOfAlts :: MonadGamma m => (CaseAlternative -> m (MatchedRanks, Type)) -> [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAlts f alts = do
    (ks, ts) <- unzip <$> traverse f alts
    case ts of
        [] -> throwWithContext EEmptyCase
        t:ts' -> do
            forM_ ts' $ \t' -> unless (alphaType t t') $
                throwWithContext ETypeMismatch{foundType = t', expectedType = t, expr = Nothing}
            pure (unionsMR ks, t)

typeOfAltsVariant :: MonadGamma m => Qualified TypeConName -> [Type] -> [TypeVarName] -> [(VariantConName, Type)] -> [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsVariant scrutTCon scrutTArgs tparams cons =
    typeOfAlts $ \(CaseAlternative patn rhs) -> case patn of
        CPVariant patnTCon con var
          | scrutTCon == patnTCon -> do
            (conRank, conArgType) <- match _Just (EUnknownDataCon con) (con `lookupWithIndex` cons)
            -- NOTE(MH): The next line should never throw since @scrutType@ has
            -- already been checked. The call to 'substitute' is hence safe for
            -- the same reason as in 'checkTypeConApp'.
            subst <- match _Just (ETypeConAppWrongArity (TypeConApp scrutTCon scrutTArgs)) (zipExactMay tparams scrutTArgs)
            let varType = substitute (Map.fromList subst) conArgType
            rhsType <- introExprVar var varType $ typeOf rhs
            pure (singletonMR conRank, rhsType)
        CPDefault -> (,) AllRanks <$> typeOf rhs
        _ -> throwWithContext (EPatternTypeMismatch patn (TConApp scrutTCon scrutTArgs))

typeOfAltsEnum :: MonadGamma m => Qualified TypeConName -> [VariantConName] -> [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsEnum scrutTCon cons =
    typeOfAlts $ \(CaseAlternative patn rhs) -> case patn of
        CPEnum patnTCon con
          | scrutTCon == patnTCon -> do
            conRank <- match _Just (EUnknownDataCon con) (con `elemIndex` cons)
            rhsType <- typeOf rhs
            pure (singletonMR conRank, rhsType)
        CPDefault -> (,) AllRanks <$> typeOf rhs
        _ -> throwWithContext (EPatternTypeMismatch patn (TCon scrutTCon))

typeOfAltsUnit :: MonadGamma m => [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsUnit  =
    typeOfAlts $ \(CaseAlternative patn rhs) -> do
        case patn of
            CPUnit -> (,) AllRanks <$> typeOf rhs
            CPDefault -> (,) AllRanks <$> typeOf rhs
            _ -> throwWithContext (EPatternTypeMismatch patn TUnit)

typeOfAltsBool :: MonadGamma m => [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsBool =
    typeOfAlts $ \(CaseAlternative patn rhs) -> do
        case patn of
            CPBool (b :: Bool) -> do
                rhsType <- typeOf rhs
                pure (singletonMR (fromEnum b), rhsType)
            CPDefault -> (,) AllRanks <$> typeOf rhs
            _ -> throwWithContext (EPatternTypeMismatch patn TBool)

typeOfAltsList :: MonadGamma m => Type -> [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsList elemType =
    typeOfAlts $ \(CaseAlternative patn rhs) -> do
        case patn of
            CPNil -> (,) (singletonMR 0) <$> typeOf rhs
            CPCons headVar tailVar
              | headVar == tailVar -> throwWithContext (EClashingPatternVariables headVar)
              | otherwise -> do
                rhsType <- introExprVar headVar elemType $ introExprVar tailVar (TList elemType) $ typeOf rhs
                pure (singletonMR 1, rhsType)
            CPDefault -> (,) AllRanks <$> typeOf rhs
            _ -> throwWithContext (EPatternTypeMismatch patn (TList elemType))

typeOfAltsOptional :: MonadGamma m => Type -> [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsOptional elemType =
    typeOfAlts $ \(CaseAlternative patn rhs) -> do
        case patn of
            CPNone -> (,) (singletonMR 0) <$> typeOf rhs
            CPSome bodyVar -> do
                rhsType <- introExprVar bodyVar elemType $ typeOf rhs
                pure (singletonMR 1, rhsType)
            CPDefault -> (,) AllRanks <$> typeOf rhs
            _ -> throwWithContext (EPatternTypeMismatch patn (TOptional elemType))

-- NOTE(MH): The Daml-LF spec says that `CPDefault` matches _every_ value,
-- regardless of its type.
typeOfAltsOnlyDefault :: MonadGamma m => Type -> [CaseAlternative] -> m (MatchedRanks, Type)
typeOfAltsOnlyDefault scrutType =
    typeOfAlts $ \(CaseAlternative patn rhs) -> do
        case patn of
            CPDefault -> (,) AllRanks <$> typeOf rhs
            _ -> throwWithContext (EPatternTypeMismatch patn scrutType)

typeOfCase :: MonadGamma m => Expr -> [CaseAlternative] -> m Type
typeOfCase scrut alts = do
    scrutType <- typeOf scrut
    (numRanks, rankToPat, (matchedRanks, rhsType)) <- case scrutType of
        TConApp scrutTCon scrutTArgs -> do
            DefDataType{dataParams, dataCons} <- inWorld (lookupDataType scrutTCon)
            case dataCons of
                DataVariant cons -> (,,) (length cons) (\k -> CPVariant scrutTCon (fst (cons !! k)) wildcard)
                    <$> typeOfAltsVariant scrutTCon scrutTArgs (map fst dataParams) cons alts
                DataEnum cons ->  (,,) (length cons) (\k -> CPEnum scrutTCon (cons !! k))
                    <$> typeOfAltsEnum scrutTCon cons alts
                DataRecord{} -> (,,) 1 (const CPDefault) <$> typeOfAltsOnlyDefault scrutType alts
                DataInterface -> (,,) 1 (const CPDefault) <$> typeOfAltsOnlyDefault scrutType alts
        TUnit -> (,,) 1 (const CPUnit) <$> typeOfAltsUnit alts
        TBool -> (,,) 2 (CPBool . toEnum) <$> typeOfAltsBool alts
        TList elemType -> (,,) 2 ([CPNil, CPCons wildcard wildcard] !!) <$> typeOfAltsList elemType alts
        TOptional elemType -> (,,) 2 ([CPNone, CPSome wildcard] !!) <$> typeOfAltsOptional elemType alts
        _ -> (,,) 1 (const CPDefault) <$> typeOfAltsOnlyDefault scrutType alts
    whenJust (missingMR matchedRanks numRanks) $ \k ->
        throwWithContext (ENonExhaustivePatterns (rankToPat k) scrutType)
    pure rhsType
  where
    wildcard = ExprVarName "_"

typeOfLet :: MonadGamma m => Binding -> Expr -> m Type
typeOfLet (Binding (var, typ0) expr) body = do
  checkType typ0 KStar
  typ1 <- checkExpr' expr typ0
  introExprVar var typ1 (typeOf body)

checkCons :: MonadGamma m => Type -> Expr -> Expr -> m ()
checkCons elemType headExpr tailExpr = do
  checkType elemType KStar
  checkExpr headExpr elemType
  checkExpr tailExpr (TList elemType)

checkSome :: MonadGamma m => Type -> Expr -> m ()
checkSome bodyType bodyExpr = do
  checkType bodyType KStar
  checkExpr bodyExpr bodyType

checkPure :: MonadGamma m => Type -> Expr -> m ()
checkPure typ expr = do
  checkType typ KStar
  checkExpr expr typ

typeOfBind :: MonadGamma m => Binding -> Expr -> m Type
typeOfBind (Binding (var, typ) bound) body = do
  checkType typ KStar
  checkExpr bound (TUpdate typ)
  bodyType <- introExprVar var typ (typeOf body)
  _ :: Type <- match _TUpdate (EExpectedUpdateType bodyType) bodyType
  pure bodyType

checkCreate :: MonadGamma m => Qualified TypeConName -> Expr -> m ()
checkCreate tpl arg = do
  _ :: Template <- inWorld (lookupTemplate tpl)
  checkExpr arg (TCon tpl)

checkCreateInterface :: MonadGamma m => Qualified TypeConName -> Expr -> m ()
checkCreateInterface iface arg = do
  _ :: DefInterface <- inWorld (lookupInterface iface)
  checkExpr arg (TCon iface)

typeOfExercise :: MonadGamma m =>
  Qualified TypeConName -> ChoiceName -> Expr -> Expr -> m Type
typeOfExercise tpl chName cid arg = do
  choice <- inWorld (lookupChoice (tpl, chName))
  checkExpr cid (TContractId (TCon tpl))
  checkExpr arg (chcArgType choice)
  pure (TUpdate (chcReturnType choice))

typeOfExerciseInterface :: MonadGamma m =>
  Qualified TypeConName -> ChoiceName -> Expr -> Expr -> Maybe Expr -> m Type
typeOfExerciseInterface iface chName cid arg mayGuard = do
  choice <- inWorld (lookupInterfaceChoice (iface, chName))
  checkExpr cid (TContractId (TCon iface))
  checkExpr arg (chcArgType choice)
  case mayGuard of
    Nothing -> pure ()
    Just guard -> checkExpr guard (TCon iface :-> TBool)
  pure (TUpdate (chcReturnType choice))

typeOfExerciseByKey :: MonadGamma m =>
  Qualified TypeConName -> ChoiceName -> Expr -> Expr -> m Type
typeOfExerciseByKey tplId chName key arg = do
  tpl <- inWorld (lookupTemplate tplId)
  case tplKey tpl of
    Nothing -> throwWithContext (EKeyOperationOnTemplateWithNoKey tplId)
    Just tKey -> do
      choice <- inWorld (lookupChoice (tplId, chName))
      checkExpr key (tplKeyType tKey)
      checkExpr arg (chcArgType choice)
      pure (TUpdate (chcReturnType choice))

checkFetch :: MonadGamma m => Qualified TypeConName -> Expr -> m ()
checkFetch tpl cid = do
  _ :: Template <- inWorld (lookupTemplate tpl)
  checkExpr cid (TContractId (TCon tpl))

checkFetchInterface :: MonadGamma m => Qualified TypeConName -> Expr -> m ()
checkFetchInterface tpl cid = do
  void $ inWorld (lookupInterface tpl)
  checkExpr cid (TContractId (TCon tpl))

-- | Check that there's a unique interface instance for the given
-- interface + template pair.
checkUniqueInterfaceInstance :: MonadGamma m => InterfaceInstanceHead -> m ()
checkUniqueInterfaceInstance = inWorld . lookupInterfaceInstance

-- returns the contract id and contract type
checkRetrieveByKey :: MonadGamma m => RetrieveByKey -> m (Type, Type)
checkRetrieveByKey RetrieveByKey{..} = do
  tpl <- inWorld (lookupTemplate retrieveByKeyTemplate)
  case tplKey tpl of
    Nothing -> throwWithContext (EKeyOperationOnTemplateWithNoKey retrieveByKeyTemplate)
    Just key -> do
      checkExpr retrieveByKeyKey (tplKeyType key)
      return (TContractId (TCon retrieveByKeyTemplate), TCon retrieveByKeyTemplate)

typeOfUpdate :: MonadGamma m => Update -> m Type
typeOfUpdate = \case
  UPure typ expr -> checkPure typ expr $> TUpdate typ
  UBind binding body -> typeOfBind binding body
  UCreate tpl arg -> checkCreate tpl arg $> TUpdate (TContractId (TCon tpl))
  UCreateInterface iface arg -> checkCreateInterface iface arg $> TUpdate (TContractId (TCon iface))
  UExercise tpl choice cid arg -> typeOfExercise tpl choice cid arg
  UDynamicExercise tpl choice cid arg -> typeOfExercise tpl choice cid arg
  UExerciseInterface tpl choice cid arg guard ->
    typeOfExerciseInterface tpl choice cid arg guard
  UExerciseByKey tpl choice key arg -> typeOfExerciseByKey tpl choice key arg
  UFetch tpl cid -> checkFetch tpl cid $> TUpdate (TCon tpl)
  UFetchInterface tpl cid -> checkFetchInterface tpl cid $> TUpdate (TCon tpl)
  UGetTime -> pure (TUpdate TTimestamp)
  UEmbedExpr typ e -> do
    checkExpr e (TUpdate typ)
    return (TUpdate typ)
  UFetchByKey retrieveByKey -> do
    (cidType, contractType) <- checkRetrieveByKey retrieveByKey
    return (TUpdate (TTuple2 cidType contractType))
  ULookupByKey retrieveByKey -> do
    (cidType, _contractType) <- checkRetrieveByKey retrieveByKey
    return (TUpdate (TOptional cidType))
  UTryCatch typ expr var handler -> do
    checkType typ KStar
    checkExpr expr (TUpdate typ)
    introExprVar var TAnyException $ do
        checkExpr handler (TOptional (TUpdate typ))
    pure (TUpdate typ)

typeOfScenario :: MonadGamma m => Scenario -> m Type
typeOfScenario = \case
  SPure typ expr -> checkPure typ expr $> TScenario typ
  SBind (Binding (var, typ) bound) body -> do
    checkType typ KStar
    checkExpr bound (TScenario typ)
    bodyType <- introExprVar var typ (typeOf body)
    _ :: Type <- match _TScenario (EExpectedScenarioType bodyType) bodyType
    pure bodyType
  SCommit typ party update -> do
    checkType typ KStar
    checkExpr party TParty
    checkExpr' update (TUpdate typ) >>= \case
      TUpdate t -> pure (TScenario t)
      t -> throwWithContext (EExpectedUpdateType t)
  SMustFailAt typ party update -> do
    checkType typ KStar
    checkExpr party TParty
    checkExpr update (TUpdate typ)
    pure (TScenario TUnit)
  SPass delta -> checkExpr delta TInt64 $> TScenario TTimestamp
  SGetTime -> pure (TScenario TTimestamp)
  SGetParty name -> checkExpr name TText $> TScenario TParty
  SEmbedExpr typ e -> do
    checkExpr e (TScenario typ)
    return (TScenario typ)

typeOf' :: MonadGamma m => Expr -> m Type
typeOf' = \case
  EVar var -> lookupExprVar var
  EVal val -> dvalType <$> inWorld (lookupValue val)
  EBuiltinFun bexpr -> typeOfBuiltin bexpr
  ERecCon typ recordExpr -> checkRecCon typ recordExpr $> typeConAppToType typ
  ERecProj typ field rec -> typeOfRecProj typ field rec
  ERecUpd typ field record update -> typeOfRecUpd typ field record update
  EVariantCon typ con arg -> checkVariantCon typ con arg $> typeConAppToType typ
  EEnumCon typ con -> checkEnumCon typ con $> TCon typ
  EStructCon recordExpr -> typeOfStructCon recordExpr
  EStructProj field expr -> typeOfStructProj field expr
  EStructUpd field struct update -> typeOfStructUpd field struct update
  ETmApp fun arg -> typeOfTmApp fun arg
  ETyApp expr typ -> typeOfTyApp expr typ
  ETmLam binder body -> typeOfTmLam binder body
  ETyLam tvar expr -> typeOfTyLam tvar expr
  ECase scrut alts -> typeOfCase scrut alts
  ELet binding body -> typeOfLet binding body
  ENil elemType -> checkType elemType KStar $> TList elemType
  ECons elemType headExpr tailExpr -> checkCons elemType headExpr tailExpr $> TList elemType
  ESome bodyType bodyExpr -> checkSome bodyType bodyExpr $> TOptional bodyType
  ENone bodyType -> checkType bodyType KStar $> TOptional bodyType
  EToAny ty bodyExpr -> do
    checkGroundType ty
    checkExpr bodyExpr ty
    pure $ TBuiltin BTAny
  EFromAny ty bodyExpr -> do
    checkGroundType ty
    checkExpr bodyExpr (TBuiltin BTAny)
    pure $ TOptional ty
  ETypeRep ty -> do
    checkGroundType ty
    pure $ TBuiltin BTTypeRep
  EToAnyException ty val -> do
    checkExceptionType ty
    checkExpr val ty
    pure TAnyException
  EFromAnyException ty val -> do
    checkExceptionType ty
    checkExpr val TAnyException
    pure (TOptional ty)
  EThrow ty1 ty2 val -> do
    checkType ty1 KStar
    checkExceptionType ty2
    checkExpr val ty2
    pure ty1
  EToInterface iface tpl val -> do
    checkUniqueInterfaceInstance (InterfaceInstanceHead iface tpl)
    checkExpr val (TCon tpl)
    pure (TCon iface)
  EFromInterface iface tpl val -> do
    checkUniqueInterfaceInstance (InterfaceInstanceHead iface tpl)
    checkExpr val (TCon iface)
    pure (TOptional (TCon tpl))
  EUnsafeFromInterface iface tpl cid val -> do
    checkUniqueInterfaceInstance (InterfaceInstanceHead iface tpl)
    checkExpr cid (TContractId (TCon iface))
    checkExpr val (TCon iface)
    pure (TCon tpl)
  ECallInterface iface method val -> do
    method <- inWorld (lookupInterfaceMethod (iface, method))
    checkExpr val (TCon iface)
    pure (ifmType method)
  EToRequiredInterface requiredIface requiringIface expr -> do
    allRequiredIfaces <- intRequires <$> inWorld (lookupInterface requiringIface)
    unless (S.member requiredIface allRequiredIfaces) $ do
      throwWithContext (EWrongInterfaceRequirement requiringIface requiredIface)
    checkExpr expr (TCon requiringIface)
    pure (TCon requiredIface)
  EFromRequiredInterface requiredIface requiringIface expr -> do
    allRequiredIfaces <- intRequires <$> inWorld (lookupInterface requiringIface)
    unless (S.member requiredIface allRequiredIfaces) $ do
      throwWithContext (EWrongInterfaceRequirement requiringIface requiredIface)
    checkExpr expr (TCon requiredIface)
    pure (TOptional (TCon requiringIface))
  EUnsafeFromRequiredInterface requiredIface requiringIface cid expr -> do
    allRequiredIfaces <- intRequires <$> inWorld (lookupInterface requiringIface)
    unless (S.member requiredIface allRequiredIfaces) $ do
      throwWithContext (EWrongInterfaceRequirement requiringIface requiredIface)
    checkExpr cid (TContractId (TCon requiredIface))
    checkExpr expr (TCon requiredIface)
    pure (TCon requiringIface)
  EInterfaceTemplateTypeRep iface expr -> do
    void $ inWorld (lookupInterface iface)
    checkExpr expr (TCon iface)
    pure TTypeRep
  ESignatoryInterface iface expr -> do
    void $ inWorld (lookupInterface iface)
    checkExpr expr (TCon iface)
    pure (TList TParty)
  EObserverInterface iface expr -> do
    void $ inWorld (lookupInterface iface)
    checkExpr expr (TCon iface)
    pure (TList TParty)
  EUpdate upd -> typeOfUpdate upd
  EScenario scen -> typeOfScenario scen
  ELocation _ expr -> typeOf' expr
  EViewInterface ifaceId expr -> do
    iface <- inWorld (lookupInterface ifaceId)
    checkExpr expr (TCon ifaceId)
    pure (intView iface)
  EChoiceController tpl choiceName contract choiceArg -> do
    ty <- inWorld (lookupTemplateOrInterface tpl)
    choice <- inWorld $ case ty of
      TemplateOrInterface.Template {} -> lookupChoice (tpl, choiceName)
      TemplateOrInterface.Interface {} -> lookupInterfaceChoice (tpl, choiceName)
    checkExpr contract (TCon tpl)
    checkExpr choiceArg (chcArgType choice)
    pure (TList TParty)
  EChoiceObserver tpl choiceName contract choiceArg -> do
    ty <- inWorld (lookupTemplateOrInterface tpl)
    choice <- inWorld $ case ty of
      TemplateOrInterface.Template {} -> lookupChoice (tpl, choiceName)
      TemplateOrInterface.Interface {} -> lookupInterfaceChoice (tpl, choiceName)
    checkExpr contract (TCon tpl)
    checkExpr choiceArg (chcArgType choice)
    pure (TList TParty)
  EExperimental name ty -> do
    checkFeature featureExperimental
    checkExperimentalType name ty
    pure ty

checkExperimentalType :: MonadGamma m => T.Text -> Type -> m ()
checkExperimentalType "ANSWER" (TUnit :-> TInt64) = pure ()
checkExperimentalType name ty =
  throwWithContext (EUnknownExperimental name ty)

typeOf :: MonadGamma m => Expr -> m Type
typeOf expr = do
  ty <- typeOf' expr
  expandTypeSynonyms ty

-- Check that the type contains no type variables or quantifiers
checkGroundType' :: MonadGamma m => Type -> m ()
checkGroundType' ty =
    when (para (\t children -> or (isForbidden t : children)) ty) $ throwWithContext $ EExpectedAnyType ty
  where isForbidden (TVar _) = True
        isForbidden (TForall _ _) = True
        isForbidden _ = False

checkGroundType :: MonadGamma m => Type -> m ()
checkGroundType ty = do
    _ <- checkType ty KStar
    checkGroundType' ty

checkExceptionType' :: MonadGamma m => Type -> m ()
checkExceptionType' = \case
    TCon qtcon -> do
      _ <- inWorld (lookupException qtcon)
      pure ()
    ty -> throwWithContext (EExpectedExceptionType ty)

checkExceptionType :: MonadGamma m => Type -> m ()
checkExceptionType ty = do
    checkType ty KStar
    checkExceptionType' ty

checkExpr' :: MonadGamma m => Expr -> Type -> m Type
checkExpr' expr typ = do
  exprType <- typeOf expr
  typX <- expandTypeSynonyms typ
  unless (alphaType exprType typX) $
    throwWithContext $
      case expr of
        ERecProj { recTypeCon, recField } ->
          EFieldTypeMismatch
            { targetRecord = typeConAppToType recTypeCon
            , fieldName = recField
            , foundType = exprType
            , expectedType = typX
            , expr = Just expr
            }
        _ ->
          ETypeMismatch{foundType = exprType, expectedType = typX, expr = Just expr}
  pure exprType

checkExpr :: MonadGamma m => Expr -> Type -> m ()
checkExpr expr typ = void (checkExpr' expr typ)

-- | Check that a type synonym definition is well-formed.
checkDefTypeSyn :: MonadGamma m => DefTypeSyn -> m ()
checkDefTypeSyn DefTypeSyn{synParams,synType} = do
  checkUnique EDuplicateTypeParam $ map fst synParams
  mapM_ (checkKind . snd) synParams
  foldr (uncurry introTypeVar) base synParams
  where
    base = checkType synType KStar

-- | Check that an interface definition is well defined.
checkIface :: MonadGamma m => Module -> DefInterface -> m ()
checkIface m iface = do

  -- check view
  let (func, _) = viewtype ^. _TApps
  tycon <- case func of
    TCon tycon -> pure tycon
    TApp {} -> error "checkIface: should not happen"
    _ -> throwWithContext (EViewTypeHeadNotCon func viewtype)
  DefDataType _loc _naem _serializable tparams dataCons <- inWorld (lookupDataType tycon)
  unless (null tparams) $ throwWithContext (EViewTypeHasVars viewtype)
  case dataCons of
    DataRecord {} -> pure ()
    _ -> throwWithContext (EViewTypeConNotRecord dataCons viewtype)

  -- check requires
  when (tcon `S.member` intRequires iface) $
    throwWithContext (ECircularInterfaceRequires (intName iface) Nothing)
  forM_ (intRequires iface) $ \requiredIfaceId -> do
    requiredIface <- inWorld (lookupInterface requiredIfaceId)
    when (tcon `S.member` intRequires requiredIface) $
      throwWithContext (ECircularInterfaceRequires (intName iface) (Just requiredIfaceId))
    let missing = intRequires requiredIface `S.difference` intRequires iface
    unless (S.null missing) $ throwWithContext $
      ENotClosedInterfaceRequires (intName iface) requiredIfaceId (S.toList missing)

  -- check methods
  checkUnique (EDuplicateInterfaceMethodName (intName iface)) $ NM.names (intMethods iface)
  forM_ (intMethods iface) \method ->
    withPart (IPMethod method) do
      checkIfaceMethod method

  -- check choices
  checkUnique (EDuplicateInterfaceChoiceName (intName iface)) $ NM.names (intChoices iface)
  introExprVar (intParam iface) (TCon tcon) do
    forM_ (intChoices iface) \choice ->
      withPart (IPChoice choice) do
        checkTemplateChoice tcon choice

  where
    tcon = Qualified PRSelf (moduleName m) (intName iface)
    viewtype = intView iface
    withPart p = withContext (ContextDefInterface m iface p)

checkIfaceMethod :: MonadGamma m => InterfaceMethod -> m ()
checkIfaceMethod InterfaceMethod{ifmType} = do
  checkType ifmType KStar

-- | Check that a type constructor definition is well-formed.
checkDefDataType :: MonadGamma m => Module -> DefDataType -> m ()
checkDefDataType m (DefDataType _loc name _serializable params dataCons) = do
  checkUnique EDuplicateTypeParam $ map fst params
  mapM_ (checkKind . snd) params
  foldr (uncurry introTypeVar) base params
  where
    base = case dataCons of
      DataRecord  fields  -> checkRecordType fields
      DataVariant (unzip -> (names, types)) -> do
        checkUnique EDuplicateConstructor names
        traverse_ (`checkType` KStar) types
      DataEnum names -> do
        unless (null params) $ throwWithContext EEnumTypeWithParams
        checkUnique EDuplicateConstructor names
      DataInterface -> do
        unless (null params) $ throwWithContext EInterfaceTypeWithParams
        void $ inWorld $ lookupInterface (Qualified PRSelf (moduleName m) name)

checkDefValue :: MonadGamma m => DefValue -> m ()
checkDefValue (DefValue _loc (_, typ) (IsTest isTest) expr) = do
  checkType typ KStar
  checkExpr expr typ
  when isTest $
    case view _TForalls typ of
      (_, TScenario _) -> pure ()
      _ -> throwWithContext (EExpectedScenarioType typ)

checkTemplateChoice :: MonadGamma m => Qualified TypeConName -> TemplateChoice -> m ()
checkTemplateChoice tpl (TemplateChoice _loc _ _ controllers mbObservers mbAuthorizers selfBinder (param, paramType) retType upd) = do
  checkType paramType KStar
  checkType retType KStar
  introExprVar param paramType $ checkExpr controllers (TList TParty)
  introExprVar param paramType $ do
    whenJust mbObservers $ \observers -> do
      checkExpr observers (TList TParty)
  introExprVar param paramType $ do
    whenJust mbAuthorizers $ \authorizers -> do
      checkExpr authorizers (TList TParty)
  introExprVar selfBinder (TContractId (TCon tpl)) $ introExprVar param paramType $
    checkExpr upd (TUpdate retType)

checkTemplate :: forall m. MonadGamma m => Module -> Template -> m ()
checkTemplate m t@(Template _loc tpl param precond signatories observers choices mbKey implements) = do
  let tcon = Qualified PRSelf (moduleName m) tpl
  DefDataType _loc _naem _serializable tparams dataCons <- inWorld (lookupDataType tcon)
  unless (null tparams) $ throwWithContext (EExpectedTemplatableType tpl)
  _ <- match _DataRecord (EExpectedTemplatableType tpl) dataCons
  introExprVar param (TCon tcon) $ do
    withPart TPPrecondition $ checkExpr precond TBool
    withPart TPSignatories $ checkExpr signatories (TList TParty)
    withPart TPObservers $ checkExpr observers (TList TParty)
    for_ choices $ \c -> withPart (TPChoice c) $ checkTemplateChoice tcon c
  forM_ implements \TemplateImplements {tpiInterface, tpiBody, tpiLocation} -> do
    let iiHead = InterfaceInstanceHead tpiInterface tcon
    withPart (TPInterfaceInstance iiHead tpiLocation) do
      checkInterfaceInstance param iiHead tpiBody
  whenJust mbKey $ checkTemplateKey param tcon

  where
    withPart p = withContext (ContextTemplate m t p)

checkInterfaceInstance :: MonadGamma m =>
     ExprVarName
  -> InterfaceInstanceHead
  -> InterfaceInstanceBody
  -> m ()
checkInterfaceInstance tmplParam iiHead iiBody = do
  let
    InterfaceInstanceHead { iiInterface, iiTemplate } = iiHead
    InterfaceInstanceBody {iiMethods, iiView} = iiBody

  -- Check that this is the only interface instance with this head
  checkUniqueInterfaceInstance iiHead

  -- check requires
  DefInterface {intRequires, intMethods, intView} <- inWorld (lookupInterface iiInterface)
  forM_ intRequires \required -> do
    let requiredInterfaceInstance = InterfaceInstanceHead required iiTemplate
    eRequired <- inWorld (Right . lookupInterfaceInstance requiredInterfaceInstance)
    whenLeft eRequired \(_ :: LookupError) ->
      throwWithContext (EMissingRequiredInterfaceInstance requiredInterfaceInstance iiInterface)

  -- check definitions in interface instance
  -- Note (MA): we use an empty environment and add `tmplParam : TTyCon(iiTemplate)`
  introExprVar tmplParam (TCon iiTemplate) $ do
    -- check methods
    let missingMethods = HS.difference (NM.namesSet intMethods) (NM.namesSet iiMethods)
    whenJust (listToMaybe (HS.toList missingMethods)) \methodName ->
      throwWithContext (EMissingMethodInInterfaceInstance methodName)
    forM_ iiMethods \InterfaceInstanceMethod{iiMethodName, iiMethodExpr} -> do
      case NM.lookup iiMethodName intMethods of
        Nothing -> throwWithContext (EUnknownMethodInInterfaceInstance iiInterface iiTemplate iiMethodName)
        Just InterfaceMethod{ifmType} ->
          catchAndRethrow
            (\case
              ETypeMismatch { foundType, expectedType, expr } ->
                EMethodTypeMismatch
                  { emtmIfaceName = iiInterface
                  , emtmTplName = iiTemplate
                  , emtmMethodName = iiMethodName
                  , emtmFoundType = foundType
                  , emtmExpectedType = expectedType
                  , emtmExpr = expr
                  }
              e -> e)
            (checkExpr iiMethodExpr ifmType)

    -- check view result type matches interface result type
    catchAndRethrow
      (\case
          ETypeMismatch { foundType, expectedType, expr } ->
            EViewTypeMismatch
              { evtmIfaceName = iiInterface
              , evtmTplName = iiTemplate
              , evtmFoundType = foundType
              , evtmExpectedType = expectedType
              , evtmExpr = expr
              }
          e -> e)
      (checkExpr iiView intView)

checkFeature :: MonadGamma m => Feature -> m ()
checkFeature feature = do
    version <- getLfVersion
    unless (version `supports` feature) $
        throwWithContext $ EUnsupportedFeature feature

checkTemplateKey :: MonadGamma m => ExprVarName -> Qualified TypeConName -> TemplateKey -> m ()
checkTemplateKey param tcon TemplateKey{..} = do
    checkType tplKeyType KStar
    introExprVar param (TCon tcon) $ do
      checkExpr tplKeyBody tplKeyType
    checkExpr tplKeyMaintainers (tplKeyType :-> TList TParty)

checkDefException :: MonadGamma m => Module -> DefException -> m ()
checkDefException m DefException{..} = do
    let modName = moduleName m
        tcon = Qualified PRSelf modName exnName
    DefDataType _loc _name _serializable tyParams dataCons <- inWorld (lookupDataType tcon)
    unless (null tyParams) $ throwWithContext (EExpectedExceptionTypeHasNoParams modName exnName)
    checkExpr exnMessage (TCon tcon :-> TText)
    _ <- match _DataRecord (EExpectedExceptionTypeIsRecord modName exnName) dataCons
    whenJust (NM.lookup exnName (moduleTemplates m)) $ \_ ->
      throwWithContext (EExpectedExceptionTypeIsNotTemplate modName exnName)

-- NOTE(MH): It is important that the data type definitions are checked first.
-- The type checker for expressions relies on the fact that data type
-- definitions do _not_ contain free variables.
checkModule :: MonadGamma m => Module -> m ()
checkModule m@(Module _modName _path _flags synonyms dataTypes values templates exceptions interfaces) = do
  let with ctx f x = withContext (ctx x) (f x)
  traverse_ (with (ContextDefTypeSyn m) checkDefTypeSyn) synonyms
  traverse_ (with (ContextDefDataType m) $ checkDefDataType m) dataTypes
  -- NOTE(SF): Interfaces should be checked before templates, because the typechecking
  -- for templates relies on well-typed interface definitions.
  traverse_ (with (\i -> ContextDefInterface m i IPWhole) (checkIface m)) interfaces
  traverse_ (with (\t -> ContextTemplate m t TPWhole) $ checkTemplate m) templates
  traverse_ (with (ContextDefException m) (checkDefException m)) exceptions
  traverse_ (with (ContextDefValue m) checkDefValue) values
