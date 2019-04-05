-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
-- | This module contains the DAML-LF type checker.
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
module DA.Daml.LF.TypeChecker.Check where

import DA.Prelude

import           Control.Lens hiding (Context)
import           Data.Foldable
import           Data.Functor
import qualified Data.HashSet as HS
import qualified Data.Map.Strict as Map
import           Safe.Exact (zipExactMay)

import           DA.Daml.LF.Ast hiding (dataCons)
import           DA.Daml.LF.Ast.Optics (dataConsType)
import           DA.Daml.LF.Ast.Type
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
  BTDecimal -> KStar
  BTText -> KStar
  BTTimestamp -> KStar
  BTParty -> KStar
  BTEnum _ -> KStar
  BTDate -> KStar
  BTList -> KStar `KArrow` KStar
  BTUpdate -> KStar `KArrow` KStar
  BTScenario -> KStar `KArrow` KStar
  BTContractId -> KStar `KArrow` KStar
  BTOptional -> KStar `KArrow` KStar
  BTMap -> KStar `KArrow` KStar
  BTArrow -> KStar `KArrow` KStar `KArrow` KStar

kindOf :: MonadGamma m => Type -> m Kind
kindOf = \case
  TVar v -> lookupTypeVar v
  TCon tcon -> kindOfDataType <$> inWorld (lookupDataType tcon)
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
  TForall (v, k) t1 -> introTypeVar v k $ checkType t1 KStar $> KStar
  TTuple recordType -> checkRecordType recordType $> KStar

typeOfEnumCon :: EnumCon -> Type
typeOfEnumCon = \case
  ECUnit  -> TUnit
  ECFalse -> TBool
  ECTrue  -> TBool

typeOfBuiltin :: MonadGamma m => BuiltinExpr -> m Type
typeOfBuiltin = \case
  BEInt64 _          -> pure TInt64
  BEDecimal _        -> pure TDecimal
  BEText    _        -> pure TText
  BETimestamp _      -> pure TTimestamp
  BEParty   _        -> pure TParty
  BEDate _           -> pure TDate
  BEEnumCon con      -> pure $ typeOfEnumCon con
  BEError            -> pure $ TForall (alpha, KStar) (TText :-> tAlpha)
  BEEqual     btype  -> pure $ tComparison btype
  BELess      btype  -> pure $ tComparison btype
  BELessEq    btype  -> pure $ tComparison btype
  BEGreater   btype  -> pure $ tComparison btype
  BEGreaterEq btype  -> pure $ tComparison btype
  BEToText    btype  -> pure $ TBuiltin btype :-> TText
  BEPartyToQuotedText -> pure $ TParty :-> TText
  BEPartyFromText    -> pure $ TText :-> TOptional TParty
  BEAddDecimal       -> pure $ tBinop TDecimal
  BESubDecimal       -> pure $ tBinop TDecimal
  BEMulDecimal       -> pure $ tBinop TDecimal
  BEDivDecimal       -> pure $ tBinop TDecimal
  BERoundDecimal     -> pure $ TInt64 :-> TDecimal :-> TDecimal
  BEAddInt64         -> pure $ tBinop TInt64
  BESubInt64         -> pure $ tBinop TInt64
  BEMulInt64         -> pure $ tBinop TInt64
  BEDivInt64         -> pure $ tBinop TInt64
  BEModInt64         -> pure $ tBinop TInt64
  BEExpInt64         -> pure $ tBinop TInt64
  BEInt64ToDecimal   -> pure $ TInt64 :-> TDecimal
  BEDecimalToInt64   -> pure $ TDecimal :-> TInt64
  BEExplodeText      -> pure $ TText :-> TList TText
  BEAppendText       -> pure $ tBinop TText
  BEImplodeText      -> pure $ TList TText :-> TText
  BESha256Text       -> do
      v <- view lfVersion
      if supportsSha256Text v
          then pure $ TText :-> TText
          else throwWithContext $ EUnsupportedBuiltin BESha256Text version1_2
  BEFoldl -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $
             (tBeta :-> tAlpha :-> tBeta) :-> tBeta :-> TList tAlpha :-> tBeta
  BEFoldr -> pure $ TForall (alpha, KStar) $ TForall (beta, KStar) $
             (tAlpha :-> tBeta :-> tBeta) :-> tBeta :-> TList tAlpha :-> tBeta
  BEMapEmpty  -> pure $ TForall (alpha, KStar) $ TMap tAlpha
  BEMapInsert -> pure $ TForall (alpha, KStar) $ TText :-> tAlpha :-> TMap tAlpha :-> TMap tAlpha
  BEMapLookup -> pure $ TForall (alpha, KStar) $ TText :-> TMap tAlpha :-> TOptional tAlpha
  BEMapDelete -> pure $ TForall (alpha, KStar) $ TText :-> TMap tAlpha :-> TMap tAlpha
  BEMapToList -> pure $ TForall (alpha, KStar) $ TMap tAlpha :-> TList (TMapEntry tAlpha)
  BEMapSize   -> pure $ TForall (alpha, KStar) $ TMap tAlpha :-> TInt64
  BEEqualList -> pure $
    TForall (alpha, KStar) $
    (tAlpha :-> tAlpha :-> TBool) :-> TList tAlpha :-> TList tAlpha :-> TBool
  BETimestampToUnixMicroseconds -> pure $ TTimestamp :-> TInt64
  BEUnixMicrosecondsToTimestamp -> pure $ TInt64 :-> TTimestamp
  BEDateToUnixDays -> pure $ TDate :-> TInt64
  BEUnixDaysToDate -> pure $ TInt64 :-> TDate
  BETrace -> pure $ TForall (alpha, KStar) $ TText :-> tAlpha :-> tAlpha
  BEEqualContractId -> pure $
    TForall (alpha, KStar) $
    TContractId tAlpha :-> TContractId tAlpha :-> TBool
  where
    tComparison btype = TBuiltin btype :-> TBuiltin btype :-> TBool
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
  conArgType <- match _Just (EUnknownVariantCon con) (con `lookup` variantType)
  checkExpr conArg conArgType

typeOfRecProj :: MonadGamma m => TypeConApp -> FieldName -> Expr -> m Type
typeOfRecProj typ0 field record = do
  dataCons <- checkTypeConApp typ0
  recordType <- match _DataRecord (EExpectedRecordType typ0) dataCons
  fieldType <- match _Just (EUnknownField field) (lookup field recordType)
  checkExpr record (typeConAppToType typ0)
  pure fieldType

typeOfRecUpd :: MonadGamma m => TypeConApp -> FieldName -> Expr -> Expr -> m Type
typeOfRecUpd typ0 field record update = do
  dataCons <- checkTypeConApp typ0
  recordType <- match _DataRecord (EExpectedRecordType typ0) dataCons
  fieldType <- match _Just (EUnknownField field) (lookup field recordType)
  let typ1 = typeConAppToType typ0
  checkExpr record typ1
  checkExpr update fieldType
  pure typ1

typeOfTupleCon :: MonadGamma m => [(FieldName, Expr)] -> m Type
typeOfTupleCon recordExpr = do
  checkUnique EDuplicateField (map fst recordExpr)
  TTuple <$> (traverse . _2) typeOf recordExpr

typeOfTupleProj :: MonadGamma m => FieldName -> Expr -> m Type
typeOfTupleProj field expr = do
  typ <- typeOf expr
  tupleType <- match _TTuple (EExpectedTupleType typ) typ
  match _Just (EUnknownField field) (lookup field tupleType)

typeOfTupleUpd :: MonadGamma m => FieldName -> Expr -> Expr -> m Type
typeOfTupleUpd field tuple update = do
  typ <- typeOf tuple
  tupleType <- match _TTuple (EExpectedTupleType typ) typ
  fieldType <- match _Just (EUnknownField field) (lookup field tupleType)
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
typeOfTyLam (tvar, kind) expr = TForall (tvar, kind) <$> introTypeVar tvar kind (typeOf expr)

introCasePattern :: MonadGamma m => Type -> CasePattern -> m a -> m a
introCasePattern scrutType patn cont = case patn of
  CPVariant patnTCon con var -> do
    DefDataType _loc _name _serializable tparams dataCons <- inWorld (lookupDataType patnTCon)
    variantCons <- match _DataVariant (EExpectedVariantType patnTCon) dataCons
    conArgType <-
      match _Just (EUnknownVariantCon con) (con `lookup` variantCons)
    (scrutTCon, scrutTArgs) <-
      match _TConApp (EExpectedDataType scrutType) scrutType
    unless (scrutTCon == patnTCon) $
      throwWithContext (ETypeConMismatch patnTCon scrutTCon)
    -- NOTE(MH): The next line should never throw since @scrutTApp@ has passed
    -- 'checkTypeConApp'. The call to 'substitute' is hence safe for the same
    -- reason as in 'checkTypeConApp'.
    subst0 <-
      match _Just (ETypeConAppWrongArity (TypeConApp scrutTCon scrutTArgs)) (zipExactMay tparams scrutTArgs)
    let subst1 = map (\((v, _k), t) -> (v, t)) subst0
    let varType = substitute (Map.fromList subst1) conArgType
    introExprVar var varType cont
  CPEnumCon con
    | alphaEquiv scrutType conType -> cont
    | otherwise ->
        throwWithContext ETypeMismatch{foundType = scrutType, expectedType = conType, expr = Nothing}
    where conType = typeOfEnumCon con
  CPNil -> do
    _ :: Type <- match _TList (EExpectedListType scrutType) scrutType
    cont
  CPCons headVar tailVar -> do
    elemType <- match _TList (EExpectedListType scrutType) scrutType
    -- NOTE(MH): The second 'introExprVar' will catch the bad case @headVar ==
    -- tailVar@.
    introExprVar headVar elemType $ introExprVar tailVar (TList elemType) cont
  CPDefault -> cont
  CPSome bodyVar -> do
    bodyType <- match _TOptional (EExpectedOptionalType scrutType) scrutType
    introExprVar bodyVar bodyType cont
  CPNone -> do
    _ :: Type <- match _TOptional (EExpectedOptionalType scrutType) scrutType
    cont

typeOfCase :: MonadGamma m => Expr -> [CaseAlternative] -> m Type
typeOfCase _ [] = throwWithContext EEmptyCase
typeOfCase scrut (CaseAlternative patn0 rhs0:alts) = do
  scrutType <- typeOf scrut
  rhsType <- introCasePattern scrutType patn0 (typeOf rhs0)
  for_ alts $ \(CaseAlternative patn rhs) ->
    introCasePattern scrutType patn (checkExpr rhs rhsType)
  pure rhsType

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

typeOfExercise :: MonadGamma m =>
  Qualified TypeConName -> ChoiceName -> Expr -> Expr -> Expr -> m Type
typeOfExercise tpl chName cid actors arg = do
  choice <- inWorld (lookupChoice (tpl, chName))
  checkExpr cid (TContractId (TCon tpl))
  checkExpr actors (TList TParty)
  checkExpr arg (chcArgType choice)
  pure (TUpdate (chcReturnType choice))

checkFetch :: MonadGamma m => Qualified TypeConName -> Expr -> m ()
checkFetch tpl cid = do
  _ :: Template <- inWorld (lookupTemplate tpl)
  checkExpr cid (TContractId (TCon tpl))

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
  UExercise tpl choice cid actors arg -> typeOfExercise tpl choice cid actors arg
  UFetch tpl cid -> checkFetch tpl cid $> TUpdate (TCon tpl)
  UGetTime -> pure (TUpdate TTimestamp)
  UEmbedExpr typ e -> do
    checkExpr e (TUpdate typ)
    return (TUpdate typ)
  UFetchByKey retrieveByKey -> do
    (cidType, contractType) <- checkRetrieveByKey retrieveByKey
    return (TUpdate (TTuple [(Tagged "contractId", cidType), (Tagged "contract", contractType)]))
  ULookupByKey retrieveByKey -> do
    (cidType, _contractType) <- checkRetrieveByKey retrieveByKey
    return (TUpdate (TOptional cidType))

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

typeOf :: MonadGamma m => Expr -> m Type
typeOf = \case
  EVar var -> lookupExprVar var
  EVal val -> dvalType <$> inWorld (lookupValue val)
  EBuiltin bexpr -> typeOfBuiltin bexpr
  ERecCon typ recordExpr -> checkRecCon typ recordExpr $> typeConAppToType typ
  ERecProj typ field rec -> typeOfRecProj typ field rec
  ERecUpd typ field record update -> typeOfRecUpd typ field record update
  EVariantCon typ con arg -> checkVariantCon typ con arg $> typeConAppToType typ
  ETupleCon recordExpr -> typeOfTupleCon recordExpr
  ETupleProj field expr -> typeOfTupleProj field expr
  ETupleUpd field tuple update -> typeOfTupleUpd field tuple update
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
  EUpdate upd -> typeOfUpdate upd
  EScenario scen -> typeOfScenario scen
  ELocation _ expr -> typeOf expr

checkExpr' :: MonadGamma m => Expr -> Type -> m Type
checkExpr' expr typ = do
  exprType <- typeOf expr
  unless (alphaEquiv exprType typ) $
    throwWithContext ETypeMismatch{foundType = exprType, expectedType = typ, expr = Just expr}
  pure exprType

checkExpr :: MonadGamma m => Expr -> Type -> m ()
checkExpr expr typ = void (checkExpr' expr typ)

-- | Check that a type constructor definition is well-formed.
checkDefDataType :: MonadGamma m => DefDataType -> m ()
checkDefDataType (DefDataType _loc _name _serializable params dataCons) =
  -- NOTE(MH): Duplicates in the @params@ list are caught by 'introTypeVar'.
  foldr (uncurry introTypeVar) base params
  where
    base = case dataCons of
      DataRecord  fields  -> checkRecordType fields
      DataVariant (unzip -> (names, types)) -> do
        checkUnique EDuplicateVariantCon names
        traverse_ (`checkType` KStar) types

checkDefValue :: MonadGamma m => DefValue -> m ()
checkDefValue (DefValue _loc (_, typ) _noParties (IsTest isTest) expr) = do
  checkType typ KStar
  checkExpr expr typ
  when isTest $
    case view _TForalls typ of
      (_, TScenario _) -> pure ()
      _ -> throwWithContext (EExpectedScenarioType typ)

checkTemplateChoice :: MonadGamma m => Qualified TypeConName -> TemplateChoice -> m ()
checkTemplateChoice tpl (TemplateChoice _loc _ _ actors selfBinder (param, paramType) retType upd) = do
  checkType paramType KStar
  checkType retType KStar
  v <- view lfVersion
  let checkActors = checkExpr actors (TList TParty)
  if supportsDisjunctionChoices v
    then introExprVar param paramType checkActors
    else checkActors
  introExprVar selfBinder (TContractId (TCon tpl)) $ introExprVar param paramType $
    checkExpr upd (TUpdate retType)

checkTemplate :: MonadGamma m => Module -> Template -> m ()
checkTemplate m t@(Template _loc tpl param precond signatories observers text choices mbKey) = do
  let tcon = Qualified PRSelf (moduleName m) tpl
  DefDataType _loc _naem _serializable tparams dataCons <- inWorld (lookupDataType tcon)
  unless (null tparams) $ throwWithContext (EExpectedTemplatableType tpl)
  _ <- match _DataRecord (EExpectedTemplatableType tpl) dataCons
  introExprVar param (TCon tcon) $ do
    withPart TPPrecondition $ checkExpr precond TBool
    withPart TPSignatories $ checkExpr signatories (TList TParty)
    withPart TPObservers $ checkExpr observers (TList TParty)
    withPart TPAgreement $ checkExpr text TText
    for_ choices $ \c -> withPart (TPChoice c) $ checkTemplateChoice tcon c
  checkTemplateKey param tcon mbKey
  where
    withPart p = withContext (ContextTemplate m t p)

checkValidKeyExpr :: MonadGamma m => Expr -> m ()
checkValidKeyExpr = \case
  ERecCon _typ recordExpr -> do
      traverse_ (checkValidKeyExpr . snd) recordExpr
  expr ->
    checkValidProjectionsKey expr

checkValidProjectionsKey :: MonadGamma m => Expr -> m ()
checkValidProjectionsKey = \case
  EVar _var ->
    pure ()
  ERecProj _typ _field rec ->
    checkValidProjectionsKey rec
  expr ->
    throwWithContext (EInvalidKeyExpression expr)

checkTemplateKey :: MonadGamma m => ExprVarName -> Qualified TypeConName -> Maybe TemplateKey -> m ()
checkTemplateKey param tcon mbKey =
  for_ mbKey $ \TemplateKey{..} -> do
    introExprVar param (TCon tcon) $ do
      checkType tplKeyType KStar
      checkValidKeyExpr tplKeyBody
      checkExpr tplKeyBody tplKeyType
    checkExpr tplKeyMaintainers (tplKeyType :-> TList TParty)

-- NOTE(MH): It is important that the data type definitions are checked first.
-- The type checker for expressions relies on the fact that data type
-- definitions do _not_ contain free variables.
checkModule :: MonadGamma m => Module -> m ()
checkModule m@(Module _modName _path _flags  dataTypes values templates) = do
  let with ctx f x = withContext (ctx x) (f x)
  traverse_ (with (ContextDefDataType m) checkDefDataType) dataTypes
  traverse_ (with (\t -> ContextTemplate m t TPWhole) $ checkTemplate m) templates
  traverse_ (with (ContextDefValue m) checkDefValue) values
