-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}

-- | Constraint generator for DAML LF static verification
module DA.Daml.LF.Verify.Generate
  ( genPackages
  , genExpr
  , Output(..)
  , Phase(..)
  , GenPhase
  ) where

import Control.Monad.Error.Class (MonadError (..), throwError)
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.HashMap.Strict as HM
import qualified Data.NameMap as NM

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Context
import DA.Daml.LF.Verify.Subst

-- | Data type denoting the output of the constraint generator.
data Output (ph :: Phase) = Output
  { _oExpr :: Expr
    -- ^ The expression, evaluated as far as possible.
  , _oUpdate :: UpdateSet ph
    -- ^ The updates, performed by this expression.
  }

-- | Construct an output with no updates.
emptyOut :: IsPhase ph
  => Expr
  -- ^ The evaluated expression.
  -> Output ph
emptyOut expr = Output expr emptyUpdateSet

-- | Extend a generator output with the updates of the second generator output.
-- Note that the end result will contain only the first expression.
combineOut :: IsPhase ph => Output ph -> Output ph -> Output ph
combineOut out1 out2 = extendOutUpds (_oUpdate out2) out1

-- | Update an output with a new evaluated expression.
updateOutExpr :: Expr
  -- ^ The new output expression.
  -> Output ph
  -- ^ The generator output to be updated.
  -> Output ph
updateOutExpr expr out = out{_oExpr = expr}

-- | Update an output with additional updates.
extendOutUpds :: IsPhase ph
  => UpdateSet ph
  -- ^ The extension of the update set.
  -> Output ph
  -- ^ The generator output to be updated.
  -> Output ph
extendOutUpds upds out@Output{..} = out{_oUpdate = concatUpdateSet upds _oUpdate}

-- | Update an output with an additional Archive update.
addArchiveUpd :: Qualified TypeConName
  -- ^ The template to be archived.
  -> [(FieldName, Expr)]
  -- ^ The fields to be archived, with their respective values.
  -> Output 'ChoiceGathering
  -- ^ The generator output to be updated.
  -> Output 'ChoiceGathering
addArchiveUpd temp fs (Output expr upds) =
  Output expr (addUpd upds $ UpdArchive temp fs)

-- | Class containing the generator methods for different generator phases.
class IsPhase ph => GenPhase (ph :: Phase) where
  -- | Generate an environment for a given module.
  -- Depending on the generator phase, this either adds all value and data type
  -- definitions to the environment, or all template definitions with their
  -- respective choices.
  genModule :: MonadEnv m ph
    => PackageRef
    -- ^ A reference to the package in which this module is defined.
    -> Module
    -- ^ The module to analyse.
    -> m ()

  -- | Analyse a value reference.
  genForVal :: MonadEnv m ph
    => Qualified ExprValName
    -- ^ The value reference to be analysed.
    -> m (Output ph)

instance GenPhase 'ValueGathering where
  genModule pac mod = do
    extDatsEnv (HM.map (instPRSelf pac) (NM.toHashMap (moduleDataTypes mod)))
    mapM_ (genValue pac (moduleName mod)) (NM.toList $ moduleValues mod)
  genForVal w = return $ Output (EVal w) (setUpdSetValues [Determined w] emptyUpdateSet)

instance GenPhase 'ChoiceGathering where
  genModule pac mod =
    mapM_ (genTemplate pac (moduleName mod)) (NM.toList $ moduleTemplates mod)
  genForVal w = lookupVal w >>= \case
    Just (expr, upds) -> return (Output expr upds)
    Nothing -> throwError (UnknownValue w)

instance GenPhase 'Solving where
  genModule _pac _mod =
    error "Impossible: genModule can't be used in the solving phase"
  genForVal _w = error "Impossible: genForVal can't be used in the solving phase"

-- | Generate an environment for a given list of packages.
-- Depending on the generator phase, this either adds all value and data type
-- definitions to the environment, or all template definitions with their
-- respective choices.
genPackages :: (GenPhase ph, MonadEnv m ph)
  => [(PackageId, (Package, Maybe PackageName))]
  -- ^ The list of packages, as produced by `readPackages`.
  -> m ()
genPackages inp = mapM_ genPackage inp

-- | Generate an environment for a given package.
-- Depending on the generator phase, this either adds all value and data type
-- definitions to the environment, or all template definitions with their
-- respective choices.
genPackage :: (GenPhase ph, MonadEnv m ph)
  => (PackageId, (Package, Maybe PackageName))
  -- ^ The package, as produced by `readPackages`.
  -> m ()
genPackage (id, (pac, _)) = mapM_ (genModule (PRImport id)) (NM.toList $ packageModules pac)

-- | Analyse a value definition and add to the environment.
genValue :: (GenPhase ph, MonadEnv m ph)
  => PackageRef
  -- ^ A reference to the package in which this value is defined.
  -> ModuleName
  -- ^ The name of the module in which this value is defined.
  -> DefValue
  -- ^ The value to be analysed and added.
  -> m ()
genValue pac mod val = do
  expOut <- genExpr True (instPRSelf pac $ dvalBody val)
  let qname = Qualified pac mod (fst $ dvalBinder val)
  extValEnv qname (_oExpr expOut) (_oUpdate expOut)

-- | Analyse a choice definition and add to the environment.
-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadEnv m 'ChoiceGathering
  => PackageRef
  -- ^ A reference to the package in which this choice is defined.
  -> Qualified TypeConName
  -- ^ The template in which this choice is defined.
  -> ExprVarName
  -- ^ The `this` variable referencing the contract on which this choice is
  -- called.
  -> [(FieldName, Type)]
  -- ^ The list of fields available in the template.
  -> TemplateChoice
  -- ^ The choice to be analysed and added.
  -> m ()
genChoice pac tem this' temFs TemplateChoice{..} = do
  -- Get the current variable names.
  let self' = chcSelfBinder
      arg' = fst chcArgBinder
  -- Refresh the variable names and extend the environment.
  self <- genRenamedVar self'
  arg <- genRenamedVar arg'
  this <- genRenamedVar this'
  extVarEnv self
  extVarEnv arg
  extVarEnv this
  -- Extend the environment with any record fields from the arguments.
  argFs <- recTypFields (snd chcArgBinder) >>= \case
    Nothing -> throwError ExpectRecord
    Just fs -> return fs
  extRecEnv arg $ map fst argFs
  extRecEnvTCons argFs
  -- Extend the environment with any record fields from the template.
  extRecEnv this $ map fst temFs
  extRecEnvTCons temFs
  -- Extend the environment with any contract id's and constraints from the arguments.
  extEnvContractTCons argFs arg Nothing
  -- Extend the environment with any contract id's and constraints from the template.
  extEnvContract (TCon tem) (EVar self) (Just this)
  extEnvContractTCons temFs this (Just this)
  -- Substitute the renamed variables, and the `Self` package id.
  -- Run the constraint generator on the resulting choice expression.
  let substVar = createExprSubst [(self',EVar self),(this',EVar this),(arg',EVar arg)]
  -- TODO: Can preconditions perform updates?
  expOut <- genExpr True
    $ substituteTm substVar
    $ instPRSelf pac chcUpdate
  -- Add the archive update.
  let out = if chcConsuming
        then addArchiveUpd tem (fields this) expOut
        else expOut
  -- Extend the environment with the choice and its resulting updates.
  extChEnv tem chcName self this arg (_oUpdate out) chcReturnType
  where
    fields this = map (\(f,_) -> (f, ERecProj (TypeConApp tem []) f (EVar this))) temFs

-- | Analyse a template definition and add all choices to the environment.
genTemplate :: MonadEnv m 'ChoiceGathering
  => PackageRef
  -- ^ A reference to the package in which this template is defined.
  -> ModuleName
  -- ^ The module in which this template is defined.
  -> Template
  -- ^ The template to be analysed and added.
  -> m ()
genTemplate pac mod Template{..} = do
  let name = Qualified pac mod tplTypeCon
  fields <- recTypConFields tplTypeCon >>= \case
    Nothing -> throwError ExpectRecord
    Just fs -> return fs
  let preCond (this :: Expr) =
        substituteTm (singleExprSubst tplParam this)
        (instPRSelf pac tplPrecondition)
  extPrecond name preCond
  mapM_ (genChoice pac name tplParam fields)
    (archive : NM.toList tplChoices)
  where
    archive :: TemplateChoice
    archive = TemplateChoice Nothing (ChoiceName "Archive") True
      (ENil (TBuiltin BTParty)) (ExprVarName "self")
      (ExprVarName "arg", TStruct []) (TBuiltin BTUnit)
      (EUpdate $ UPure (TBuiltin BTUnit) (EBuiltin BEUnit))

-- | Analyse an expression, and produce an Output storing its (partial)
-- evaluation result and the set of performed updates.
genExpr :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> Expr
  -- ^ The expression to be analysed.
  -> m (Output ph)
genExpr updFlag = \case
  ETmApp fun arg -> genForTmApp updFlag fun arg
  ETyApp expr typ -> genForTyApp updFlag expr typ
  ELet bind body -> genForLet updFlag bind body
  EVar name -> genForVar name
  EVal w -> genForVal w
  ERecProj tc f e -> genForRecProj updFlag tc f e
  EStructProj f e -> genForStructProj updFlag f e
  ELocation _ expr -> genExpr updFlag expr
  ECase e cs -> genForCase updFlag e cs
  EUpdate upd -> if updFlag
    then do
      (out, _, _) <- genUpdate upd
      return out
    else return $ emptyOut $ EUpdate upd
  -- TODO: Remove?
  -- EUpdate (UPure typ expr) -> do
  --   out <- genExpr updFlag expr
  --   return $ updateOutExpr (EUpdate $ UPure typ (_oExpr out)) out
  -- EUpdate (UBind bind expr) -> if updFlag
  --   then do
  --     (out, _, _) <- genForBind bind expr
  --     return out
  --   else return $ emptyOut $ EUpdate (UBind bind expr)
  -- TODO: Extend additional cases
  e -> return $ emptyOut e

-- | Analyse an update expression, and produce both an Output, its return type
-- and potentially the field values of any created contracts.
genUpdate :: (GenPhase ph, MonadEnv m ph)
  => Update
  -- ^ The update expression to be analysed.
  -> m (Output ph, Type, Maybe Expr)
genUpdate = \case
  UBind bind expr -> genForBind bind expr
  UPure typ expr -> do
    out <- genExpr True expr
    let out' = updateOutExpr (EUpdate $ UPure typ (_oExpr out)) out
    return (out', typ, Nothing)
  UCreate tem arg -> genForCreate tem arg
  UExercise tem ch cid par arg -> genForExercise tem ch cid par arg
  UGetTime -> return (emptyOut (EUpdate UGetTime), TBuiltin BTTimestamp, Nothing)
  -- TODO: Extend additional cases
  u -> error ("Update not implemented yet: " ++ show u)

-- | Analyse a term application expression.
genForTmApp :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> Expr
  -- ^ The function expression.
  -> Expr
  -- ^ The argument expression.
  -> m (Output ph)
genForTmApp updFlag fun arg = do
  funOut <- genExpr updFlag fun
  arout <- genExpr updFlag arg
  case _oExpr funOut of
    -- TODO: Should we rename here?
    ETmLam bndr body -> do
      let subst = singleExprSubst (fst bndr) (_oExpr arout)
          resExpr = substituteTm subst body
      resOut <- genExpr updFlag resExpr
      return $ combineOut resOut
        $ combineOut funOut arout
    fun' -> return $ updateOutExpr (ETmApp fun' (_oExpr arout))
      $ combineOut funOut arout

-- | Analyse a type application expression.
genForTyApp :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> Expr
  -- ^ The function expression.
  -> Type
  -- ^ The argument type.
  -> m (Output ph)
genForTyApp updFlag expr typ = do
  exprOut <- genExpr updFlag expr
  case _oExpr exprOut of
    ETyLam bndr body -> do
      let subst = singleTypeSubst (fst bndr) typ
          resExpr = substituteTy subst body
      resOut <- genExpr updFlag resExpr
      return $ combineOut resOut exprOut
    expr' -> return $ updateOutExpr (ETyApp expr' typ) exprOut

-- | Analyse a let binding expression.
genForLet :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> Binding
  -- ^ The binding to be bound.
  -> Expr
  -- ^ The expression in which the binding should be available.
  -> m (Output ph)
genForLet updFlag bind body = do
  bindOut <- genExpr False (bindingBound bind)
  let var = fst $ bindingBinder bind
      subst = singleExprSubst var (_oExpr bindOut)
      resExpr = substituteTm subst body
  resOut <- genExpr updFlag resExpr
  return $ combineOut resOut bindOut

-- | Analyse an expression variable.
genForVar :: (GenPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The expression variable to be analysed.
  -> m (Output ph)
genForVar name = lookupVar name >> return (emptyOut (EVar name))

-- | Analyse a record projection expression.
genForRecProj :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> TypeConApp
  -- ^ The type constructor of the record which is projected.
  -> FieldName
  -- ^ The field which is projected.
  -> Expr
  -- ^ The record expression which is projected.
  -> m (Output ph)
genForRecProj updFlag tc f body = do
  bodyOut <- genExpr updFlag body
  case _oExpr bodyOut of
    -- TODO: I think we can reduce duplication a bit more here
    EVar x -> do
      skol <- lookupRec x f
      if skol
        then return $ updateOutExpr (ERecProj tc f (EVar x)) bodyOut
        else error ("Impossible: expected skolem record: " ++ show x ++ "." ++ show f)
    expr -> do
      recExpFields expr >>= \case
        Just fs -> case lookup f fs of
          -- TODO: Include the bodyOut updates.
          Just expr -> genExpr updFlag expr
          Nothing -> throwError $ UnknownRecField f
        Nothing -> return $ updateOutExpr (ERecProj tc f expr) bodyOut

-- | Analyse a struct projection expression.
genForStructProj :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> FieldName
  -- ^ The field which is projected.
  -> Expr
  -- ^ The record expression which is projected.
  -> m (Output ph)
genForStructProj updFlag f body = do
  bodyOut <- genExpr updFlag body
  case _oExpr bodyOut of
    -- TODO: I think we can reduce duplication a bit more here
    EVar x -> do
      skol <- lookupRec x f
      if skol
        then return $ updateOutExpr (EStructProj f (EVar x)) bodyOut
        else error ("Impossible: expected skolem record: " ++ show x ++ "." ++ show f)
    expr -> do
      recExpFields expr >>= \case
        Just fs -> case lookup f fs of
          -- TODO: Include the bodyOut updates.
          Just expr -> genExpr updFlag expr
          Nothing -> throwError $ UnknownRecField f
        Nothing -> return $ updateOutExpr (EStructProj f expr) bodyOut

-- | Analyse a case expression.
-- TODO: Atm only boolean cases are supported
genForCase :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether to analyse update expressions.
  -> Expr
  -- ^ The expression to match on.
  -> [CaseAlternative]
  -- ^ The list of alternatives.
  -> m (Output ph)
genForCase updFlag exp cs = do
  expOut <- genExpr updFlag exp
  case findBool True of
    Just tru -> do
      truOut <- genExpr updFlag tru
      case findBool False of
        Just fal -> do
          falOut <- genExpr updFlag fal
          let resExp = ECase (_oExpr expOut)
                [ CaseAlternative (CPBool True) (_oExpr truOut)
                , CaseAlternative (CPBool False) (_oExpr falOut) ]
              resUpd = _oUpdate expOut `concatUpdateSet`
                conditionalUpdateSet (_oExpr expOut) (_oUpdate truOut) (_oUpdate falOut)
          return $ Output resExp resUpd
        Nothing -> error "Impossible: Missing False-case in if statement"
    Nothing -> return $ emptyOut (ECase exp cs)
  where
    findBool :: Bool -> Maybe Expr
    findBool b1 = listToMaybe $ [e | CaseAlternative (CPBool b2) e <- cs, b1 == b2]

-- | Analyse a create update expression.
-- Returns both the generator output and the return type.
genForCreate :: (GenPhase ph, MonadEnv m ph)
  => Qualified TypeConName
  -- ^ The template of which a new instance is being created.
  -> Expr
  -- ^ The argument expression.
  -> m (Output ph, Type, Maybe Expr)
genForCreate tem arg = do
  arout <- genExpr True arg
  recExpFields (_oExpr arout) >>= \case
    Just fs -> do
      fsEval <- mapM partial_eval_field fs
      return ( Output (EUpdate (UCreate tem $ _oExpr arout)) $ addUpd emptyUpdateSet (UpdCreate tem fsEval)
             , TCon tem
             , Just $ EStructCon fsEval )
    Nothing -> throwError ExpectRecord
  where
    partial_eval_field :: (GenPhase ph, MonadEnv m ph)
      => (FieldName, Expr)
      -> m (FieldName, Expr)
    partial_eval_field (f,e) = do
      e' <- genExpr False e
      return (f,_oExpr e')

-- | Analyse an exercise update expression.
-- Returns both the generator output and the return type of the choice.
genForExercise :: (GenPhase ph, MonadEnv m ph)
  => Qualified TypeConName
  -- ^ The template on which a choice is being exercised.
  -> ChoiceName
  -- ^ The choice which is being exercised.
  -> Expr
  -- ^ The contract id on which the choice is being exercised.
  -> Maybe Expr
  -- ^ The party which exercises the choice.
  -> Expr
  -- ^ The arguments with which the choice is being exercised.
  -> m (Output ph, Type, Maybe Expr)
genForExercise tem ch cid par arg = do
  cidOut <- genExpr True cid
  arout <- genExpr True arg
  (updSubst, resType) <- lookupChoice tem ch
  this <- fst <$> lookupCid (_oExpr cidOut)
  -- TODO: Should we further eval after subst? But how to eval an update set?
  let updSet = updSubst (_oExpr cidOut) (EVar this) (_oExpr arout)
  return ( Output (EUpdate (UExercise tem ch (_oExpr cidOut) par (_oExpr arout))) updSet
         , resType
         , Nothing ) -- TODO!

-- | Analyse a bind update expression.
-- Returns both the generator output and the return type.
genForBind :: (GenPhase ph, MonadEnv m ph)
  => Binding
  -- ^ The binding being bound with this update.
  -> Expr
  -- ^ The expression in which this binding is being made available.
  -> m (Output ph, Type, Maybe Expr)
genForBind bind body = do
  bindOut <- genExpr False (bindingBound bind)
  (bindUpd, subst) <- case _oExpr bindOut of
    EUpdate (UFetch tc cid) -> do
      let var0 = fst $ bindingBinder bind
      var1 <- genRenamedVar var0
      let subst = singleExprSubst var0 (EVar var1)
      _ <- bindCids False (TContractId (TCon tc)) cid (EVar var1) Nothing
      return (emptyUpdateSet, subst)
    EUpdate upd -> do
      (updOut, updTyp, creFs) <- genUpdate upd
      this <- genRenamedVar (ExprVarName "this")
      subst <- bindCids True updTyp (EVar $ fst $ bindingBinder bind) (EVar this) creFs
      return (_oUpdate updOut, subst)
    _ -> return (emptyUpdateSet, emptyExprSubst)
  extVarEnv (fst $ bindingBinder bind)
  bodyOut <- genExpr False $ substituteTm subst body
  case _oExpr bodyOut of
    EUpdate bodyUpd -> do
      (bodyUpdOut, bodyTyp, creFs) <- genUpdate bodyUpd
      return ( Output
                 (_oExpr bodyUpdOut)
                 (_oUpdate bindOut
                   `concatUpdateSet` bindUpd
                   `concatUpdateSet` _oUpdate bodyOut
                   `concatUpdateSet` _oUpdate bodyUpdOut)
             , bodyTyp
             , creFs )
    -- TODO: Temporary hack
    expr -> do
      let bodyUpd = UPure (TBuiltin BTUnit) expr
      (bodyUpdOut, bodyTyp, creFs) <- genUpdate bodyUpd
      return ( Output
                 (_oExpr bodyUpdOut)
                 (_oUpdate bindOut
                   `concatUpdateSet` bindUpd
                   `concatUpdateSet` _oUpdate bodyOut
                   `concatUpdateSet` _oUpdate bodyUpdOut)
             , bodyTyp
             , creFs )
    -- _ -> error "Impossible: The body of a bind should be an update expression"

-- | Refresh and bind the fetched contract id to the given variable. Returns a
-- substitution, mapping the old id to the refreshed one.
bindCids :: (GenPhase ph, MonadEnv m ph)
  => Bool
  -- ^ Flag denoting whether the contract id's should be refreshed.
  -- Note that even with the flag on, contract id's are only refreshed on their
  -- first encounter.
  -> Type
  -- ^ The type of the contract id's being bound.
  -> Expr
  -- ^ The contract id's being bound.
  -> Expr
  -- ^ The variables to bind them to.
  -> Maybe Expr
  -- ^ The field values for any created contracts, if available.
  -> m ExprSubst
-- TODO: combine these two cases.
bindCids b (TContractId (TCon tc)) cid (EVar this) fsExpM = do
  fs <- recTypConFields (qualObject tc) >>= \case
    Nothing -> throwError ExpectRecord
    Just fs -> return fs
  extRecEnv this (map fst fs)
  subst <- extCidEnv b cid this
  case fsExpM of
    Just fsExp -> do
      fsOut <- genExpr False $ substituteTm subst fsExp
      recExpFields (_oExpr fsOut) >>= \case
        Just fields -> do
          extCtrRec this fields
          return subst
        Nothing -> throwError ExpectRecord
    Nothing -> return subst
bindCids b (TCon tc) cid (EVar this) fsExpM = do
  fs <- recTypConFields (qualObject tc) >>= \case
    Nothing -> throwError ExpectRecord
    Just fs -> return fs
  extRecEnv this (map fst fs)
  subst <- extCidEnv b cid this
  case fsExpM of
    Just fsExp -> do
      fsOut <- genExpr False $ substituteTm subst fsExp
      recExpFields (_oExpr fsOut) >>= \case
        Just fields -> do
          extCtrRec this fields
          return subst
        Nothing -> throwError ExpectRecord
    Nothing -> return subst
bindCids b (TApp (TApp (TCon con) t1) t2) cid var fsExpM =
  case head $ unTypeConName $ qualObject con of
    "Tuple2" -> do
      -- TODO: Test this part more extensively.
      -- cidOut <- genExpr True cid
      subst1 <- bindCids b t1 (EStructProj (FieldName "_1") cid) var fsExpM
      subst2 <- bindCids b t2 (substituteTm subst1 $ EStructProj (FieldName "_2") cid) var fsExpM
      return (subst1 `concatExprSubst` subst2)
    con' -> error ("Binding contract id's for this constructor has not been implemented yet: " ++ show con')
bindCids _ (TBuiltin BTUnit) _ _ _ = return emptyExprSubst
bindCids _ (TBuiltin BTTimestamp) _ _ _ = return emptyExprSubst
-- TODO: Extend additional cases, like tuples.
bindCids _ typ _ _ _ =
  error ("Binding contract id's for this particular type has not been implemented yet: " ++ show typ)

-- TODO: It would be much nicer to have these functions in Context.hs, but they
-- require the `GenPhase` monad. Find a nicer way to fix this.
-- | Lookup the preconditions for a given type, and instantiate the `this`
-- variable.
lookupPreconds :: (GenPhase ph, MonadEnv m ph)
  => Type
  -- ^ The type to lookup.
  -> ExprVarName
  -- ^ The variable for `this` to instantiate with.
  -> m (Maybe Expr)
lookupPreconds typ this = case typ of
  (TCon tem) -> do
    preconds <- envPreconds <$> getEnv
    case HM.lookup tem preconds of
      Nothing -> return Nothing
      Just preFunc -> do
        preOut <- genExpr False (preFunc $ EVar this)
        return $ Just $ _oExpr preOut
  _ -> return Nothing

-- | Bind the given contract id, and add any required additional constraints to
-- the environment.
-- TODO: Combine this with `bindCids`?
extEnvContract :: (GenPhase ph, MonadEnv m ph)
  => Type
  -- ^ The contract id type to add to the environment.
  -> Expr
  -- ^ The contract id being bound.
  -> Maybe ExprVarName
  -- ^ The variable to bind the contract id to, if available.
  -> m ()
extEnvContract (TContractId typ) cid bindM = extEnvContract typ cid bindM
extEnvContract (TCon tem) cid bindM = do
  bindV <- genRenamedVar (ExprVarName "var")
  let bind = fromMaybe bindV bindM
  _ <- bindCids False (TCon tem) cid (EVar bind) Nothing
  lookupPreconds (TCon tem) bind >>= \case
    Nothing -> return ()
    Just precond -> do
      precondOut <- genExpr False precond
      extCtr (_oExpr precondOut)
extEnvContract _ _ _ = return ()

-- | Bind any contract id's, together with their constraints, for the fields of
-- any given record or type constructor type.
extEnvContractTCons :: (GenPhase ph, MonadEnv m ph)
  => [(FieldName, Type)]
  -- ^ The record fields to bind, together with their types.
  -> ExprVarName
  -- ^ The variable name to project from, e.g. the `args` in `args.field`.
  -> Maybe ExprVarName
  -- ^ The variable to bind the contract id's to, if available.
  -> m ()
extEnvContractTCons fields var bind = mapM_ step fields
  where
    step :: (GenPhase ph, MonadEnv m ph)
      => (FieldName, Type) -> m ()
    step (f,typ) = extEnvContract typ (EStructProj f (EVar var)) bind
