-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Constraint generator for DAML LF static verification
module DA.Daml.LF.Verify.Generate
  ( genPackages
  , Phase(..)
  ) where

import Control.Monad.Error.Class (catchError, throwError)
import qualified Data.NameMap as NM
import Debug.Trace

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Context
import DA.Daml.LF.Verify.Subst

-- | Data type denoting the phase of the constraint generator.
data Phase
  = ValuePhase
  -- ^ The value phase gathers all value and data type definitions across modules.
  | TemplatePhase
  -- ^ The template phase gathers the updates performed by choices.

-- | Data type denoting the output of the constraint generator.
data Output = Output
  { _oExpr :: Expr
    -- ^ The expression, evaluated as far as possible.
  , _oUpdate :: UpdateSet
    -- ^ The updates, performed by this expression.
  }

-- | Construct an output with no updates.
emptyOut :: Expr
  -- ^ The evaluated expression.
  -> Output
emptyOut expr = Output expr emptyUpdateSet

-- | Extend a generator output with the updates of the second generator output.
-- Note that the end result will contain only the first expression.
combineOut :: Output -> Output -> Output
combineOut out1 out2 = extendOutUpds (_oUpdate out2) out1

-- | Update an output with a new evaluated expression.
updateOutExpr :: Expr
  -- ^ The new output expression.
  -> Output
  -- ^ The generator output to be updated.
  -> Output
updateOutExpr expr out = out{_oExpr = expr}

-- | Update an output with additional updates.
extendOutUpds :: UpdateSet
  -- ^ The extension of the update set.
  -> Output
  -- ^ The generator output to be updated.
  -> Output
extendOutUpds upds out@Output{..} = out{_oUpdate = concatUpdateSet upds _oUpdate}

-- | Update an output with an additional Archive update.
addArchiveUpd :: Qualified TypeConName
  -- ^ The template to be archived.
  -> [(FieldName, Expr)]
  -- ^ The fields to be archived, with their respective values.
  -> Output
  -- ^ The generator output to be updated.
  -> Output
addArchiveUpd temp fs (Output expr upd@UpdateSet{..}) =
  Output expr upd{_usArc = UpdArchive temp fs : _usArc}

-- | Generate an environment for a given list of packages.
-- Depending on the generator phase, this either adds all value and data type
-- definitions to the environment, or all template definitions with their
-- respective choices.
genPackages :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> [(PackageId, (Package, Maybe PackageName))]
  -- ^ The list of packages, as produced by `readPackages`.
  -> m ()
genPackages ph inp = mapM_ (genPackage ph) inp

-- | Generate an environment for a given package.
-- Depending on the generator phase, this either adds all value and data type
-- definitions to the environment, or all template definitions with their
-- respective choices.
genPackage :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> (PackageId, (Package, Maybe PackageName))
  -- ^ The package, as produced by `readPackages`.
  -> m ()
genPackage ph (id, (pac, _)) = mapM_ (genModule ph (PRImport id)) (NM.toList $ packageModules pac)

-- | Generate an environment for a given module.
-- Depending on the generator phase, this either adds all value and data type
-- definitions to the environment, or all template definitions with their
-- respective choices.
genModule :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> PackageRef
  -- ^ A reference to the package in which this module is defined.
  -> Module
  -- ^ The module to analyse.
  -> m ()
genModule ValuePhase pac mod = do
  extDatsEnv (NM.toHashMap (moduleDataTypes mod))
  mapM_ (genValue pac (moduleName mod)) (NM.toList $ moduleValues mod)
genModule TemplatePhase pac mod =
  mapM_ (genTemplate pac (moduleName mod)) (NM.toList $ moduleTemplates mod)

-- | Analyse a value definition and add to the environment.
genValue :: MonadEnv m
  => PackageRef
  -- ^ A reference to the package in which this value is defined.
  -> ModuleName
  -- ^ The name of the module in which this value is defined.
  -> DefValue
  -- ^ The value to be analysed and added.
  -> m ()
genValue pac mod val = do
  expOut <- genExpr ValuePhase (dvalBody val)
  let qname = Qualified pac mod (fst $ dvalBinder val)
  extValEnv qname (_oExpr expOut) (_oUpdate expOut)

-- | Analyse a choice definition and add to the environment.
-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadEnv m
  => Qualified TypeConName
  -- ^ The template in which this choice is defined.
  -> (ExprVarName,ExprVarName)
  -- ^ The original and renamed variable `this` referencing the contract on
  -- which this choice is called.
  -> [FieldName]
  -- ^ The list of fields available in the template.
  -> TemplateChoice
  -- ^ The choice to be analysed and added.
  -> m ()
genChoice tem (this',this) temFs TemplateChoice{..} = do
  let self' = chcSelfBinder
      arg' = fst chcArgBinder
  self <- genRenamedVar self'
  arg <- genRenamedVar arg'
  extVarEnv self
  extVarEnv arg
  argFs <- recTypFields (snd chcArgBinder)
  extRecEnv arg argFs
  expOut <- genExpr TemplatePhase
    $ substituteTmTm (createExprSubst [(self',EVar self),(this',EVar this),(arg',EVar arg)]) chcUpdate
  let out = if chcConsuming
        then addArchiveUpd tem fields expOut
        else expOut
  extChEnv tem chcName self this arg (_oUpdate out)
  where
    fields = map (\f -> (f, ERecProj (TypeConApp tem []) f (EVar this))) temFs

-- | Analyse a template definition and add all choices to the environment.
genTemplate :: MonadEnv m
  => PackageRef
  -- ^ A reference to the package in which this template is defined.
  -> ModuleName
  -- ^ The module in which this template is defined.
  -> Template
  -- ^ The template to be analysed and added.
  -> m ()
-- TODO: Take preconditions into account?
genTemplate pac mod Template{..} = do
  let name = Qualified pac mod tplTypeCon
  fields <- recTypConFields tplTypeCon
  let fs = map fst fields
  this <- genRenamedVar tplParam
  extVarEnv this
  extRecEnv this fs
  extRecEnvLvl1 fields
  mapM_ (genChoice name (tplParam,this) fs) (archive : NM.toList tplChoices)
  where
    archive :: TemplateChoice
    archive = TemplateChoice Nothing (ChoiceName "Archive") True
      (ENil (TBuiltin BTParty)) (ExprVarName "self")
      (ExprVarName "arg", TStruct []) (TBuiltin BTUnit)
      (EUpdate $ UPure (TBuiltin BTUnit) (EBuiltin BEUnit))

-- | Analyse an expression, and produce an Output storing its (partial)
-- evaluation result and the set of performed updates.
genExpr :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Expr
  -- ^ The expression to be analysed.
  -> m Output
genExpr ph = \case
  ETmApp fun arg -> genForTmApp ph fun arg
  ETyApp expr typ -> genForTyApp ph expr typ
  ELet bind body -> genForLet ph bind body
  EVar name -> genForVar ph name
  EVal w -> genForVal ph w
  ERecProj tc f e -> genForRecProj ph tc f e
  ELocation _ expr -> genExpr ph expr
  EUpdate (UCreate tem arg) -> genForCreate ph tem arg
  EUpdate (UExercise tem ch cid par arg) -> genForExercise ph tem ch cid par arg
  EUpdate (UBind bind expr) -> genForBind ph bind expr
  EUpdate (UPure _ expr) -> genExpr ph expr
  -- TODO: Extend additional cases
  e -> return $ emptyOut e

-- | Analyse a term application expression.
genForTmApp :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Expr
  -- ^ The function expression.
  -> Expr
  -- ^ The argument expression.
  -> m Output
genForTmApp ph fun arg = do
  funOut <- genExpr ph fun
  arout <- genExpr ph arg
  case _oExpr funOut of
    -- TODO: Should we rename here?
    ETmLam bndr body -> do
      let subst = singleExprSubst (fst bndr) (_oExpr arout)
          resExpr = substituteTmTm subst body
      resOut <- genExpr ph resExpr
      return $ combineOut resOut
        $ combineOut funOut arout
    fun' -> return $ updateOutExpr (ETmApp fun' (_oExpr arout))
      $ combineOut funOut arout

-- | Analyse a type application expression.
genForTyApp :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Expr
  -- ^ The function expression.
  -> Type
  -- ^ The argument type.
  -> m Output
genForTyApp ph expr typ = do
  exprOut <- genExpr ph expr
  case _oExpr exprOut of
    ETyLam bndr body -> do
      let subst = singleTypeSubst (fst bndr) typ
          resExpr = substituteTyTm subst body
      resOut <- genExpr ph resExpr
      return $ combineOut resOut exprOut
    expr' -> return $ updateOutExpr (ETyApp expr' typ) exprOut

-- | Analyse a let binding expression.
genForLet :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Binding
  -- ^ The binding to be bound.
  -> Expr
  -- ^ The expression in which the binding should be available.
  -> m Output
genForLet ph bind body = do
  bindOut <- genExpr ph (bindingBound bind)
  let subst = singleExprSubst (fst $ bindingBinder bind) (_oExpr bindOut)
      resExpr = substituteTmTm subst body
  resOut <- genExpr ph resExpr
  return $ combineOut resOut bindOut

-- | Analyse an expression variable.
genForVar :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> ExprVarName
  -- ^ The expression variable to be analysed.
  -> m Output
genForVar _ph name = lookupVar name >> return (emptyOut (EVar name))

-- | Analyse a value reference.
genForVal :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Qualified ExprValName
  -- ^ The value reference to be analysed.
  -> m Output
genForVal ValuePhase w
  = return $ Output (EVal w) (emptyUpdateSet{_usVal = [w]})
genForVal TemplatePhase w
  = lookupVal w >>= \ (expr, upds) -> return (Output expr upds)

-- | Analyse a record projection expression.
genForRecProj :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> TypeConApp
  -- ^ The type constructor of the record which is projected.
  -> FieldName
  -- ^ The field which is projected.
  -> Expr
  -- ^ The record expression which is projected.
  -> m Output
genForRecProj ph tc f body = do
  bodyOut <- genExpr ph body
  case _oExpr bodyOut of
    -- TODO: I think we can reduce duplication a bit more here
    EVar x -> do
      skol <- lookupRec x f
      if skol
        then return $ updateOutExpr (ERecProj tc f (EVar x)) bodyOut
        else error ("Impossible: expected skolem record: " ++ show x ++ "." ++ show f)
    expr -> do
      fs <- recExpFields expr
      case lookup f fs of
        Just expr -> genExpr ph expr
        Nothing -> throwError $ UnknownRecField f

-- | Analyse a create update expression.
genForCreate :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Qualified TypeConName
  -- ^ The template of which a new instance is being created.
  -> Expr
  -- ^ The argument expression.
  -> m Output
genForCreate ph tem arg = do
  arout <- genExpr ph arg
  fs <- recExpFields (_oExpr arout)
  return (Output (EUpdate (UCreate tem $ _oExpr arout)) emptyUpdateSet{_usCre = [UpdCreate tem fs]})
  -- TODO: We could potentially filter here to only store the interesting fields?

-- | Analyse an exercise update expression.
genForExercise :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Qualified TypeConName
  -- ^ The template on which a choice is being exercised.
  -> ChoiceName
  -- ^ The choice which is being exercised.
  -> Expr
  -- ^ The contract id on which the choice is being exercised.
  -> Maybe Expr
  -- ^ The party which exercises the choice.
  -> Expr
  -- ^ The arguments with which the choice is being exercised.
  -> m Output
genForExercise ph tem ch cid par arg = do
  cidOut <- genExpr ph cid
  arout <- genExpr ph arg
  updSubst <- lookupChoice tem ch
  -- TODO: Temporary solution
  this <- lookupCid (_oExpr cidOut) `catchError` (\_ -> trace ("Not found: " ++ show (_oExpr cidOut)) $ return $ ExprVarName "this")
  -- TODO: Should we further eval after subst? But how to eval an update set?
  let updSet = updSubst (_oExpr cidOut) (EVar this) (_oExpr arout)
  return (Output (EUpdate (UExercise tem ch (_oExpr cidOut) par (_oExpr arout))) updSet)

-- | Analyse a bind update expression.
-- TODO: Handle arbitrary update outputs, not just simple fetches
genForBind :: MonadEnv m
  => Phase
  -- ^ The current generator phase.
  -> Binding
  -- ^ The binding being bound with this update.
  -> Expr
  -- ^ The expression in which this binding is being made available.
  -> m Output
genForBind ph bind body = do
  bindOut <- genExpr ph (bindingBound bind)
  case _oExpr bindOut of
    EUpdate (UFetch tc cid) -> do
      fs <- recTypConFields $ qualObject tc
      extRecEnv (fst $ bindingBinder bind) (map fst fs)
      cidOut <- genExpr ph cid
      extCidEnv (_oExpr cidOut) (fst $ bindingBinder bind)
    _ -> return ()
  extVarEnv (fst $ bindingBinder bind)
  bodyOut <- genExpr ph body
  return $ combineOut bodyOut bindOut
