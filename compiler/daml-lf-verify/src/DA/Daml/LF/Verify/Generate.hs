-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}

-- | Constraint generator for DAML LF static verification
module DA.Daml.LF.Verify.Generate
  ( genPackages
  , Phase(..)
  ) where

import Control.Monad.Error.Class (catchError, throwError)
import Data.Maybe (listToMaybe)
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
emptyOut :: GenPhase ph
  => Expr
  -- ^ The evaluated expression.
  -> Output ph
emptyOut expr = Output expr emptyUpdateSet

-- | Extend a generator output with the updates of the second generator output.
-- Note that the end result will contain only the first expression.
combineOut :: Output ph -> Output ph -> Output ph
combineOut out1 out2 = extendOutUpds (_oUpdate out2) out1

-- | Update an output with a new evaluated expression.
updateOutExpr :: Expr
  -- ^ The new output expression.
  -> Output ph
  -- ^ The generator output to be updated.
  -> Output ph
updateOutExpr expr out = out{_oExpr = expr}

-- | Update an output with additional updates.
extendOutUpds :: UpdateSet ph
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

-- | Generate an environment for a given module.
-- Depending on the generator phase, this either adds all value and data type
-- definitions to the environment, or all template definitions with their
-- respective choices.
genModule :: (GenPhase ph, MonadEnv m ph)
  => PackageRef
  -- ^ A reference to the package in which this module is defined.
  -> Module
  -- ^ The module to analyse.
  -> m ()
genModule pac mod = getEnv >>= \case
  EnvVG{} -> do
    extDatsEnv (NM.toHashMap (moduleDataTypes mod))
    mapM_ (genValue pac (moduleName mod)) (NM.toList $ moduleValues mod)
  EnvCG{} ->
    mapM_ (genTemplate pac (moduleName mod)) (NM.toList $ moduleTemplates mod)
  EnvS{} -> error "Impossible: genModule can't be used in the solving phase"

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
  expOut <- genExpr (dvalBody val)
  let qname = Qualified pac mod (fst $ dvalBinder val)
  extValEnv qname (_oExpr expOut) (_oUpdate expOut)

-- | Analyse a choice definition and add to the environment.
-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadEnv m 'ChoiceGathering
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
  expOut <- genExpr
    $ substituteTm (createExprSubst [(self',EVar self),(this',EVar this),(arg',EVar arg)]) chcUpdate
  let out = if chcConsuming
        then addArchiveUpd tem fields expOut
        else expOut
  extChEnv tem chcName self this arg (_oUpdate out)
  where
    fields = map (\f -> (f, ERecProj (TypeConApp tem []) f (EVar this))) temFs

-- | Analyse a template definition and add all choices to the environment.
genTemplate :: MonadEnv m 'ChoiceGathering
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
genExpr :: (GenPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The expression to be analysed.
  -> m (Output ph)
genExpr = \case
  ETmApp fun arg -> genForTmApp fun arg
  ETyApp expr typ -> genForTyApp expr typ
  ELet bind body -> genForLet bind body
  EVar name -> genForVar name
  EVal w -> genForVal w
  ERecProj tc f e -> genForRecProj tc f e
  EStructProj f e -> genForStructProj f e
  ELocation _ expr -> genExpr expr
  EUpdate (UCreate tem arg) -> genForCreate tem arg
  EUpdate (UExercise tem ch cid par arg) -> genForExercise tem ch cid par arg
  EUpdate (UBind bind expr) -> genForBind bind expr
  EUpdate (UPure _ expr) -> genExpr expr
  ECase e cs -> genForCase e cs
  -- TODO: Extend additional cases
  e -> return $ emptyOut e

-- | Analyse a term application expression.
genForTmApp :: (GenPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The function expression.
  -> Expr
  -- ^ The argument expression.
  -> m (Output ph)
genForTmApp fun arg = do
  funOut <- genExpr fun
  arout <- genExpr arg
  case _oExpr funOut of
    -- TODO: Should we rename here?
    ETmLam bndr body -> do
      let subst = singleExprSubst (fst bndr) (_oExpr arout)
          resExpr = substituteTm subst body
      resOut <- genExpr resExpr
      return $ combineOut resOut
        $ combineOut funOut arout
    fun' -> return $ updateOutExpr (ETmApp fun' (_oExpr arout))
      $ combineOut funOut arout

-- | Analyse a type application expression.
genForTyApp :: (GenPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The function expression.
  -> Type
  -- ^ The argument type.
  -> m (Output ph)
genForTyApp expr typ = do
  exprOut <- genExpr expr
  case _oExpr exprOut of
    ETyLam bndr body -> do
      let subst = singleTypeSubst (fst bndr) typ
          resExpr = substituteTy subst body
      resOut <- genExpr resExpr
      return $ combineOut resOut exprOut
    expr' -> return $ updateOutExpr (ETyApp expr' typ) exprOut

-- | Analyse a let binding expression.
genForLet :: (GenPhase ph, MonadEnv m ph)
  => Binding
  -- ^ The binding to be bound.
  -> Expr
  -- ^ The expression in which the binding should be available.
  -> m (Output ph)
genForLet bind body = do
  bindOut <- genExpr (bindingBound bind)
  let subst = singleExprSubst (fst $ bindingBinder bind) (_oExpr bindOut)
      resExpr = substituteTm subst body
  resOut <- genExpr resExpr
  return $ combineOut resOut bindOut

-- | Analyse an expression variable.
genForVar :: (GenPhase ph, MonadEnv m ph)
  => ExprVarName
  -- ^ The expression variable to be analysed.
  -> m (Output ph)
genForVar name = lookupVar name >> return (emptyOut (EVar name))

-- | Analyse a value reference.
genForVal :: (GenPhase ph, MonadEnv m ph)
  => Qualified ExprValName
  -- ^ The value reference to be analysed.
  -> m (Output ph)
genForVal w = getEnv >>= \case
  EnvVG{} -> return $ Output (EVal w) (emptyUpdateSet{_usvgValue = [Determined w]})
  EnvCG{} -> lookupVal w >>= \ (expr, upds) -> return (Output expr upds)
  EnvS{} -> error "Impossible: genForVal can't be used in the solving phase"

-- | Analyse a record projection expression.
genForRecProj :: (GenPhase ph, MonadEnv m ph)
  => TypeConApp
  -- ^ The type constructor of the record which is projected.
  -> FieldName
  -- ^ The field which is projected.
  -> Expr
  -- ^ The record expression which is projected.
  -> m (Output ph)
genForRecProj tc f body = do
  bodyOut <- genExpr body
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
        Just expr -> genExpr expr
        Nothing -> throwError $ UnknownRecField f

-- | Analyse a struct projection expression.
genForStructProj :: (GenPhase ph, MonadEnv m ph)
  => FieldName
  -- ^ The field which is projected.
  -> Expr
  -- ^ The record expression which is projected.
  -> m (Output ph)
genForStructProj f body = do
  bodyOut <- genExpr body
  case _oExpr bodyOut of
    -- TODO: I think we can reduce duplication a bit more here
    EVar x -> do
      skol <- lookupRec x f
      if skol
        then return $ updateOutExpr (EStructProj f (EVar x)) bodyOut
        else error ("Impossible: expected skolem record: " ++ show x ++ "." ++ show f)
    expr -> do
      fs <- recExpFields expr
      case lookup f fs of
        Just expr -> genExpr expr
        Nothing -> throwError $ UnknownRecField f

-- | Analyse a case expression.
-- TODO: Atm only boolean cases are supported
genForCase :: (GenPhase ph, MonadEnv m ph)
  => Expr
  -- ^ The expression to match on.
  -> [CaseAlternative]
  -- ^ The list of alternatives.
  -> m (Output ph)
genForCase exp cs = do
  expOut <- genExpr exp
  case findBool True of
    Just tru -> do
      truOut <- genExpr tru
      case findBool False of
        Just fal -> do
          falOut <- genExpr fal
          let resExp = ECase (_oExpr expOut)
                [ CaseAlternative (CPBool True) (_oExpr truOut)
                , CaseAlternative (CPBool False) (_oExpr falOut) ]
              resUpd = _oUpdate expOut `concatUpdateSet`
                conditionalUpdateSet (_oExpr expOut) (_oUpdate truOut) (Just $ _oUpdate falOut)
          return $ Output resExp resUpd
        Nothing -> do
          let resExp = ECase (_oExpr expOut)
                [ CaseAlternative (CPBool True) (_oExpr truOut) ]
              resUpd = _oUpdate expOut `concatUpdateSet`
                conditionalUpdateSet (_oExpr expOut) (_oUpdate truOut) Nothing
          return $ Output resExp resUpd
    Nothing -> return $ emptyOut (ECase exp cs)
  where
    findBool :: Bool -> Maybe Expr
    findBool b1 = listToMaybe $ [e | CaseAlternative (CPBool b2) e <- cs, b1 == b2]

-- | Analyse a create update expression.
genForCreate :: (GenPhase ph, MonadEnv m ph)
  => Qualified TypeConName
  -- ^ The template of which a new instance is being created.
  -> Expr
  -- ^ The argument expression.
  -> m (Output ph)
genForCreate tem arg = do
  arout <- genExpr arg
  fs <- recExpFields (_oExpr arout)
  return (Output (EUpdate (UCreate tem $ _oExpr arout)) $ addUpd emptyUpdateSet (UpdCreate tem fs))
  -- TODO: We could potentially filter here to only store the interesting fields?

-- | Analyse an exercise update expression.
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
  -> m (Output ph)
genForExercise tem ch cid par arg = do
  cidOut <- genExpr cid
  arout <- genExpr arg
  updSubst <- lookupChoice tem ch
  -- TODO: Temporary solution. To be fixed urgently.
  this <- lookupCid (_oExpr cidOut) `catchError`
    -- (\_ -> trace ("Not found: " ++ show (_oExpr cidOut)) $ return $ ExprVarName "this")
    (\_ -> return $ ExprVarName "this")
  -- TODO: Should we further eval after subst? But how to eval an update set?
  let updSet = updSubst (_oExpr cidOut) (EVar this) (_oExpr arout)
  return (Output (EUpdate (UExercise tem ch (_oExpr cidOut) par (_oExpr arout))) updSet)

-- | Analyse a bind update expression.
-- TODO: Handle arbitrary update outputs, not just simple fetches
genForBind :: (GenPhase ph, MonadEnv m ph)
  => Binding
  -- ^ The binding being bound with this update.
  -> Expr
  -- ^ The expression in which this binding is being made available.
  -> m (Output ph)
genForBind bind body = do
  bindOut <- genExpr (bindingBound bind)
  case _oExpr bindOut of
    EUpdate (UFetch tc cid) -> do
      fs <- recTypConFields $ qualObject tc
      extRecEnv (fst $ bindingBinder bind) (map fst fs)
      cidOut <- genExpr cid
      extCidEnv (_oExpr cidOut) (fst $ bindingBinder bind)
    _ -> return ()
  extVarEnv (fst $ bindingBinder bind)
  bodyOut <- genExpr body
  return $ combineOut bodyOut bindOut
