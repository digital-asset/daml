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

data Phase
  = ValuePhase
  | TemplatePhase

data Output = Output
  { _oExpr :: Expr
    -- ^ The expression, evaluated as far as possible.
  , _oUpdate :: UpdateSet
    -- ^ The updates, performed by this expression.
  }

emptyOut :: Expr -> Output
emptyOut expr = Output expr emptyUpdateSet

-- | Extend a generator output with the updates of the second generator output.
-- Note that the end result will contain the first expression.
combineOut :: Output -> Output -> Output
combineOut out1 out2 = extendOutUpds (_oUpdate out2) out1

updateOutExpr :: Expr
  -- ^ The new output expression.
  -> Output
  -- ^ The current generator output.
  -> Output
updateOutExpr expr out = out{_oExpr = expr}

extendOutUpds :: UpdateSet
  -- ^ The extension of the update set.
  -> Output
  -- ^ The current generator output.
  -> Output
extendOutUpds upds out@Output{..} = out{_oUpdate = concatUpdateSet upds _oUpdate}

addArchiveUpd :: Qualified TypeConName -> [(FieldName, Expr)] -> Output -> Output
addArchiveUpd temp fs (Output expr upd@UpdateSet{..}) =
  Output expr upd{_usArc = UpdArchive temp fs : _usArc}

genPackages :: MonadEnv m => Phase -> [(PackageId, (Package, Maybe PackageName))] -> m ()
genPackages ph inp = mapM_ (genPackage ph) inp

genPackage :: MonadEnv m => Phase -> (PackageId, (Package, Maybe PackageName)) -> m ()
genPackage ph (id, (pac, _)) = mapM_ (genModule ph (PRImport id)) (NM.toList $ packageModules pac)

genModule :: MonadEnv m => Phase -> PackageRef -> Module -> m ()
genModule ValuePhase pac mod = do
  extDatsEnv (NM.toHashMap (moduleDataTypes mod))
  mapM_ (genValue pac (moduleName mod)) (NM.toList $ moduleValues mod)
genModule TemplatePhase pac mod =
  mapM_ (genTemplate pac (moduleName mod)) (NM.toList $ moduleTemplates mod)

genValue :: MonadEnv m => PackageRef -> ModuleName -> DefValue -> m ()
genValue pac mod val = do
  expOut <- genExpr ValuePhase (dvalBody val)
  let qname = Qualified pac mod (fst $ dvalBinder val)
  extValEnv qname (_oExpr expOut) (_oUpdate expOut)

-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadEnv m => Qualified TypeConName -> (ExprVarName,ExprVarName)
  -> [FieldName] -> TemplateChoice -> m ()
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

genTemplate :: MonadEnv m => PackageRef -> ModuleName -> Template -> m ()
-- TODO: Take precondition into account?
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

genExpr :: MonadEnv m => Phase -> Expr -> m Output
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

genForTmApp :: MonadEnv m => Phase -> Expr -> Expr -> m Output
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

genForTyApp :: MonadEnv m => Phase -> Expr -> Type -> m Output
genForTyApp ph expr typ = do
  exprOut <- genExpr ph expr
  case _oExpr exprOut of
    ETyLam bndr body -> do
      let subst = singleTypeSubst (fst bndr) typ
          resExpr = substituteTyTm subst body
      resOut <- genExpr ph resExpr
      return $ combineOut resOut exprOut
    expr' -> return $ updateOutExpr (ETyApp expr' typ) exprOut

genForLet :: MonadEnv m => Phase -> Binding -> Expr -> m Output
genForLet ph bind body = do
  bindOut <- genExpr ph (bindingBound bind)
  let subst = singleExprSubst (fst $ bindingBinder bind) (_oExpr bindOut)
      resExpr = substituteTmTm subst body
  resOut <- genExpr ph resExpr
  return $ combineOut resOut bindOut

genForVar :: MonadEnv m => Phase -> ExprVarName -> m Output
genForVar _ph name = lookupVar name >> return (emptyOut (EVar name))

genForVal :: MonadEnv m => Phase -> Qualified ExprValName -> m Output
genForVal ValuePhase w
  = return $ Output (EVal w) (emptyUpdateSet{_usVal = [w]})
genForVal TemplatePhase w
  = lookupVal w >>= \ (expr, upds) -> return (Output expr upds)

genForRecProj :: MonadEnv m => Phase -> TypeConApp -> FieldName -> Expr -> m Output
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

genForCreate :: MonadEnv m => Phase -> Qualified TypeConName -> Expr -> m Output
genForCreate ph tem arg = do
  arout <- genExpr ph arg
  fs <- recExpFields (_oExpr arout)
  return (Output (EUpdate (UCreate tem $ _oExpr arout)) emptyUpdateSet{_usCre = [UpdCreate tem fs]})
  -- TODO: We could potentially filter here to only store the interesting fields?

genForExercise :: MonadEnv m => Phase -> Qualified TypeConName -> ChoiceName
  -> Expr -> Maybe Expr -> Expr -> m Output
genForExercise ph tem ch cid par arg = do
  cidOut <- genExpr ph cid
  arout <- genExpr ph arg
  updSubst <- lookupChoice tem ch
  -- TODO: Temporary solution
  this <- lookupCid (_oExpr cidOut) `catchError` (\_ -> trace ("Not found: " ++ show (_oExpr cidOut)) $ return $ ExprVarName "this")
  -- TODO: Should we further eval after subst? But how to eval an update set?
  let updSet = updSubst (_oExpr cidOut) (EVar this) (_oExpr arout)
  return (Output (EUpdate (UExercise tem ch (_oExpr cidOut) par (_oExpr arout))) updSet)

-- TODO: Handle arbitrary update outputs, not just simple fetches
genForBind :: MonadEnv m => Phase -> Binding -> Expr -> m Output
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
