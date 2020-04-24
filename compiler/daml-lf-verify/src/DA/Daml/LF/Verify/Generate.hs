-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Constraint generator for DAML LF static verification
module DA.Daml.LF.Verify.Generate
  ( genPackages
  , Phase(..)
  ) where

import Control.Monad.Error.Class (catchError, throwError)
import qualified Data.NameMap as NM

import DA.Daml.LF.Ast hiding (lookupChoice)
import DA.Daml.LF.Verify.Context
import DA.Daml.LF.Verify.Subst

data Phase
  = ValuePhase
  | TemplatePhase

data GenOutput = GenOutput
  { _goExp :: Expr
    -- ^ The expression, evaluated as far as possible.
  , _goUpd :: UpdateSet
    -- ^ The updates, performed by this expression.
  }

emptyGO :: Expr -> GenOutput
emptyGO expr = GenOutput expr emptyUpdateSet

-- | Extend a generator output with the updates of the second generator output.
-- Note that the end result will contain the first expression.
combineGO :: GenOutput -> GenOutput -> GenOutput
combineGO genOut1 genOut2 = extendGOUpds (_goUpd genOut2) genOut1

updateGOExpr :: Expr
  -- ^ The new output expression.
  -> GenOutput
  -- ^ The current generator output.
  -> GenOutput
updateGOExpr expr gout = gout{_goExp = expr}

extendGOUpds :: UpdateSet
  -- ^ The extension of the update set.
  -> GenOutput
  -- ^ The current generator output.
  -> GenOutput
extendGOUpds upds gout@GenOutput{..} = gout{_goUpd = concatUpdateSet upds _goUpd}

addArchiveUpd :: Qualified TypeConName -> [(FieldName, Expr)] -> GenOutput -> GenOutput
addArchiveUpd temp fs (GenOutput expr upd@UpdateSet{..}) =
  GenOutput expr upd{_usArc = UpdArchive temp fs : _usArc}

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
  extValEnv qname (_goExp expOut) (_goUpd expOut)

-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadEnv m => Qualified TypeConName -> ExprVarName -> [FieldName]
  -> TemplateChoice -> m ()
genChoice tem this temFs TemplateChoice{..} = do
  let self' = chcSelfBinder
      arg' = fst chcArgBinder
  self <- genRenamedVar self'
  arg <- genRenamedVar arg'
  extVarEnv self
  extVarEnv arg
  argFs <- recTypFields (snd chcArgBinder)
  extRecEnv arg argFs
  expOut <- genExpr TemplatePhase
    $ substituteTmTm (createExprSubst [(self',EVar self),(arg',EVar arg)]) chcUpdate
  let go = if chcConsuming
        then addArchiveUpd tem fields expOut
        else expOut
  extChEnv tem chcName self this arg (_goUpd go)
  where
    fields = map (\f -> (f, ERecProj (TypeConApp tem []) f (EVar this))) temFs

genTemplate :: MonadEnv m => PackageRef -> ModuleName -> Template -> m ()
-- TODO: Take precondition into account?
genTemplate pac mod Template{..} = do
  let name = Qualified pac mod tplTypeCon
  fields <- recTypConFields tplTypeCon
  let fs = map fst fields
      xs = map fieldName2VarName fs
  -- TODO: if daml indead translates into `amount = this.amount`, this can be dropped.
  mapM_ extVarEnv xs
  -- TODO: refresh tplParam (this)
  extRecEnv tplParam fs
  extRecEnvLvl1 fields
  mapM_ (genChoice name tplParam fs) (archive : NM.toList tplChoices)
  where
    archive :: TemplateChoice
    archive = TemplateChoice Nothing (ChoiceName "Archive") True
      (ENil (TBuiltin BTParty)) (ExprVarName "self")
      (ExprVarName "arg", TStruct []) (TBuiltin BTUnit)
      (EUpdate $ UPure (TBuiltin BTUnit) (EBuiltin BEUnit))

genExpr :: MonadEnv m => Phase -> Expr -> m GenOutput
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
  e -> return $ emptyGO e

genForTmApp :: MonadEnv m => Phase -> Expr -> Expr -> m GenOutput
genForTmApp ph fun arg = do
  funOut <- genExpr ph fun
  argOut <- genExpr ph arg
  case _goExp funOut of
    -- TODO: Should we rename here?
    ETmLam bndr body -> do
      let subst = singleExprSubst (fst bndr) (_goExp argOut)
          resExpr = substituteTmTm subst body
      resOut <- genExpr ph resExpr
      return $ combineGO resOut
        $ combineGO funOut argOut
    fun' -> return $ updateGOExpr (ETmApp fun' (_goExp argOut))
      $ combineGO funOut argOut

genForTyApp :: MonadEnv m => Phase -> Expr -> Type -> m GenOutput
genForTyApp ph expr typ = do
  exprOut <- genExpr ph expr
  case _goExp exprOut of
    ETyLam bndr body -> do
      let subst = singleTypeSubst (fst bndr) typ
          resExpr = substituteTyTm subst body
      resOut <- genExpr ph resExpr
      return $ combineGO resOut exprOut
    expr' -> return $ updateGOExpr (ETyApp expr' typ) exprOut

genForLet :: MonadEnv m => Phase -> Binding -> Expr -> m GenOutput
genForLet ph bind body = do
  bindOut <- genExpr ph (bindingBound bind)
  let subst = singleExprSubst (fst $ bindingBinder bind) (_goExp bindOut)
      resExpr = substituteTmTm subst body
  resOut <- genExpr ph resExpr
  return $ combineGO resOut bindOut

genForVar :: MonadEnv m => Phase -> ExprVarName -> m GenOutput
genForVar _ph name = lookupVar name >> return (emptyGO (EVar name))

genForVal :: MonadEnv m => Phase -> Qualified ExprValName -> m GenOutput
genForVal ValuePhase w
  = return $ GenOutput (EVal w) (emptyUpdateSet{_usVal = [w]})
genForVal TemplatePhase w
  = lookupVal w >>= \ (expr, upds) -> return (GenOutput expr upds)

genForRecProj :: MonadEnv m => Phase -> TypeConApp -> FieldName -> Expr -> m GenOutput
genForRecProj ph tc f body = do
  bodyOut <- genExpr ph body
  case _goExp bodyOut of
    -- TODO: I think we can reduce duplication a bit more here
    EVar x -> do
      skol <- lookupRec x f
      if skol
        then return $ updateGOExpr (ERecProj tc f (EVar x)) bodyOut
        else error ("Impossible: expected skolem record: " ++ show x ++ "." ++ show f)
    expr -> do
      fs <- recExpFields expr
      case lookup f fs of
        Just expr -> genExpr ph expr
        Nothing -> throwError $ UnknownRecField f

genForCreate :: MonadEnv m => Phase -> Qualified TypeConName -> Expr -> m GenOutput
genForCreate ph tem arg = do
  argOut <- genExpr ph arg
  fs <- recExpFields (_goExp argOut)
  return (GenOutput (EUpdate (UCreate tem $ _goExp argOut)) emptyUpdateSet{_usCre = [UpdCreate tem fs]})
  -- TODO: We could potentially filter here to only store the interesting fields?

genForExercise :: MonadEnv m => Phase -> Qualified TypeConName -> ChoiceName
  -> Expr -> Maybe Expr -> Expr -> m GenOutput
genForExercise ph tem ch cid par arg = do
  cidOut <- genExpr ph cid
  argOut <- genExpr ph arg
  updSubst <- lookupChoice tem ch
  -- TODO: Temporary solution
  this <- lookupCid (_goExp cidOut) `catchError` (\_ -> return $ ExprVarName "this")
  -- TODO: Should we further eval after subst? But how to eval an update set?
  let updSet = updSubst (_goExp cidOut) (EVar this) (_goExp argOut)
  return (GenOutput (EUpdate (UExercise tem ch (_goExp cidOut) par (_goExp argOut))) updSet)

-- TODO: Handle arbitrary update outputs, not just simple fetches
genForBind :: MonadEnv m => Phase -> Binding -> Expr -> m GenOutput
genForBind ph bind body = do
  bindOut <- genExpr ph (bindingBound bind)
  case _goExp bindOut of
    EUpdate (UFetch tc cid) -> do
      fs <- recTypConFields $ qualObject tc
      extRecEnv (fst $ bindingBinder bind) (map fst fs)
      cidOut <- genExpr ph cid
      extCidEnv (_goExp cidOut) (fst $ bindingBinder bind)
    _ -> return ()
  extVarEnv (fst $ bindingBinder bind)
  bodyOut <- genExpr ph body
  return $ combineGO bodyOut bindOut
