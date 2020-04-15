-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

-- | Constraint generator for DAML LF static verification
module DA.Daml.LF.Verify.Generate
  ( genPackages
  , Phase(..)
  ) where

import Control.Lens hiding (Context)
import Data.HashMap.Strict (singleton, fromList, union)
import Control.Monad.Error.Class (throwError, catchError)
import Control.Monad.Reader
import qualified Data.NameMap as NM
import Debug.Trace

import DA.Daml.LF.Ast
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
  , _goDel :: Delta
    -- ^ The context extension, made by this expression.
  }

makeLenses ''GenOutput

-- | Extend a generator output with the updates and the context extensions of a
-- second generator output. Note that the end result will contain the first
-- expression.
combineGO :: GenOutput -> GenOutput
          -- ^ The two generator outputs to combine.
          -> GenOutput
combineGO genOut1 genOut2
  = extendGOUpds (_goUpd genOut2)
  $ extendGODelta (_goDel genOut2)
    genOut1

updateGOExpr :: Expr
             -- ^ The new output expression.
             -> GenOutput
             -- ^ The current generator output.
             -> GenOutput
updateGOExpr expr = set goExp expr

extendGOUpds :: UpdateSet
             -- ^ The extension of the update set.
             -> GenOutput
             -- ^ The current generator output.
             -> GenOutput
extendGOUpds upds = over goUpd (concatUpdateSet upds)

extendGODelta :: Delta
              -- ^ The additional context extension.
              -> GenOutput
              -- ^ The current generator output.
              -> GenOutput
extendGODelta delta = over goDel (concatDelta delta)

-- | Builds up the Delta environment by recursively extending the environment
-- for each computation, and combining the output environments.
buildDelta :: MonadDelta m => Delta -> (a -> m Delta) -> [a] -> m Delta
buildDelta del op args = foldM step del args
  where
    step d x = concatDelta d <$> setDelta d (op x)

genPackages :: MonadDelta m => Phase -> [(PackageId, (Package, Maybe PackageName))] -> m Delta
genPackages ph inp = do
  del0 <- ask
  buildDelta del0 (genPackage ph) inp

genPackage :: MonadDelta m => Phase -> (PackageId, (Package, Maybe PackageName)) -> m Delta
genPackage ph (id, (pac, _)) = do
  del0 <- ask
  buildDelta del0 (genModule ph (PRImport id)) (NM.toList $ packageModules pac)

-- TODO: Type synonyms and data types are ignored for now.
genModule :: MonadDelta m => Phase -> PackageRef -> Module -> m Delta
genModule ValuePhase pac mod = do
  del <- ask
  buildDelta del (genValue pac (moduleName mod)) (NM.toList $ moduleValues mod)
genModule TemplatePhase pac mod = do
  del <- ask
  buildDelta del (genTemplate pac (moduleName mod)) (NM.toList $ moduleTemplates mod)

genValue :: MonadDelta m => PackageRef -> ModuleName -> DefValue -> m Delta
genValue pac mod val =
  catchError
    (do { expOut <- genExpr ValuePhase (dvalBody val)
        ; let qname = Qualified pac mod (fst $ dvalBinder val)
        ; return emptyDelta{_devals = singleton qname (_goExp expOut, _goUpd expOut)}
        })
    (\err -> trace (show err)
             $ trace ("Fail: " ++ (show $ unExprValName $ fst $ dvalBinder val))
             $ return emptyDelta)

-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadDelta m => Qualified TypeConName -> TemplateChoice
          -> m GenOutput
genChoice tem cho = do
  expOut <- extVarDelta (fst $ chcArgBinder cho) $ genExpr TemplatePhase (chcUpdate cho)
  let updSet = if chcConsuming cho
        -- TODO: Convert the `ExprVarName`s to `FieldName`s
        then over usArc ((:) (UpdArchive tem [])) (_goUpd expOut)
        else _goUpd expOut
  return $ set goUpd updSet
         $ over (goDel . devars) ((:) (fst $ chcArgBinder cho))
         expOut

genTemplate :: MonadDelta m => PackageRef -> ModuleName -> Template -> m Delta
-- TODO: lookup the data type and skolemise all fieldnames
genTemplate pac mod Template{..} = do
  let name = Qualified pac mod tplTypeCon
  choOuts <- mapM (genChoice name) (NM.toList tplChoices)
  let delta = foldl concatDelta emptyDelta (map _goDel choOuts)
      choices = fromList $ zip (zip (repeat name) (map chcName $ NM.toList tplChoices)) (map _goUpd choOuts)
  return $ over dchs (union choices) delta

genExpr :: MonadDelta m => Phase -> Expr -> m GenOutput
genExpr ph = \case
  ETmApp fun arg  -> genForTmApp ph fun arg
  ETyApp expr typ -> genForTyApp ph expr typ
  EVar name       -> genForVar ph name
  EVal w          -> genForVal ph w
  EUpdate (UCreate tem arg)              -> genForCreate ph tem arg
  EUpdate (UExercise tem ch cid par arg) -> genForExercise ph tem ch cid par arg
  -- TODO: Extend additional cases
  e -> return $ GenOutput e emptyUpdateSet emptyDelta

genForTmApp :: MonadDelta m => Phase -> Expr -> Expr -> m GenOutput
genForTmApp ph fun arg = do
  funOut <- genExpr ph fun
  argOut <- genExpr ph arg
  case _goExp funOut of
    ETmLam bndr body -> do
      let updDelta = concatDelta (_goDel funOut) (_goDel argOut)
          subst    = singleExprSubst (fst bndr) (_goExp argOut)
          resExpr  = substituteTmTm subst body
      resOut <- introDelta updDelta $ genExpr ph resExpr
      return $ combineGO resOut
             $ combineGO funOut argOut
    fun'             -> return $ updateGOExpr (ETmApp fun' (_goExp argOut))
                               $ combineGO funOut argOut

genForTyApp :: MonadDelta m => Phase -> Expr -> Type -> m GenOutput
genForTyApp ph expr typ = do
  exprOut <- genExpr ph expr
  case _goExp exprOut of
    ETyLam bndr body -> do
      let subst   = singleTypeSubst (fst bndr) typ
          resExpr = substituteTyTm subst body
      resOut <- introDelta (_goDel exprOut) $ genExpr ph resExpr
      return $ combineGO resOut exprOut
    expr'            -> return $ updateGOExpr (ETyApp expr' typ) exprOut

genForVar :: MonadDelta m => Phase -> ExprVarName -> m GenOutput
genForVar _ph name = lookupDExprVar name
                 >> return (GenOutput (EVar name) emptyUpdateSet emptyDelta)

genForVal :: MonadDelta m => Phase -> Qualified ExprValName -> m GenOutput
genForVal ValuePhase w
  = return $ GenOutput (EVal w) (emptyUpdateSet{_usVal = [w]}) emptyDelta
genForVal TemplatePhase w
  = lookupDVal w >>= \ (expr, upds) -> return (GenOutput expr upds emptyDelta)

genForCreate :: MonadDelta m => Phase -> Qualified TypeConName -> Expr -> m GenOutput
genForCreate ph tem arg = do
  argOut <- genExpr ph arg
  case _goExp argOut of
    argExpr@(ERecCon _ fs) -> return (GenOutput (EUpdate (UCreate tem argExpr))
                                      -- TODO: We could potentially filter here
                                      -- to only store the interesting fields?
                                      emptyUpdateSet{_usCre = [UpdCreate tem fs]}
                                      (_goDel argOut))
    _ -> throwError ExpectRecord

genForExercise :: MonadDelta m => Phase -> Qualified TypeConName -> ChoiceName
               -> Expr -> Maybe Expr -> Expr
               -> m GenOutput
genForExercise ph tem ch cid par arg = do
  cidOut <- genExpr ph cid
  argOut <- genExpr ph arg
  -- TODO: Take possibility into account that the choice is not found?
  updSet <- lookupDChoice tem ch
  return (GenOutput (EUpdate (UExercise tem ch (_goExp cidOut) par (_goExp argOut)))
                     updSet
                     (concatDelta (_goDel cidOut) (_goDel argOut)))

