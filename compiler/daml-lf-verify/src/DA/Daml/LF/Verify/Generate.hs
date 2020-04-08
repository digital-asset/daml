-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

-- | Constraint generator for DAML LF static verification
module DA.Daml.LF.Verify.Generate
  ( genPackages
  , genChoice -- Added export to suppress the unused warning.
  ) where

import Control.Lens hiding (Context)
import Control.Monad.Error.Class (MonadError (..))
import Data.HashMap.Strict (singleton)
import Control.Monad.Reader
import qualified Data.NameMap as NM

import DA.Daml.LF.Ast
import DA.Daml.LF.Verify.Context
import DA.Daml.LF.Verify.Subst
import DA.Daml.LF.TypeChecker.Error

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

genPackages :: MonadDelta m => [(PackageId, (Package, Maybe PackageName))] -> m Delta
genPackages inp = do
  del0 <- ask
  buildDelta del0 genPackage inp

genPackage :: MonadDelta m => (PackageId, (Package, Maybe PackageName)) -> m Delta
genPackage (id, (pac, _)) = do
  del0 <- ask
  buildDelta del0 (genModule (PRImport id)) (NM.toList $ packageModules pac)

-- TODO: Type synonyms and data types are ignored for now.
genModule :: MonadDelta m => PackageRef -> Module -> m Delta
genModule pac mod = do
  del0 <- ask
  del1 <- buildDelta del0 (genValue pac (moduleName mod)) (NM.toList $ moduleValues mod)
  buildDelta del1 genTemplate (NM.toList $ moduleTemplates mod)

genValue :: MonadDelta m => PackageRef -> ModuleName -> DefValue -> m Delta
genValue pac mod val = do
  expOut <- genExpr (dvalBody val)
  let qname = Qualified pac mod (fst $ dvalBinder val)
  return emptyDelta{_devals = singleton qname (_goExp expOut, _goUpd expOut)}

-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadDelta m => Qualified TypeConName -> TemplateChoice
          -> m GenOutput
genChoice tem cho = do
  expOut <- extVarDelta (fst $ chcArgBinder cho) $ genExpr (chcUpdate cho)
  let updSet = if chcConsuming cho
        -- TODO: Convert the `ExprVarName`s to `FieldName`s
        then over usArc ((:) (UpdArchive tem [])) (_goUpd expOut)
        else _goUpd expOut
  return $ set goUpd updSet
         $ over (goDel . devars) ((:) (fst $ chcArgBinder cho))
         expOut

genTemplate :: MonadDelta m => Template -> m Delta
genTemplate = undefined -- TODO

genExpr :: MonadDelta m => Expr -> m GenOutput
genExpr = \case
  ETmApp fun arg  -> genForTmApp fun arg
  ETyApp expr typ -> genForTyApp expr typ
  EVar name       -> genForVar name
  EVal w          -> genForVal w
  EUpdate (UCreate tem arg)              -> genForCreate tem arg
  EUpdate (UExercise tem ch cid par arg) -> genForExercise tem ch cid par arg
  _ -> error "Not implemented"

genForTmApp :: MonadDelta m => Expr -> Expr -> m GenOutput
genForTmApp fun arg = do
  funOut <- genExpr fun
  argOut <- genExpr arg
  case _goExp funOut of
    ETmLam bndr body -> do
      let updDelta = concatDelta (_goDel funOut) (_goDel argOut)
          subst    = singleExprSubst (fst bndr) (_goExp argOut)
          resExpr  = substituteTmTm subst body
      resOut <- introDelta updDelta $ genExpr resExpr
      return $ combineGO resOut
             $ combineGO funOut argOut
    fun'             -> return $ updateGOExpr (ETmApp fun' (_goExp argOut))
                               $ combineGO funOut argOut

genForTyApp :: MonadDelta m => Expr -> Type -> m GenOutput
genForTyApp expr typ = do
  exprOut <- genExpr expr
  case _goExp exprOut of
    ETyLam bndr body -> do
      let subst   = singleTypeSubst (fst bndr) typ
          resExpr = substituteTyTm subst body
      resOut <- introDelta (_goDel exprOut) $ genExpr resExpr
      return $ combineGO resOut exprOut
    expr'            -> return $ updateGOExpr (ETyApp expr' typ) exprOut

genForVar :: MonadDelta m => ExprVarName -> m GenOutput
genForVar name = lookupDExprVar name
                 >> return (GenOutput (EVar name) emptyUpdateSet emptyDelta)

genForVal :: MonadDelta m => Qualified ExprValName -> m GenOutput
genForVal w = lookupDVal w
              >>= \ (expr, upds) -> return (GenOutput expr upds emptyDelta)

genForCreate :: MonadDelta m => Qualified TypeConName -> Expr -> m GenOutput
genForCreate tem arg = do
  argOut <- genExpr arg
  case _goExp argOut of
    argExpr@(ERecCon _ fs) -> return (GenOutput (EUpdate (UCreate tem argExpr))
                                      -- TODO: We could potentially filter here
                                      -- to only store the interesting fields?
                                      emptyUpdateSet{_usCre = [UpdCreate tem fs]}
                                      (_goDel argOut))
    _                      -> throwError EEnumTypeWithParams
    -- TODO: This is a random error, as we do not have access to the expected
    -- type, which we need to constructed the error we really want.
    -- Perhaps we do need to define our own set of errors.
    -- _                      -> throwError (EExpectedRecordType ty)

genForExercise :: MonadDelta m => Qualified TypeConName -> ChoiceName
               -> Expr -> Maybe Expr -> Expr
               -> m GenOutput
genForExercise tem ch cid par arg = do
  cidOut <- genExpr cid
  argOut <- genExpr arg
  -- TODO: Take possibility into account that the choice is not found?
  updSet <- lookupDChoice tem ch
  return (GenOutput (EUpdate (UExercise tem ch (_goExp cidOut) par (_goExp argOut)))
                     updSet
                     (concatDelta (_goDel cidOut) (_goDel argOut)))

