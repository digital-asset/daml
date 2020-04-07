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
  = extendGOUpds (genOut2 ^. goUpd)
  $ extendGODelta (genOut2 ^. goDel)
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

genPackages :: MonadDelta m => [(PackageId, (Package, Maybe PackageName))]
            -> m Delta
genPackages inp = ask >>= \del0 -> foldM genPackage' del0 inp
  where
    -- TODO: These prime definitions are bit silly. Merge with the regular
    -- functions.
    -- TODO: This >>= return . concatDelta returns a lot. Figure out a way to
    -- abstract over it. Same for this returning foldM.
    genPackage' :: MonadDelta m => Delta
                -> (PackageId, (Package, Maybe PackageName)) -> m Delta
    genPackage' deli (idi, (paci, _)) =
      -- TODO: This is getting quite unreadable.
      foldM (\deli' modi -> genModule' deli' (PRImport idi, modi))
            deli (NM.toList $ packageModules paci)
      >>= return . (concatDelta deli)
    genModule' :: MonadDelta m => Delta -> (PackageRef, Module) -> m Delta
    genModule' deli (paci, modi) = introDelta deli (genModule paci modi)
                                   >>= return . (concatDelta deli)

-- TODO: Type synonyms and data types are ignored for now.
genModule :: MonadDelta m => PackageRef -> Module -> m Delta
genModule pac mod = do
  del0 <- ask
  del1 <- foldM genValue' del0 (NM.toList $ moduleValues mod)
  del2 <- foldM genTemplate' del1 (NM.toList $ moduleTemplates mod)
  return $ concatDelta del1 del2
  where
    genValue' :: MonadDelta m => Delta -> DefValue -> m Delta
    genValue' deli vali = introDelta deli (genValue pac (moduleName mod) vali)
                          >>= return . (concatDelta deli)
    genTemplate' :: MonadDelta m => Delta -> Template -> m Delta
    genTemplate' deli temi = introDelta deli (genTemplate temi)
                          >>= return . (concatDelta deli)

genValue :: MonadDelta m => PackageRef -> ModuleName -> DefValue -> m Delta
genValue pac mod val = do
  expOut <- genExpr (dvalBody val)
  let qname = Qualified pac mod (fst $ dvalBinder val)
  return $ set devals (singleton qname (expOut ^. goExp, expOut ^. goUpd)) emptyDelta

-- TODO: Handle annotated choices, by returning a set of annotations.
genChoice :: MonadDelta m => (Qualified TypeConName) -> TemplateChoice
          -> m GenOutput
genChoice tem cho = do
  expOut <- extVarDelta (fst $ chcArgBinder cho) $ genExpr (chcUpdate cho)
  let updSet = if chcConsuming cho
        -- TODO: Convert the `ExprVarName`s to `FieldName`s
        then over usArc ((:) (UpdArchive tem [])) (expOut ^. goUpd)
        else expOut ^. goUpd
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
  case funOut ^. goExp of
    ETmLam bndr body -> do
      let updDelta = concatDelta (funOut ^. goDel) (argOut ^. goDel)
          subst    = singleExprSubst (fst bndr) (argOut ^. goExp)
          resExpr  = substituteTmTm subst body
      resOut <- introDelta updDelta $ genExpr resExpr
      return $ combineGO resOut
             $ combineGO funOut argOut
    fun'             -> return $ updateGOExpr (ETmApp fun' (argOut ^. goExp))
                               $ combineGO funOut argOut

genForTyApp :: MonadDelta m => Expr -> Type -> m GenOutput
genForTyApp expr typ = do
  exprOut <- genExpr expr
  case exprOut ^. goExp of
    ETyLam bndr body -> do
      let subst   = singleTypeSubst (fst bndr) typ
          resExpr = substituteTyTm subst body
      resOut <- introDelta (exprOut ^. goDel) $ genExpr resExpr
      return $ combineGO resOut exprOut
    expr'            -> return $ updateGOExpr (ETyApp expr' typ) exprOut

genForVar :: MonadDelta m => ExprVarName -> m GenOutput
genForVar name = lookupDExprVar name
                 >> return (GenOutput (EVar name) emptyUpdateSet emptyDelta)

genForVal :: MonadDelta m => (Qualified ExprValName) -> m GenOutput
genForVal w = lookupDVal w
              >>= \ (expr, upds) -> return (GenOutput expr upds emptyDelta)

genForCreate :: MonadDelta m => (Qualified TypeConName) -> Expr -> m GenOutput
genForCreate tem arg = do
  argOut <- genExpr arg
  case argOut ^. goExp of
    argExpr@(ERecCon _ fs) -> return (GenOutput (EUpdate (UCreate tem argExpr))
                                      -- TODO: We could potentially filter here
                                      -- to only store the interesting fields?
                                      (set usCre [UpdCreate tem fs] emptyUpdateSet)
                                      (argOut ^. goDel))
    _                      -> throwError EEnumTypeWithParams
    -- TODO: This is a random error, as we do not have access to the expected
    -- type, which we need to constructed the error we really want.
    -- Perhaps we do need to define our own set of errors.
    -- _                      -> throwError (EExpectedRecordType ty)

genForExercise :: MonadDelta m => (Qualified TypeConName) -> ChoiceName
               -> Expr -> Maybe Expr -> Expr
               -> m GenOutput
genForExercise tem ch cid par arg = do
  cidOut <- genExpr cid
  argOut <- genExpr arg
  -- TODO: Take possibility into account that the choice is not found?
  updSet <- lookupDChoice tem ch
  return (GenOutput (EUpdate (UExercise tem ch (cidOut ^. goExp) par (argOut ^. goExp)))
                     updSet
                     (concatDelta (cidOut ^. goDel) (argOut ^. goDel)))

