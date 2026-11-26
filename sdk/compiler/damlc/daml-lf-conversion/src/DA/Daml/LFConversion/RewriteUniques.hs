-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Rewrite local system binders' uniques deterministically before LF conversion.
-- GHC allocates uniques from a process-global supply, so loading a dependency's
-- original interface versus a reconstructed data-dependency interface can shift
-- the uniques allocated when compiling otherwise identical source.
--
-- 'DA.Daml.UtilGHC.varPrettyPrint' includes these uniques in system variable
-- names. They therefore appear in LF binders and references, for example the
-- @dt_a1h4@ arguments of a @$WHolder@ constructor wrapper, and affect the package
-- hash. Numbering them in traversal order makes these names independent of
-- earlier interface loading without resetting GHC's shared unique supply.
module DA.Daml.LFConversion.RewriteUniques
    ( rewriteUniques
    ) where

import Data.Tuple.Extra (secondM)
import Control.Monad.State.Strict
import "ghc-lib" GHC (CoreModule(..))
import "ghc-lib" GhcPlugins
    ( Bind(..), CoreBind, CoreExpr, Expr(..), Tickish(..), Var, VarEnv
    , emptyVarEnv, extendVarEnv, isNonCoVarId, isSystemName
    , lookupVarEnv, setVarUnique, varName
    )
import "ghc-lib-parser" Unique (Unique, mkUnique)

type UniqueCounter = Int

data RewriteState = RewriteState
    { uniqueCounter :: !UniqueCounter
    , varEnv :: !(VarEnv Var)
    }

-- | Generate a new unique from our local counter
getNewUnique :: State RewriteState Unique
getNewUnique = do
    -- 'V' is unused by GHC's unique supplies and wired-in names.
    unique <- gets $ mkUnique 'V' . uniqueCounter
    modify' $ \s -> s { uniqueCounter = uniqueCounter s + 1 }
    pure unique

-- | Restore the environment after an action without rewinding the unique counter.
preservingScope :: State RewriteState a -> State RewriteState a
preservingScope action = do
    scope <- gets varEnv
    result <- action
    modify' $ \s -> s { varEnv = scope }
    pure result

-- | Give local system binders deterministic uniques before conversion to LF.
-- GHC's global unique supply depends on how dependency interfaces were loaded.
-- Keep this Core conversion-local: top-level identities, types and IdInfo still
-- refer to the original GHC session.
rewriteUniques :: CoreModule -> CoreModule
rewriteUniques core = core
    { cm_binds = evalState (traverse rewriteTopBind $ cm_binds core)
        (RewriteState 0 emptyVarEnv)
    }
  where
    rewriteTopBind :: CoreBind -> State RewriteState CoreBind
    rewriteTopBind (NonRec binder rhs) =
        NonRec binder <$> rewriteExpr rhs
    rewriteTopBind (Rec binds) =
        Rec <$> traverse (secondM rewriteExpr) binds

-- | Rewrite a binder and extend the current scope.
rewriteBinder :: Var -> State RewriteState Var
rewriteBinder binder
    | isNonCoVarId binder && isSystemName (varName binder) = do
        unique <- getNewUnique
        let binder' = setVarUnique binder unique
        modify' $ \s -> s { varEnv = extendVarEnv (varEnv s) binder binder' }
        pure binder'
    | otherwise = pure binder

-- | Rewrite a unique if present in the VarEnv
rewriteOccurrence :: Var -> State RewriteState Var
rewriteOccurrence var = gets $ \s -> maybe var id (lookupVarEnv (varEnv s) var)

rewriteBind :: CoreBind -> State RewriteState CoreBind
rewriteBind (NonRec binder rhs) = do
    -- Rewrite expression before binder to avoid the non-rec bind being in scope for the expression
    rhs' <- rewriteExpr rhs
    binder' <- rewriteBinder binder
    pure (NonRec binder' rhs')
rewriteBind (Rec binds) = do
    -- Rewrite all binders before any expressions, to ensure they're all in scope
    binders' <- traverse (rewriteBinder . fst) binds
    rhss' <- traverse (rewriteExpr . snd) binds
    pure (Rec (zip binders' rhss'))

rewriteExpr :: CoreExpr -> State RewriteState CoreExpr
rewriteExpr expr = case expr of
    Var var -> Var <$> rewriteOccurrence var
    Lit lit -> pure $ Lit lit
    App fun arg -> App <$> rewriteExpr fun <*> rewriteExpr arg
    Lam binder body -> preservingScope $ do
        binder' <- rewriteBinder binder
        body' <- rewriteExpr body
        pure $ Lam binder' body'
    Let bind body -> preservingScope $ do
        bind' <- rewriteBind bind
        body' <- rewriteExpr body
        pure $ Let bind' body'
    Case scrut binder ty alts -> preservingScope $ do
        scrut' <- rewriteExpr scrut
        binder' <- rewriteBinder binder
        alts' <- traverse (\(con, binders, rhs) -> preservingScope $ do
            binders' <- traverse rewriteBinder binders
            rhs' <- rewriteExpr rhs
            pure (con, binders', rhs')) alts
        pure $ Case scrut' binder' ty alts'
    Cast body co -> (`Cast` co) <$> rewriteExpr body
    Tick tick body -> do
        tick' <- case tick of
            Breakpoint n vars -> Breakpoint n <$> traverse rewriteOccurrence vars
            _ -> pure tick
        Tick tick' <$> rewriteExpr body
    Type ty -> pure $ Type ty
    Coercion co -> pure $ Coercion co
