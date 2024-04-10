-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

-- | This module provides the data type for the environment of the Daml-LF type
-- checker and functions to manipulate it.
module DA.Daml.LF.TypeChecker.Env(
    MonadGamma,
    MonadGammaF,
    TcM,
    TcMF,
    throwWithContext, throwWithContextF,
    warnWithContext, warnWithContextF,
    catchAndRethrow,
    inWorld,
    match,
    lookupTypeVar,
    introTypeVar,
    introTypeVars,
    introExprVar,
    lookupExprVar,
    withContext, withContextF,
    getLfVersion,
    getWorld,
    runGamma, runGammaF,
    Gamma,
    emptyGamma
    ) where

import           Control.Lens hiding (Context)
import           Control.Monad.Error.Class (MonadError (..))
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.HashMap.Strict (HashMap)

import           DA.Daml.LF.Ast
import           DA.Daml.LF.TypeChecker.Error

-- | The environment for the Daml-LF type checker.
data Gamma = Gamma
  { _locCtx :: !Context
    -- ^ The current type checking context for error reporting.
  , _tvars :: !(HashMap TypeVarName Kind)
    -- ^ The type variables in scope.
  , _evars :: !(HashMap ExprVarName Type)
    -- ^ The term variables in scope and their types.
  , _world :: !World
    -- ^ The packages in scope.
  , _lfVersion :: Version
    -- ^ The Daml-LF version of the package being type checked.
  }

makeLenses ''Gamma

getLfVersion :: MonadGamma m => m Version
getLfVersion = view lfVersion

getWorld :: MonadGamma m => m World
getWorld = view world

-- | Type class constraint capturing the needed monadic effects for the
-- functions manipulating the type checker environment.
type MonadGamma m = MonadGammaF Gamma m
type MonadGammaF gamma m = (MonadError Error m, MonadReader gamma m, MonadState [Warning] m)
type TcMF gamma = ReaderT gamma (StateT [Warning] (Either Error))
type TcM = TcMF Gamma

runGamma
  :: World
  -> Version
  -> ReaderT Gamma (StateT [Warning] (Either Error)) a
  -> Either Error (a, [Warning])
runGamma world0 version act = runGammaF (emptyGamma world0 version) act

runGammaF
  :: gamma
  -> ReaderT gamma (StateT [Warning] (Either Error)) a
  -> Either Error (a, [Warning])
runGammaF gamma act = runStateT (runReaderT act gamma) []

-- | Helper function which tries to match on a prism and fails with a provided
-- error in case is does not match.
match :: MonadGamma m => Prism' a b -> Error -> a -> m b
match p e x = either (const (throwWithContext e)) pure (matching p x)

-- | Environment containing only the packages in scope but no type or term
-- variables.
emptyGamma :: World -> Version -> Gamma
emptyGamma = Gamma ContextNone mempty mempty

-- | Run a computation in the current environment extended by a new type
-- variable/kind binding. Does not fail on shadowing.
introTypeVar :: MonadGamma m => TypeVarName -> Kind -> m a -> m a
introTypeVar v k = local (tvars . at v ?~ k)

-- | Introduce multiple type variables ('introTypeVar' but iterated).
introTypeVars :: MonadGamma m => [(TypeVarName, Kind)] -> m a -> m a
introTypeVars binders m = foldr (uncurry introTypeVar) m binders

-- | Run a computation in the current enviroment extended by a new term
-- variable/type binding. Does not fail on shadowing.
introExprVar :: MonadGamma m => ExprVarName -> Type -> m a -> m a
introExprVar x t = local (evars . at x ?~ t)

-- | Check whether a type variable exists in the current environment. Fails with
-- 'EUnknownTypeVar' if it does not exist.
lookupTypeVar :: MonadGamma m => TypeVarName -> m Kind
lookupTypeVar v =
  view (tvars . at v) >>= match _Just (EUnknownTypeVar v)

-- | Lookup a term variable in the current environment and return its type. Fails
-- with 'EUnknownExprVar' if the variables does not exist.
lookupExprVar :: MonadGamma m => ExprVarName -> m Type
lookupExprVar x =
  view (evars . at x) >>= match _Just (EUnknownExprVar x)

inWorld  :: MonadGamma m => (World -> Either LookupError a) -> m a
inWorld look = do
  w <- view world
  case look w of
    Left e -> throwWithContext (EUnknownDefinition e)
    Right x -> pure x

throwWithContext :: MonadGamma m => Error -> m a
throwWithContext err = do
  ctx <- view locCtx
  throwError $ EContext ctx err

warnWithContext :: MonadGamma m => Warning -> m ()
warnWithContext warning = do
  ctx <- view locCtx
  modify' (WContext ctx warning :)

withContext :: MonadGamma m => Context -> m b -> m b
withContext ctx = local (set locCtx ctx)

catchAndRethrow :: MonadGamma m => (Error -> Error) -> m b -> m b
catchAndRethrow handler mb = catchError mb $ throwWithContext . handler

throwWithContextF :: MonadGammaF gamma m => Getter gamma Gamma -> Error -> m a
throwWithContextF getter err = do
  ctx <- view $ getter . locCtx
  throwError $ EContext ctx err

warnWithContextF :: MonadGammaF gamma m => Getter gamma Gamma -> Warning -> m ()
warnWithContextF getter warning = do
  ctx <- view $ getter . locCtx
  modify' (WContext ctx warning :)

withContextF :: MonadGammaF gamma m => Setter' gamma Gamma -> Context -> m b -> m b
withContextF setter ctx = local (set (setter . locCtx) ctx)
