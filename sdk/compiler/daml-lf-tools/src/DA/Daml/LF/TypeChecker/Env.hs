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
    diagnosticWithContext,
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
    Gamma(..),
    emptyGamma,
    SomeErrorOrWarning(..),
    getWarningStatusF,
    damlWarningFlags,
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
  , _damlWarningFlags :: [DamlWarningFlag]
    -- ^ Function for relaxing errors into warnings and strictifying warnings into errors
  }

makeLenses ''Gamma

class SomeErrorOrWarning d where
  diagnosticWithContextF :: forall m gamma. MonadGammaF gamma m => Getter gamma Gamma -> d -> m ()

getLfVersion :: MonadGamma m => m Version
getLfVersion = view lfVersion

getWarningStatusF :: forall m gamma. MonadGammaF gamma m => Getter gamma Gamma -> WarnableError -> m DamlWarningFlagStatus
getWarningStatusF getter warnableError = do
  flags <- view (getter . damlWarningFlags)
  pure (getWarningStatus flags warnableError)

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
match :: MonadGamma m => Prism' a b -> UnwarnableError -> a -> m b
match p e x = either (const (throwWithContext e)) pure (matching p x)

-- | Environment containing only the packages in scope but no type or term
-- variables.
emptyGamma :: World -> Version -> Gamma
emptyGamma world version = Gamma ContextNone mempty mempty world version []

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

diagnosticWithContext :: (SomeErrorOrWarning d, MonadGamma m) => d -> m ()
diagnosticWithContext = diagnosticWithContextF id

throwWithContext :: MonadGamma m => UnwarnableError -> m a
throwWithContext = throwWithContextF id

warnWithContext :: MonadGamma m => StandaloneWarning -> m ()
warnWithContext = warnWithContextF id

withContext :: MonadGamma m => Context -> m b -> m b
withContext = withContextF id

catchAndRethrow :: MonadGamma m => (Error -> Error) -> m b -> m b
catchAndRethrow handler mb = catchError mb $ throwWithContextFRaw id . handler

throwWithContextF :: forall m gamma a. MonadGammaF gamma m => Getter gamma Gamma -> UnwarnableError -> m a
throwWithContextF getter err = throwWithContextFRaw getter (EUnwarnableError err)

throwWithContextFRaw :: forall m gamma a. MonadGammaF gamma m => Getter gamma Gamma -> Error -> m a
throwWithContextFRaw getter err = do
  ctx <- view $ getter . locCtx
  throwError $ EContext ctx err

warnWithContextF :: forall m gamma. MonadGammaF gamma m => Getter gamma Gamma -> StandaloneWarning -> m ()
warnWithContextF getter warning = warnWithContextFRaw getter (WStandaloneWarning warning)

warnWithContextFRaw :: forall m gamma. MonadGammaF gamma m => Getter gamma Gamma -> Warning -> m ()
warnWithContextFRaw getter warning = do
  ctx <- view $ getter . locCtx
  modify' (WContext ctx warning :)

withContextF :: MonadGammaF gamma m => Setter' gamma Gamma -> Context -> m b -> m b
withContextF setter newCtx = local (over (setter . locCtx) setCtx)
  where
    setCtx :: Context -> Context
    setCtx oldCtx =
      case (oldCtx, newCtx) of
        (ContextDefUpgrading {}, ContextDefUpgrading {}) -> newCtx
        (ContextDefUpgrading { cduPkgName, cduPkgVersion, cduIsDependency }, _) -> ContextDefUpgrading cduPkgName cduPkgVersion newCtx cduIsDependency
        (_, _) -> newCtx

instance SomeErrorOrWarning UnwarnableError where
  diagnosticWithContextF = throwWithContextF

instance SomeErrorOrWarning WarnableError where
  diagnosticWithContextF getter err = do
    status <- getWarningStatusF getter err
    case status of
      AsError -> throwWithContextFRaw getter (EWarnableError err)
      AsWarning -> warnWithContextFRaw getter (WErrorToWarning err)
      Hidden -> pure ()

instance SomeErrorOrWarning StandaloneWarning where
  diagnosticWithContextF = warnWithContextF
