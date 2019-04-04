-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
-- | This module provides the data type for the environment of the DAML-LF type
-- checker and functions to manipulate it.
module DA.Daml.LF.TypeChecker.Env where

import           Control.Lens hiding (Context)
import           Control.Monad.Error.Class (MonadError (..))
import           Control.Monad.Extra
import           Control.Monad.Reader
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS

import           DA.Daml.LF.Ast
import           DA.Daml.LF.TypeChecker.Error

-- | The environment for the DAML-LF type checker.
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
    -- ^ The DAML-LF version of the package being type checked.
  }

-- | Type class constraint capturing the needed monadic effects for the
-- functions manipulating the type checker environment.
type MonadGamma m = (MonadError Error m, MonadReader Gamma m)

makeLenses ''Gamma

runGamma
  :: World
  -> Version
  -> ReaderT Gamma (Either Error) a
  -> Either Error a
runGamma world0 version act = runReaderT act (emptyGamma world0 version)

-- | Helper function which tries to match on a prism and fails with a provided
-- error in case is does not match.
match :: MonadGamma m => Prism' a b -> Error -> a -> m b
match p e x = either (const (throwWithContext e)) pure (matching p x)

-- | Environment containing only the packages in scope but no type or term
-- variables.
emptyGamma :: World -> Version -> Gamma
emptyGamma = Gamma ContextNone mempty mempty

-- | Run a computation in the current environment extended by a new type
-- variable. Fails if the type variable would shadow some existing type
-- variable.
introTypeVar :: MonadGamma m => TypeVarName -> Kind -> m a -> m a
introTypeVar v k act = do
  whenM (views tvars (HMS.member v)) $ throwWithContext (EShadowingTypeVar v)
  local (tvars . at v ?~ k) act

-- -- | Introduce a fresh type variable to the environment. Uses the given name as
-- -- starting point for searching a fresh name. The fresh name finally found will
-- -- then be put in the enviroment and passed to the continuation.
-- introFreshTypeVar :: MonadGamma m => TypeVarName -> (TypeVarName -> m a) -> m a
-- introFreshTypeVar v act = do
--   let vs = v : map (\n -> fmap (<> T.pack (show n)) v) [1::Int ..]
--   -- NOTE(MH): This 'findM' cannot fail since @tvars@ is finite.
--   v' <- fromJust <$> findM (\v' -> not <$> view (tvars . contains v')) vs
--   introTypeVar v' (act v')

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

withContext :: MonadGamma m => Context -> m b -> m b
withContext ctx = local (set locCtx ctx)
