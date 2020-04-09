-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Contexts for DAML LF static verification
module DA.Daml.LF.Verify.Context
  ( Delta
  , MonadDelta, dchs, devars, _devals
  , UpdateSet(..)
  , UpdCreate(..), usCre, usArc, usCho
  , UpdArchive(..)
  , UpdChoice(..)
  , runDelta
  , emptyDelta, setDelta
  , introDelta, extVarDelta
  , lookupDExprVar, lookupDVal, lookupDChoice
  , concatDelta
  , emptyUpdateSet
  , concatUpdateSet
  ) where

import Control.Lens hiding (Context)
import Control.Monad.Error.Class (MonadError (..))
import Control.Monad.Reader
import Data.HashMap.Strict (HashMap, union, empty)

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Error

data UpdCreate = UpdCreate
  { _creTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _creField :: ![(FieldName, Expr)]
    -- ^ The fields to be verified, together with their value.
  }
data UpdArchive = UpdArchive
  { _arcTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _arcField :: ![(FieldName, Expr)]
    -- ^ The fields to be verified, together with their value.
  }
data UpdChoice = UpdChoice
  { _choTemp  :: !(Qualified TypeConName)
    -- ^ Qualified type constructor corresponding to the contract template.
  , _choName  :: !ChoiceName
    -- ^ The name of the choice.
  }

-- | The List of updates being performed
data UpdateSet = UpdateSet
  { _usCre :: ![UpdCreate]
    -- ^ The list of create updates.
  , _usArc :: ![UpdArchive]
    -- ^ The list of archive updates.
  , _usCho :: ![UpdChoice]
    -- ^ The list of choice updates.
  }

makeLenses ''UpdateSet

emptyUpdateSet :: UpdateSet
emptyUpdateSet = UpdateSet [] [] []

concatUpdateSet :: UpdateSet -> UpdateSet -> UpdateSet
concatUpdateSet (UpdateSet cres1 arcs1 chos1) (UpdateSet cres2 arcs2 chos2) =
  UpdateSet (cres1 ++ cres2) (arcs1 ++ arcs2) (chos1 ++ chos2)

-- | The environment for the DAML-LF verifier
data Delta = Delta
  { _devars :: ![ExprVarName]
    -- ^ The skolemised term variables.
  , _devals :: !(HashMap (Qualified ExprValName) (Expr, UpdateSet))
    -- ^ The bound values.
  , _dchs :: !(HashMap (Qualified TypeConName, ChoiceName) UpdateSet)
    -- ^ The set of relevant choices.
  -- TODO: split this off into data types for readability?
  }

makeLenses ''Delta

emptyDelta :: Delta
emptyDelta = Delta [] empty empty

concatDelta :: Delta -> Delta -> Delta
concatDelta (Delta vars1 vals1 chs1) (Delta vars2 vals2 chs2) =
  Delta (vars1 ++ vars2) (vals1 `union` vals2) (chs1 `union` chs2)
  -- TODO: union makes me slightly nervous, as it allows overlapping keys
  -- (and just uses the first)

-- | Type class constraint with the required monadic effects for functions
-- manipulating the verification environment.
type MonadDelta m = (MonadError Error m, MonadReader Delta m)

runDelta :: ReaderT Delta (Either Error) a -> Either Error a
runDelta act = runReaderT act emptyDelta

-- | Run a computation in the current environment, with an additional
-- environment extension.
introDelta :: MonadDelta m => Delta -> m a -> m a
introDelta delta = local (concatDelta delta)

-- TODO: This is a bit strange in a reader monad.
-- Figure out a way to extend, instead of overwrite every time.
setDelta :: MonadDelta m => Delta -> m a -> m a
setDelta delta = local (const delta)

extVarDelta :: MonadDelta m => ExprVarName -> m a -> m a
extVarDelta x = local (over devars ((:) x))

lookupDExprVar :: MonadDelta m => ExprVarName -> m ()
lookupDExprVar x = ask >>= \ del -> unless (elem x $ _devars del)
                                          $ throwError $ EUnknownExprVar x

lookupDVal :: MonadDelta m => Qualified ExprValName -> m (Expr, UpdateSet)
lookupDVal w = view (devals . at w) >>= match _Just EEmptyCase
-- TODO: This is a random error. The thing we really want to write is:
-- lookupDVal w = view (devals . at w) >>= match _Just (EUnknownDefinition $ LEValue w)
-- The issue here is that our values are currently stored in Delta instead
-- of in the world like in the type checker.
-- This means that we can't throw the error we want to throw here.
-- 2 options:
--   + either define our own errors, as they don't correspond 100% to the type
--   checking errors anyway.
--   + or create our own world environment to store values. This also makes
--   sense as these values work similar to how they work in the type checker,
--   except that we need to store a partially evaluated definition as well.
-- Both approaches make sense, but both imply a lot of code duplication, so they
-- don't sound that enticing...

lookupDChoice :: MonadDelta m => Qualified TypeConName -> ChoiceName
             -> m UpdateSet
lookupDChoice tem ch = view (dchs . at (tem, ch)) >>= match _Just EEmptyCase
-- TODO: Random error.

-- | Helper functions mirrored from Env.
-- TODO: Reduce duplication by abstracting over MonadGamma and MonadDelta?
match :: MonadDelta m => Prism' a b -> Error -> a -> m b
match p e x = either (const (throwError e)) pure (matching p x)
-- TODO: no context, like in Gamma

