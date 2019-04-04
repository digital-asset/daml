-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Extended version of "Control.Monad.Except" that provides
-- extra utility functions.
module Control.Monad.Except.Extended
    (
      module Control.Monad.Except
    , module Control.Monad.Trans.Except

      -- * Convenience functions for converting between error types
    , errorIfNothing
    , errorIfFalse

      -- * Handling and running except monad transformers
    , handleExceptT
    , rethrowExceptT
    , handleExcept
    , rethrowExcept
    , squashExceptT
    , runDefaultExceptT
    ) where

import           Control.Monad.Except
import           Control.Monad.Trans.Except


-- | Throw an error if the argument is 'Nothing' and pack up the value
-- otherwise.
errorIfNothing :: MonadError e m => e -> Maybe a -> m a
errorIfNothing err = maybe (throwError err) return

-- | Throw an error if the argument is 'False' and pack up the value
-- otherwise.
errorIfFalse :: MonadError e m => e -> Bool -> m ()
errorIfFalse err condition = unless condition $ throwError err

-- | Run an 'ExceptT' computation and immediatly handle the returned errors.
handleExceptT
  :: Monad m
  => (e -> m b)
  -> ExceptT e m b
  -> m b
handleExceptT handler act =
    runExceptT act >>= \case
      Left e -> handler e
      Right x -> return x

-- | Run an 'Except' computation and immediatly handle the returned errors.
handleExcept
  :: Monad m
  => (e -> m b)
  -> Except e b
  -> m b
handleExcept handler act =
    case runExcept act of
      Left e -> handler e
      Right x -> return x

-- | Run an 'ExceptT' computation and immediatly rethrow errors adapted to the
-- error type of the outer monad.
rethrowExceptT
  :: MonadError e' m
  => (e -> e')
  -> ExceptT e m b
  -> m b
rethrowExceptT = handleExceptT . (throwError .)

-- | Run an 'Except' computation and immediatly rethrow errors adapted to the
-- error type of the outer monad.
rethrowExcept
  :: MonadError e' m
  => (e -> e')
  -> Except e a
  -> m a
rethrowExcept = handleExcept . (throwError .)

-- | Squash a nesting of two ExceptT transformers into one by translating the
-- inner error to the outer.
squashExceptT
    :: Monad m
    => (err' -> err)
    -> ExceptT err' (ExceptT err m) a -> ExceptT err m a
squashExceptT f m = do
    errOrResult <- runExceptT m
    case errOrResult of
      Left e -> throwError (f e)
      Right a -> return a

-- | Run an 'ExceptT' and return a default value in case of an error.
runDefaultExceptT :: Monad m => a -> ExceptT e m a -> m a
runDefaultExceptT def = handleExceptT (\_ -> pure def)
