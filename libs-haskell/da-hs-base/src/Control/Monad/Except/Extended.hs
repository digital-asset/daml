-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Extended version of "Control.Monad.Except" that provides
-- extra utility functions.
module Control.Monad.Except.Extended
    (
      module Control.Monad.Except
    , module Control.Monad.Trans.Except

      -- * Handling and running except monad transformers
    , runDefaultExceptT
    ) where

import           Control.Monad.Except
import           Control.Monad.Trans.Except

-- | Run an 'ExceptT' and return a default value in case of an error.
runDefaultExceptT :: Monad m => a -> ExceptT e m a -> m a
runDefaultExceptT def m = either (const def) id <$> runExceptT m
