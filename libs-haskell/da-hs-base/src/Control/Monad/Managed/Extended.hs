-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}
module Control.Monad.Managed.Extended
  ( module Control.Monad.Managed
  , mapManaged
  ) where

import Control.Monad.Managed

-- | Map over the 'IO' computation inside a 'Managed'. This can be used, e.g.,
-- to catch exceptions, log and rethrow them.
mapManaged :: (forall b. IO b -> IO b) -> Managed a -> Managed a
mapManaged f m = managed (f . with m)
