-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Cli.Monad.Timeout
    ( MonadTimeout (..)
    , Duration
    , toMicroseconds
    , microseconds
    , milliseconds
    , seconds
    ) where

import qualified System.Timeout as System
import           Control.Monad.Logger (LoggingT(..))

-- | Specific duration for a timeout.
newtype Duration = Microseconds Int -- don't expose constructor

class Monad m => MonadTimeout m where
    -- | Perform action for given duration, or time out with a nothing value.
    timeout :: Duration -> m t -> m (Maybe t)

instance MonadTimeout IO where
    timeout (Microseconds µs) = System.timeout µs

instance MonadTimeout m => MonadTimeout (LoggingT m) where
    timeout d a = LoggingT $ \log' -> timeout d (runLoggingT a log')

toMicroseconds :: Duration -> Int
toMicroseconds (Microseconds n) = n

microseconds :: Int -> Duration
milliseconds :: Int -> Duration
seconds      :: Int -> Duration
microseconds n = Microseconds  n
milliseconds n = Microseconds (n * 1000)
seconds      n = Microseconds (n * 1000000)
