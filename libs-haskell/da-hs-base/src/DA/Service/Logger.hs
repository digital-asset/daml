-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}

-- | Structured logger with metrics.
--
-- This module is intended to be imported qualified as follows.
--
-- > import qualified Da.Service.Logger as Logger
--
module DA.Service.Logger
  ( Handle(..)
  , Counter(..)
  , Gauge(..)
  , Distribution(..)
  , Priority(..)
  , logDebug
  , logInfo
  , logWarning
  , logError
  , liftHandle
  ) where


import           Control.Monad.Base
import           Control.Monad.Trans.Control
import qualified Data.Aeson                   as Aeson
import qualified Data.Text                    as T
import GHC.Stack
import Data.Data
import GHC.Generics


------------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------------

data Priority
-- Don't change the ordering of this Sum type or you will mess up the Ord
-- instance
    = Debug -- ^ Verbose debug logging.
    | Info  -- ^ Useful information in case an error has to be understood.
    | Warning
      -- ^ These error messages should not occur in a expected usage, and
      -- should be investigated.
    | Error -- ^ Such log messages must never occur in expected usage.
    deriving (Eq, Show, Data, Generic, Ord, Enum, Bounded)

instance Aeson.FromJSON Priority
instance Aeson.ToJSON Priority

-- | Counter for tracking monotonically increasing values such as number of times
-- an operation has been performed.
newtype Counter m = Counter
    { counterAdd :: Int -> m ()
    }

-- | Gauge for tracking variable values such as number of open resources.
data Gauge m = Gauge
    { gaugeAdd :: Int -> m ()
    , gaugeSet :: Int -> m ()
    }

-- | Distribution for tracking statistics in a series of events.
newtype Distribution m = Distribution
    { distributionAdd :: Double -> m ()
      -- ^ @distributionAdd value@ Include the given value into the set of
      -- observed samples over which we compute the distribution's statistics.
      -- NOTE(JM): This is currently only implemented in the IO logger.
    }

-- | Logger service
-- Provides structural logging with dynamic tagging of actions and
-- metrics with counters and gauges.
--
-- "<timestamp> Info [handle tags] [action tags] <source loc> <message>"
--                    ^- stack of handles through 'tagHandle'
--                                  ^- stack of action tags through 'tagAction'
data Handle m = Handle
    {
      logJson :: !(forall a. (Aeson.ToJSON a, HasCallStack) => Priority -> a -> m ())
      -- ^ @logJson handle prio message@ logs the @message@ with Priority @prio@.
      -- The message is logged with a source location.
      -- with the call trace as constructed with tagAction, and with
      -- the handle contexts constructed with tagHandle.

    , tagAction :: !(forall a. T.Text -> m a -> m a)
      -- ^ @tagAction handle tag action@ tags all log messages on @handle@
      -- of @action@ with @tag@. Tagging can be nested.

    , tagHandle :: !(T.Text -> Handle m)
      -- ^ @tagHandle handle tag@ creates a sub-handle with the
      -- added @tag@.
    }

logError :: HasCallStack => Handle m -> T.Text -> m ()
logError h = logJson h Error

logWarning :: HasCallStack => Handle m -> T.Text -> m ()
logWarning h = logJson h Warning

logInfo :: HasCallStack => Handle m -> T.Text -> m ()
logInfo h = logJson h Info

logDebug :: HasCallStack => Handle m -> T.Text -> m ()
logDebug h = logJson h Debug

liftHandle :: MonadBaseControl b m => Handle b -> Handle m
liftHandle h =
  Handle
  { logJson = \p -> liftBase . logJson h p
  , tagAction = liftBaseOp_ . tagAction h
  , tagHandle = liftHandle . tagHandle h
  }
