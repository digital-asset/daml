-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  , Priority(..)
  , logTelemetry
  , logDebug
  , logInfo
  , logWarning
  , logError
  ) where


import qualified Data.Aeson                   as Aeson
import qualified Data.Text                    as T
import GHC.Stack


------------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------------

data Priority
-- Don't change the ordering of this Sum type or you will mess up the Ord
-- instance
    = Telemetry -- ^ Events that are interesting for user metrics.
    | Debug -- ^ Verbose debug logging.
    | Info  -- ^ Useful information in case an error has to be understood.
    | Warning
      -- ^ These error messages should not occur in a expected usage, and
      -- should be investigated.
    | Error -- ^ Such log messages must never occur in expected usage.
    deriving (Eq, Show, Ord, Enum, Bounded)


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

logTelemetry :: HasCallStack => Handle m -> T.Text -> m ()
logTelemetry h = logJson h Telemetry
