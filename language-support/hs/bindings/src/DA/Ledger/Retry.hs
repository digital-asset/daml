-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Retry (ledgerRetry) where

import Control.Monad (when)
import Control.Monad.Catch(Handler(..))
import Network.GRPC.HighLevel.Generated(GRPCIOError(..),StatusCode(..))
import Control.Retry qualified as Retry

enableDevPrint :: Bool
enableDevPrint = False

ledgerRetry :: IO a -> IO a
ledgerRetry io =
    recover $ \rs -> do
    when (enableDevPrint && Retry.rsIterNumber rs > 0) $ print rs
    io

recover :: (Retry.RetryStatus -> IO a) -> IO a
recover = Retry.recovering policy [
    \_ -> Handler $ \(e::GRPCIOError) ->
        return $ case e of
            GRPCIOTimeout -> True
            GRPCIOBadStatusCode StatusUnavailable _ -> True
            _ -> False
    ]

policy :: Retry.RetryPolicy
policy =
    -- do not wait for more than 5 secs
    Retry.limitRetriesByCumulativeDelay (5 * 1000 * 1000) $
    Retry.capDelay (1 * 1000 * 1000) $ -- wait at most one second
    Retry.exponentialBackoff (25 * 1000)
