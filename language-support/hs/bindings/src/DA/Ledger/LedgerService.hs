-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Abstraction for LedgerService, which can be composed monadically.
module DA.Ledger.LedgerService (
    LedgerService, runLedgerService, makeLedgerService, TimeoutSeconds(..),
    ) where

import Prelude hiding (fail)
import Control.Monad (ap,liftM)
import Control.Monad.Fail (MonadFail,fail)
import Control.Monad.IO.Class (MonadIO,liftIO)
import DA.Ledger.Retry (ledgerRetry)
import Network.GRPC.HighLevel.Generated

newtype TimeoutSeconds = TimeoutSeconds { unTimeoutSeconds :: Int } deriving Num

newtype LedgerService a =
    LedgerService { runLedgerService :: TimeoutSeconds -> ClientConfig -> IO a }
    --deriving (Monad)

-- TODO: How do we avoid the boiler plate here?

instance Functor LedgerService where fmap = liftM
instance Applicative LedgerService where pure = return; (<*>) = ap

instance Monad LedgerService where
    return a = LedgerService $ \_ _ -> return a
    (>>=) = bind

instance MonadIO LedgerService where
    liftIO io = LedgerService $ \_ _ -> io

instance MonadFail LedgerService where
    fail s = LedgerService $ \_ _ -> fail s

makeLedgerService :: (TimeoutSeconds -> ClientConfig -> IO a) -> LedgerService a
makeLedgerService f = LedgerService $ \t cc -> do ledgerRetry (f t cc)

bind :: LedgerService a -> (a -> LedgerService b) -> LedgerService b
bind m f =
    LedgerService $ \to cc -> do
        a <- runLedgerService m to cc
        runLedgerService (f a) to cc
