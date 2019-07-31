-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Abstraction for LedgerService, which can be composed monadically.
module DA.Ledger.LedgerService (
    LedgerService, runLedgerService, makeLedgerService, TimeoutSeconds,
    askTimeout,
    ) where

import Control.Monad.Fail (MonadFail)
import Control.Monad.Catch (MonadThrow,MonadCatch)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Reader (ReaderT(..),runReaderT,ask)
import DA.Ledger.Retry (ledgerRetry)
import Network.GRPC.HighLevel.Client(TimeoutSeconds)
import Network.GRPC.HighLevel.Generated(ClientConfig)
import UnliftIO(MonadUnliftIO)

type Context = (TimeoutSeconds,ClientConfig)

newtype LedgerService a = LedgerService (ReaderT Context IO a)
    deriving (Functor,Applicative,Monad,MonadFail,MonadIO,MonadUnliftIO,MonadThrow,MonadCatch)

runLedgerService :: LedgerService a -> TimeoutSeconds -> ClientConfig -> IO a
runLedgerService (LedgerService r) ts cc = runReaderT r (ts,cc)

makeLedgerService :: (TimeoutSeconds -> ClientConfig -> IO a) -> LedgerService a
makeLedgerService f = LedgerService $ ReaderT $ \(ts,cc) -> ledgerRetry $ f ts cc

askTimeout :: LedgerService TimeoutSeconds
askTimeout = LedgerService $ fmap fst ask
