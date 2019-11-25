-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Abstraction for LedgerService, which can be composed monadically.
module DA.Ledger.LedgerService (
    LedgerService, runLedgerService, makeLedgerService, TimeoutSeconds,
    Jwt, setToken,
    askTimeout,
    ) where

import Control.Monad.Fail (MonadFail)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader,local,asks)
import Control.Monad.Trans.Reader (ReaderT(..))
import DA.Ledger.Jwt (Jwt)
import DA.Ledger.Retry (ledgerRetry)
import Network.GRPC.HighLevel.Client(TimeoutSeconds)
import Network.GRPC.HighLevel.Generated(ClientConfig,MetadataMap(..))
import UnliftIO(MonadUnliftIO)
import qualified DA.Ledger.Jwt as Jwt (toString)
import qualified Data.ByteString.UTF8 as BSU8
import qualified Data.Map as Map
import qualified Data.SortedList as SortedList

data Context = Context
  { ts :: TimeoutSeconds
  , cc :: ClientConfig
  , jwtMaybe :: Maybe Jwt
  }

newtype LedgerService a = LedgerService (ReaderT Context IO a)
  deriving ( Functor,Applicative,Monad,MonadFail,MonadIO,MonadUnliftIO
           , MonadReader Context )

runLedgerService :: LedgerService a -> TimeoutSeconds -> ClientConfig -> IO a
runLedgerService (LedgerService r) ts cc =
  runReaderT r $ Context { ts, cc, jwtMaybe = Nothing }

setToken :: Jwt -> LedgerService a -> LedgerService a
setToken jwt = local $ \context -> context { jwtMaybe = Just jwt }

makeLedgerService :: (TimeoutSeconds -> ClientConfig -> MetadataMap -> IO a) -> LedgerService a
makeLedgerService f = do
  LedgerService $ ReaderT $ \Context{ts,cc,jwtMaybe} ->
    ledgerRetry $ f ts cc (makeMdm jwtMaybe)

makeMdm :: Maybe Jwt -> MetadataMap
makeMdm = \case
  Nothing -> MetadataMap Map.empty
  Just jwt -> MetadataMap $ Map.fromList [
    ("authorization",
     SortedList.toSortedList [ BSU8.fromString $ "Bearer " <> Jwt.toString jwt ])]

askTimeout :: LedgerService TimeoutSeconds
askTimeout = asks ts
