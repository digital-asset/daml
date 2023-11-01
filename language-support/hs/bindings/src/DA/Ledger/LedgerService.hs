-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Abstraction for LedgerService, which can be composed monadically.
module DA.Ledger.LedgerService (
    LedgerService, runLedgerService, makeLedgerService, TimeoutSeconds,
    Token(..), setToken,
    askTimeout,
    ) where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader,local,asks)
import Control.Monad.Trans.Reader (ReaderT(..))
import DA.Ledger.Retry (ledgerRetry)
import Data.List
import Network.GRPC.HighLevel.Client(TimeoutSeconds)
import Network.GRPC.HighLevel.Generated(ClientConfig,MetadataMap(..))
import UnliftIO(MonadUnliftIO)
import qualified Data.ByteString.UTF8 as BSU8
import qualified Data.Map as Map

data Context = Context
  { ts :: TimeoutSeconds
  , cc :: ClientConfig
  , tokMaybe :: Maybe Token
  }

newtype Token = Token String

newtype LedgerService a = LedgerService (ReaderT Context IO a)
  deriving ( Functor,Applicative,Monad,MonadFail,MonadIO,MonadUnliftIO
           , MonadReader Context )

runLedgerService :: LedgerService a -> TimeoutSeconds -> ClientConfig -> IO a
runLedgerService (LedgerService r) ts cc =
  runReaderT r $ Context { ts, cc, tokMaybe = Nothing }

setToken :: Token -> LedgerService a -> LedgerService a
setToken tok = local $ \context -> context { tokMaybe = Just tok }


makeLedgerService :: (TimeoutSeconds -> ClientConfig -> MetadataMap -> IO a) -> LedgerService a
makeLedgerService f = do
  LedgerService $ ReaderT $ \Context{ts,cc,tokMaybe} ->
    ledgerRetry $ f ts cc (makeMdm tokMaybe)

makeMdm :: Maybe Token -> MetadataMap
makeMdm = \case
  Nothing -> MetadataMap Map.empty
  Just (Token tok) ->
      -- This matches how the com.daml.ledger.api.auth.client.LedgerCallCredentials
      -- behaves.
      let tok' | "Bearer " `isPrefixOf` tok = tok
               | otherwise = "Bearer " <> tok
      in MetadataMap $ Map.fromList
             [ ( "authorization"
               , [ BSU8.fromString tok' ]
               )
             ]

askTimeout :: LedgerService TimeoutSeconds
askTimeout = asks ts
