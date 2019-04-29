-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}

module LedgerIdentity(
  LedgerId,
  ledgerId,
  Port(..),
  )
where

import qualified Data.Map           as Map
import           Data.String        (fromString)
import qualified Data.Text.Lazy     as Text

import Network.GRPC.HighLevel.Generated (
  withGRPCClient,
  ClientRequest(
      --ClientReaderRequest
      ClientNormalRequest
      ),
  ClientResult(
        --ClientReaderResponse,
      ClientNormalResponse,
      ClientErrorResponse
      ),
  MetadataMap(..),
  GRPCMethodType(Normal
                 --ServerStreaming,
                 ),
  ClientConfig(..),
  Port(..),
  Host(..),
  )

import Network.GRPC.HighLevel.Client(
  TimeoutSeconds,
  )

import Com.Digitalasset.Ledger.Api.V1.TraceContext(TraceContext)
import Com.Digitalasset.Ledger.Api.V1.LedgerIdentityService as LIS
    
data LedgerId = LedgerId String deriving Show

ledgerId :: Port -> IO LedgerId
ledgerId port = do
  withGRPCClient (config port) $ \client -> do
    rpcs <- LIS.ledgerIdentityServiceClient client
    callLedgerIdService rpcs

callLedgerIdService :: LIS.LedgerIdentityService ClientRequest ClientResult -> IO LedgerId
callLedgerIdService (LIS.LedgerIdentityService rpc) = do
  response <- rpc (wrap (LIS.GetLedgerIdentityRequest noTrace))
  LIS.GetLedgerIdentityResponse text <- unwrap response
  let id = LedgerId (Text.unpack text)
  return id

noTrace :: Maybe TraceContext
noTrace = Nothing

wrap :: r -> ClientRequest 'Normal r a
wrap r = ClientNormalRequest r timeout mdm

unwrap :: ClientResult 'Normal a -> IO a
unwrap = \case
  ClientNormalResponse x _m1 _m2 _status _details -> return x
  ClientErrorResponse e -> fail (show e)


config :: Port -> ClientConfig
config port =
  ClientConfig { clientServerHost = Host (fromString "localhost")
               , clientServerPort = port
               , clientArgs = []
               , clientSSLConfig = Nothing
               }

timeout :: TimeoutSeconds
timeout = 1

mdm :: MetadataMap
mdm = MetadataMap Map.empty

