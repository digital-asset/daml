-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.LedgerConfigurationService(getLedgerConfiguration) where

import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Control.Concurrent (forkIO)
import Network.GRPC.HighLevel.Generated
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.Convert
import qualified Com.Digitalasset.Ledger.Api.V1.LedgerConfigurationService as LL

getLedgerConfiguration :: LedgerId -> LedgerService (Stream LedgerConfiguration)
getLedgerConfiguration lid =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    let request = LL.GetLedgerConfigurationRequest (unLedgerId lid) noTrace
    _ <- forkIO $
        withGRPCClient config $ \client -> do
            service <- LL.ledgerConfigurationServiceClient client
            let LL.LedgerConfigurationService {ledgerConfigurationServiceGetLedgerConfiguration=rpc} = service
            sendToStream timeout request raiseGetLedgerConfigurationResponse stream rpc
    return stream
