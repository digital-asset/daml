-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.LedgerConfigurationService(getLedgerConfiguration) where

import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.Convert
import qualified Com.Digitalasset.Ledger.Api.V1.LedgerConfigurationService as LL

getLedgerConfiguration :: LedgerId -> LedgerService (Stream LedgerConfiguration)
getLedgerConfiguration lid =
    makeLedgerService $ \timeout config -> do
    let request = LL.GetLedgerConfigurationRequest (unLedgerId lid) noTrace
    asyncStreamGen $ \stream ->
        withGRPCClient config $ \client -> do
            service <- LL.ledgerConfigurationServiceClient client
            let LL.LedgerConfigurationService {ledgerConfigurationServiceGetLedgerConfiguration=rpc} = service
            sendToStream timeout request raiseGetLedgerConfigurationResponse stream rpc
