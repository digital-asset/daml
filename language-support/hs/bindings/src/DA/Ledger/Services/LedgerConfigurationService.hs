-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.LedgerConfigurationService(getLedgerConfiguration) where

import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.Convert
import Com.Daml.Ledger.Api.V1.LedgerConfigurationService qualified as LL

getLedgerConfiguration :: LedgerId -> LedgerService (Stream LedgerConfiguration)
getLedgerConfiguration lid =
    makeLedgerService $ \timeout config mdm -> do
    let request = LL.GetLedgerConfigurationRequest (unLedgerId lid)
    asyncStreamGen $ \stream ->
        withGRPCClient config $ \client -> do
            service <- LL.ledgerConfigurationServiceClient client
            let LL.LedgerConfigurationService {ledgerConfigurationServiceGetLedgerConfiguration=rpc} = service
            sendToStream timeout mdm request raiseGetLedgerConfigurationResponse stream rpc
