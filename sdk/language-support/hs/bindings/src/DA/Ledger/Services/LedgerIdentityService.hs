-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Services.LedgerIdentityService (getLedgerIdentity) where

import Com.Daml.Ledger.Api.V1.LedgerIdentityService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types(LedgerId(..))
import Network.GRPC.HighLevel.Generated

getLedgerIdentity :: LedgerService LedgerId
getLedgerIdentity =
    makeLedgerService $ \timeout config mdm -> do
    let request = GetLedgerIdentityRequest
    withGRPCClient config $ \client -> do
        service <- ledgerIdentityServiceClient client
        let LedgerIdentityService{ledgerIdentityServiceGetLedgerIdentity=rpc} = service
        response <- rpc (ClientNormalRequest request timeout mdm)
        GetLedgerIdentityResponse text <- unwrap response
        return $ LedgerId text
