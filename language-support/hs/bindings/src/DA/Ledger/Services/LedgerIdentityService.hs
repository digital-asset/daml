-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Services.LedgerIdentityService (getLedgerIdentity) where

import Com.Digitalasset.Ledger.Api.V1.LedgerIdentityService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types(LedgerId(..))
import Network.GRPC.HighLevel.Generated

getLedgerIdentity :: LedgerService LedgerId
getLedgerIdentity =
    makeLedgerService $ \timeout config -> do
    let request = GetLedgerIdentityRequest noTrace
    withGRPCClient config $ \client -> do
        service <- ledgerIdentityServiceClient client
        let LedgerIdentityService{ledgerIdentityServiceGetLedgerIdentity=rpc} = service
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        GetLedgerIdentityResponse text <- unwrap response
        return $ LedgerId text
