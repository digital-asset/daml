-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Services.ResetService (reset) where

import Com.Digitalasset.Ledger.Api.V1.Testing.ResetService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Google.Protobuf.Empty
import Network.GRPC.HighLevel.Generated

reset :: LedgerId -> LedgerService ()
reset lid =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- resetServiceClient client
        let ResetService {resetServiceReset=rpc} = service
        let request = ResetRequest (unLedgerId lid)
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        Empty{} <- unwrap response
        return ()
