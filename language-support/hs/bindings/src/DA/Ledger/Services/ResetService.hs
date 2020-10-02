-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Services.ResetService (reset) where

import Com.Daml.Ledger.Api.V1.Testing.ResetService
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Google.Protobuf.Empty
import Network.GRPC.HighLevel.Generated

reset :: LedgerId -> LedgerService ()
reset lid =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- resetServiceClient client
        let ResetService {resetServiceReset=rpc} = service
        let request = ResetRequest (unLedgerId lid)
        response <- rpc (ClientNormalRequest request timeout mdm)
        Empty{} <- unwrap response
        return ()
