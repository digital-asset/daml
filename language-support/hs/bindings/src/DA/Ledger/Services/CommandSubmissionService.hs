-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.CommandSubmissionService (submit) where

import Data.Functor
import Com.Daml.Ledger.Api.V1.CommandSubmissionService
import DA.Ledger.Convert (lowerCommands)
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types (Commands)
import Google.Protobuf.Empty (Empty(..))
import Network.GRPC.HighLevel.Generated

submit :: Commands -> LedgerService (Either String ())
submit commands =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- commandSubmissionServiceClient client
        let CommandSubmissionService rpc = service
        let request = SubmitRequest (Just (lowerCommands commands))
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrapWithCommandSubmissionFailure
            <&> fmap (\Empty{} -> ())
