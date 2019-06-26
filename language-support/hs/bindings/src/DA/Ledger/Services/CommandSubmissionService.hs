-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.CommandSubmissionService (submit) where

import Com.Digitalasset.Ledger.Api.V1.CommandSubmissionService
import DA.Ledger.Convert (lowerCommands)
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types (Commands)
import Google.Protobuf.Empty (Empty(..))
import Network.GRPC.HighLevel.Generated

submit :: Commands -> LedgerService (Either String ())
submit commands =
    makeLedgerService $ \(TimeoutSeconds timeout) config ->
    withGRPCClient config $ \client -> do
        service <- commandSubmissionServiceClient client
        let CommandSubmissionService rpc = service
        let request = SubmitRequest (Just (lowerCommands commands)) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse Empty{} _m1 _m2 _status _details ->
                return $ Right ()
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInvalidArgument details)) ->
                return $ Left $ show $ unStatusDetails details
            ClientErrorResponse e ->
                fail (show e)
