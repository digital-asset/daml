-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.CommandCompletionService (completionStream, completionEnd) where

import Com.Digitalasset.Ledger.Api.V1.CommandCompletionService hiding (Checkpoint)
import Com.Digitalasset.Ledger.Api.V1.LedgerOffset
import Control.Concurrent (forkIO)
import DA.Ledger.Convert(raiseCompletionStreamResponse)
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Data.Vector as Vector


type Request = (LedgerId,ApplicationId,[Party],LedgerOffset)
type Response = (Maybe Checkpoint, [Completion])

completionStream :: Request -> LedgerService (Stream Response)
completionStream (lid,aid,partys,offset) =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    let request = mkCompletionStreamRequest lid aid partys offset
    _ <- forkIO $
        withGRPCClient config $ \client -> do
            service <- commandCompletionServiceClient client
            let CommandCompletionService {commandCompletionServiceCompletionStream=rpc} = service
            sendToStream timeout request raiseCompletionStreamResponse stream rpc
    return stream


mkCompletionStreamRequest :: LedgerId -> ApplicationId -> [Party] -> LedgerOffset -> CompletionStreamRequest
mkCompletionStreamRequest (LedgerId id) aid parties offset = CompletionStreamRequest {
    completionStreamRequestLedgerId = id,
    completionStreamRequestApplicationId = unApplicationId aid,
    completionStreamRequestParties = Vector.fromList (map unParty parties),

    -- From: command_completion_service.proto
    -- // Optional, if not set the ledger uses the current ledger end offset instead.
    -- LedgerOffset offset = 4;
    --
    -- which is entirely pointless, as it just results in an empty/closed stream of results
    -- so dont support the optionality in the haskell interface
    completionStreamRequestOffset = Just offset
    }

completionEnd :: LedgerId -> LedgerService LedgerOffset -- TODO: return AbsOffset. must be, although not stated in the .proto commment
completionEnd lid =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- commandCompletionServiceClient client
        let CommandCompletionService {commandCompletionServiceCompletionEnd=rpc} = service
        let request = CompletionEndRequest (unLedgerId lid) noTrace
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= \case
            ClientNormalResponse (CompletionEndResponse (Just offset)) _m1 _m2 _status _details ->
                return offset
            ClientNormalResponse (CompletionEndResponse Nothing) _m1 _m2 _status _details ->
                fail "CompletionEndResponse offset field is missing"
            ClientErrorResponse e ->
                fail (show e)

