-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.CommandCompletionService (completionStream, completionEnd) where

import Com.Digitalasset.Ledger.Api.V1.CommandCompletionService hiding (Checkpoint)
import Control.Concurrent (forkIO)
import DA.Ledger.Convert
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
    completionStreamRequestOffset = Just (lowerLedgerOffset offset)
    }

completionEnd :: LedgerId -> LedgerService AbsOffset
completionEnd lid =
    makeLedgerService $ \timeout config ->
    withGRPCClient config $ \client -> do
        service <- commandCompletionServiceClient client
        let CommandCompletionService {commandCompletionServiceCompletionEnd=rpc} = service
        let request = CompletionEndRequest (unLedgerId lid) noTrace
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        unwrap response >>= \case
            CompletionEndResponse (Just offset) ->
                case raiseAbsLedgerOffset offset of
                    Left reason -> fail (show reason)
                    Right abs -> return abs
            CompletionEndResponse Nothing ->
                fail "CompletionEndResponse, offset field is missing"
