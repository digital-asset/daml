-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Services.CommandCompletionService (completionStream) where

import Com.Digitalasset.Ledger.Api.V1.CommandCompletionService
import Com.Digitalasset.Ledger.Api.V1.LedgerOffset
import Control.Concurrent (forkIO)
import DA.Ledger.Convert (raiseCompletion,RaiseFailureReason)
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Data.Vector as Vector

-- TODO:: all the other RPCs

type Request = (LedgerId,ApplicationId,[Party],Maybe LedgerOffset)

--type Response = (Maybe Checkpoint,[Completion])
--data Checkpoint

--completionStream :: Request -> LedgerService (Stream Response) -- GOAL
completionStream :: Request -> LedgerService (Stream Completion)
completionStream (lid,aid,partys,offset) =
    makeLedgerService $ \(TimeoutSeconds timeout) config -> do
    stream <- newStream
    let request = mkCompletionStreamRequest lid aid partys offset
    _ <- forkIO $
        withGRPCClient config $ \client -> do
            service <- commandCompletionServiceClient client
            let CommandCompletionService {commandCompletionServiceCompletionStream=rpc} = service
            sendToStream timeout request f stream rpc
    return stream
    where
        f = map (raise . raiseCompletion) . Vector.toList . completionStreamResponseCompletions

raise :: Either RaiseFailureReason Completion -> Either Closed Completion
raise x = case x of
    Left reason ->
        Left (Abnormal $ "failed to parse transaction because: " <> show reason <> ":\n" <> show x)
    Right h -> Right h

mkCompletionStreamRequest :: LedgerId -> ApplicationId -> [Party] -> Maybe LedgerOffset -> CompletionStreamRequest
mkCompletionStreamRequest (LedgerId id) aid parties maybeOffset = CompletionStreamRequest {
    completionStreamRequestLedgerId = id,
    completionStreamRequestApplicationId = unApplicationId aid,
    completionStreamRequestParties = Vector.fromList (map unParty parties),
    completionStreamRequestOffset = maybeOffset
    }
