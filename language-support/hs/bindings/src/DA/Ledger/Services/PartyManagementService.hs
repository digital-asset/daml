-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Services.PartyManagementService (
    getParticipantId, ParticipantId(..),
    listKnownParties, PartyDetails(..),
    allocateParty, AllocatePartyRequest(..),
    ) where

import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Data.Functor
import Data.Text.Lazy (Text)
import Network.GRPC.HighLevel.Generated
import qualified Com.Digitalasset.Ledger.Api.V1.Admin.PartyManagementService as LL

newtype ParticipantId = ParticipantId { unParticipantId :: Text} deriving (Eq,Ord,Show)

getParticipantId :: LedgerService ParticipantId
getParticipantId =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.partyManagementServiceClient client
        let LL.PartyManagementService{partyManagementServiceGetParticipantId=rpc} = service
        let request = LL.GetParticipantIdRequest{}
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= unwrap
            <&> \(LL.GetParticipantIdResponse text) -> ParticipantId text

data PartyDetails = PartyDetails
    { party :: Party
    , displayName :: Text
    , isLocal :: Bool
    } deriving (Eq,Ord,Show)

listKnownParties :: LedgerService [PartyDetails]
listKnownParties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.partyManagementServiceClient client
        let LL.PartyManagementService{partyManagementServiceListKnownParties=rpc} = service
        let request = LL.ListKnownPartiesRequest{}
        rpc (ClientNormalRequest request timeout emptyMdm)
            >>= unwrap
            >>= \(LL.ListKnownPartiesResponse xs) ->
                    either (fail . show) return $ raiseList raisePartyDetails xs

raisePartyDetails :: LL.PartyDetails -> Perhaps PartyDetails
raisePartyDetails = \case
    LL.PartyDetails{..} -> do
        party <- raiseParty partyDetailsParty
        let displayName = partyDetailsDisplayName
        let isLocal = partyDetailsIsLocal
        return $ PartyDetails {..}

data AllocatePartyRequest = AllocatePartyRequest
    { partyIdHint :: Text
    , displayName :: Text
    }

allocateParty :: AllocatePartyRequest -> LedgerService PartyDetails
allocateParty request =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.partyManagementServiceClient client
        let LL.PartyManagementService{partyManagementServiceAllocateParty=rpc} = service
        rpc (ClientNormalRequest (lowerRequest request) timeout emptyMdm)
            >>= unwrap
            >>= \case
            LL.AllocatePartyResponse Nothing ->
                fail "AllocatePartyResponse, party_details field is missing"
            LL.AllocatePartyResponse (Just x) ->
                either (fail . show) return $ raisePartyDetails x
    where
        lowerRequest :: AllocatePartyRequest -> LL.AllocatePartyRequest
        lowerRequest AllocatePartyRequest{..} = do
            let allocatePartyRequestPartyIdHint = partyIdHint
            let allocatePartyRequestDisplayName = displayName
            LL.AllocatePartyRequest {..}
