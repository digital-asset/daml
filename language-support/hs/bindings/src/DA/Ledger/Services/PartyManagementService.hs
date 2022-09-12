-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Services.PartyManagementService (
    getParticipantId, ParticipantId(..),
    listKnownParties, PartyDetails(..),
    allocateParty, AllocatePartyRequest(..),
    ObjectMeta(..),
    ) where

import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import Data.Functor
import Data.Map
import Data.Text.Lazy (Text)
import Network.GRPC.HighLevel.Generated
import qualified Data.Aeson as A
import Data.Aeson ((.:))
import qualified Com.Daml.Ledger.Api.V1.Admin.ObjectMeta as LapiObjectMeta
import qualified Com.Daml.Ledger.Api.V1.Admin.PartyManagementService as LL

getParticipantId :: LedgerService ParticipantId
getParticipantId =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.partyManagementServiceClient client
        let LL.PartyManagementService{partyManagementServiceGetParticipantId=rpc} = service
        let request = LL.GetParticipantIdRequest{}
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrap
            <&> \(LL.GetParticipantIdResponse text) -> ParticipantId text

data ObjectMeta = ObjectMeta
    { resourceVersion :: Text
    , annotations :: Map Text Text
    } deriving (Eq,Ord,Show)

instance A.FromJSON ObjectMeta where
  parseJSON =
    A.withObject "ObjectMeta" $ \v ->
      ObjectMeta
        <$> v .: "resourceVersion"
        <*> v .: "annotations"

data PartyDetails = PartyDetails
    { party :: Party
    , displayName :: Text
    , isLocal :: Bool
    , metadata :: Maybe ObjectMeta
    } deriving (Eq,Ord,Show)

instance A.FromJSON PartyDetails where
  parseJSON =
    A.withObject "PartyDetails" $ \v ->
      PartyDetails
        <$> v .: "identifier"
        <*> v .: "displayName"
        <*> v .: "isLocal"
        <*> v .: "metadata"

listKnownParties :: LedgerService [PartyDetails]
listKnownParties =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.partyManagementServiceClient client
        let LL.PartyManagementService{partyManagementServiceListKnownParties=rpc} = service
        let request = LL.ListKnownPartiesRequest{}
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrap
            >>= \(LL.ListKnownPartiesResponse xs) ->
                    either (fail . show) return $ raiseList raisePartyDetails xs

raiseObjectMeta :: Maybe LapiObjectMeta.ObjectMeta -> Perhaps ObjectMeta
raiseObjectMeta = \case
    Just LapiObjectMeta.ObjectMeta{..} -> do
        let resourceVersion = objectMetaResourceVersion
        let annotations = objectMetaAnnotations
        return $ ObjectMeta {..}
    Nothing -> do
        let resourceVersion = ""
        let annotations = Data.Map.empty
        return $ ObjectMeta {..}

raisePartyDetails :: LL.PartyDetails -> Perhaps PartyDetails
raisePartyDetails = \case
    LL.PartyDetails{..} -> do
        party <- raiseParty partyDetailsParty
        metadataA <- raiseObjectMeta partyDetailsLocalMetadata
        let metadata = Just metadataA
        let displayName = partyDetailsDisplayName
        let isLocal = partyDetailsIsLocal
        return $ PartyDetails {..}

data AllocatePartyRequest = AllocatePartyRequest
    { partyIdHint :: Text
    , displayName :: Text
    , metadata :: Maybe ObjectMeta
    }

lowerObjectMeta :: ObjectMeta -> LapiObjectMeta.ObjectMeta
lowerObjectMeta ObjectMeta{..} = do
    let objectMetaResourceVersion = resourceVersion
    let objectMetaAnnotations = annotations
    LapiObjectMeta.ObjectMeta {..}

allocateParty :: AllocatePartyRequest -> LedgerService PartyDetails
allocateParty request =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.partyManagementServiceClient client
        let LL.PartyManagementService{partyManagementServiceAllocateParty=rpc} = service
        rpc (ClientNormalRequest (lowerRequest request) timeout mdm)
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
            let allocatePartyRequestLocalMetadata = fmap lowerObjectMeta metadata
            LL.AllocatePartyRequest {..}



