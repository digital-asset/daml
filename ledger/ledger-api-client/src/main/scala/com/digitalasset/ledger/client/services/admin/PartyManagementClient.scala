// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.admin

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain.{ParticipantId, PartyDetails}
import com.digitalasset.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  GetParticipantIdRequest,
  ListKnownPartiesRequest,
  PartyDetails => ApiPartyDetails
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService

import scala.concurrent.{ExecutionContext, Future}

final class PartyManagementClient(partyManagementService: PartyManagementService)(
    implicit ec: ExecutionContext) {

  private[this] def mapPartyDetails(details: ApiPartyDetails): PartyDetails = {
    PartyDetails(
      Party.assertFromString(details.party),
      if (details.displayName.isEmpty) None else Some(details.displayName),
      details.isLocal)
  }

  def getParticipantId(): Future[ParticipantId] =
    partyManagementService
      .getParticipantId(new GetParticipantIdRequest())
      .map(r => ParticipantId(r.participantId))

  def listKnownParties(): Future[List[PartyDetails]] =
    partyManagementService
      .listKnownParties(new ListKnownPartiesRequest())
      .map(_.partyDetails.map(mapPartyDetails).toList)

  def allocateParty(hint: Option[String], displayName: Option[String]): Future[PartyDetails] =
    partyManagementService
      .allocateParty(new AllocatePartyRequest(hint.getOrElse(""), displayName.getOrElse("")))
      .map(_.partyDetails.getOrElse(sys.error("No PartyDetails in response.")))
      .map(mapPartyDetails)
}
