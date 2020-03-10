// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{ParticipantId, Party}
import com.digitalasset.ledger.api.domain.{LedgerOffset, PartyDetails, PartyEntry}

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc]]
  */
trait IndexPartyManagementService {
  def getParticipantId(): Future[ParticipantId]

  def getParties(parties: Seq[Party]): Future[List[PartyDetails]]

  def listKnownParties(): Future[List[PartyDetails]]

  def partyEntries(beginOffset: LedgerOffset.Absolute): Source[PartyEntry, NotUsed]
}
