// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{ParticipantId, Party}
import com.daml.ledger.api.domain.{LedgerOffset, PartyDetails, PartyEntry}
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc]]
  */
trait IndexPartyManagementService {
  def getParticipantId()(implicit loggingContext: LoggingContext): Future[ParticipantId]

  def getParties(parties: Seq[Party])(
      implicit loggingContext: LoggingContext,
  ): Future[List[PartyDetails]]

  def listKnownParties()(implicit loggingContext: LoggingContext): Future[List[PartyDetails]]

  def partyEntries(startExclusive: LedgerOffset.Absolute)(
      implicit loggingContext: LoggingContext,
  ): Source[PartyEntry, NotUsed]
}
