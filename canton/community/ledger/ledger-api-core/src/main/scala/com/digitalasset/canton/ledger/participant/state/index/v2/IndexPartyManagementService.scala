// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.lf.data.Ref.{ParticipantId, Party}
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc]]
  */
trait IndexPartyManagementService {
  def getParticipantId(): Future[ParticipantId]

  def getParties(
      parties: Seq[Party]
  )(implicit loggingContext: LoggingContextWithTrace): Future[List[IndexerPartyDetails]]

  def listKnownParties()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]]

  def partyEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContextWithTrace): Source[PartyEntry, NotUsed]
}
