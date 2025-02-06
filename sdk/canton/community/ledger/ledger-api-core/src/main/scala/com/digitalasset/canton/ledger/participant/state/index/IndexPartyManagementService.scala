// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref.{ParticipantId, Party}

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc]]
  */
trait IndexPartyManagementService {
  def getParticipantId(): Future[ParticipantId]

  def getParties(
      parties: Seq[Party]
  )(implicit loggingContext: LoggingContextWithTrace): Future[List[IndexerPartyDetails]]

  def listKnownParties(
      fromExcl: Option[Party],
      maxResults: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]]
}
