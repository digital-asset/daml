// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSession
import com.daml.ledger.api.tls.TlsConfiguration

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

private[infrastructure] final class LedgerSession private (
    participantSessions: Vector[(String, ParticipantSession)],
    shuffleParticipants: Boolean,
    clientTlsConfiguration: Option[TlsConfiguration],
)(implicit val executionContext: ExecutionContext) {
  private[infrastructure] def createTestContext(
      applicationId: String,
      identifierSuffix: String,
  ): Future[LedgerTestContext] = {
    val sessions =
      if (shuffleParticipants) Random.shuffle(participantSessions)
      else participantSessions
    Future
      .traverse(sessions) { case (endpointId, session) =>
        session.createTestContext(
          endpointId,
          applicationId,
          identifierSuffix,
          clientTlsConfiguration,
        )
      }
      .map(new LedgerTestContext(_))
  }
}

object LedgerSession {
  def apply(
      participantSessions: Vector[ParticipantSession],
      shuffleParticipants: Boolean,
      clientTlsConfiguration: Option[TlsConfiguration],
  )(implicit executionContext: ExecutionContext): LedgerSession = {
    val endpointIdProvider =
      Identification.circularWithIndex(Identification.greekAlphabet)
    val sessions = participantSessions.map(endpointIdProvider() -> _)
    new LedgerSession(
      sessions,
      shuffleParticipants,
      clientTlsConfiguration,
    )
  }
}
