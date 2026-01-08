// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSession

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

private[infrastructure] final class LedgerSession private (
    participantSessions: Vector[(String, ParticipantSession)],
    shuffleParticipants: Boolean,
    connectedSynchronizers: Int,
)(implicit val executionContext: ExecutionContext) {

  private[infrastructure] def createTestContext(
      userId: String,
      identifierSuffix: String,
  ): Future[LedgerTestContext] = {
    val sessions =
      if (shuffleParticipants) Random.shuffle(participantSessions)
      else participantSessions
    Future
      .traverse(sessions) { case (endpointId, session) =>
        session.createTestContext(
          endpointId,
          userId,
          identifierSuffix,
          session.features,
        )
      }
      .map(new LedgerTestContext(_, connectedSynchronizers))
  }

}

object LedgerSession {

  def apply(
      participantSessions: Vector[ParticipantSession],
      shuffleParticipants: Boolean,
      connectedSynchronizers: Int,
  )(implicit executionContext: ExecutionContext): LedgerSession = {
    val endpointIdProvider =
      Identification.circularWithIndex(Identification.greekAlphabet)
    val sessions = participantSessions.map(endpointIdProvider() -> _)
    new LedgerSession(
      sessions,
      shuffleParticipants,
      connectedSynchronizers,
    )
  }

}
