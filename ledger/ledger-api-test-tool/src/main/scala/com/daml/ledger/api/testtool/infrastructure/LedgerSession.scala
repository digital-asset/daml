// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSession,
  ParticipantSessionManager
}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

private[infrastructure] final class LedgerSession private (
    participantSessions: immutable.Seq[(String, ParticipantSession)],
    shuffleParticipants: Boolean,
)(implicit val executionContext: ExecutionContext) {
  private[infrastructure] def createTestContext(
      applicationId: String,
      identifierSuffix: String,
  ): Future[LedgerTestContext] = {
    val sessions =
      if (shuffleParticipants) Random.shuffle(participantSessions)
      else participantSessions
    Future
      .traverse(sessions) {
        case (endpointId, session) =>
          session.createTestContext(endpointId, applicationId, identifierSuffix)
      }
      .map(new LedgerTestContext(_))
  }
}

object LedgerSession {
  def apply(
      participantSessionManager: ParticipantSessionManager,
      shuffleParticipants: Boolean,
  )(implicit executionContext: ExecutionContext): LedgerSession = {
    val endpointIdProvider =
      Identification.circularWithIndex(Identification.greekAlphabet)
    val participantSessions = participantSessionManager.allSessions.map(endpointIdProvider() -> _)
    new LedgerSession(participantSessions, shuffleParticipants)
  }
}
