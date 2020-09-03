// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSession,
  ParticipantSessionManager
}

import scala.concurrent.{ExecutionContext, Future}

private[infrastructure] final class LedgerSession(
    shuffleParticipants: Boolean,
    participantSessions: Vector[(String, ParticipantSession)],
)(implicit val executionContext: ExecutionContext) {
  private[infrastructure] def createTestContext(
      applicationId: String,
      identifierSuffix: String,
  ): Future[LedgerTestContext] =
    Future
      .sequence(
        (if (shuffleParticipants) scala.util.Random.shuffle(participantSessions)
         else participantSessions)
          .map {
            case (endpointId, session) =>
              session.createTestContext(endpointId, applicationId, identifierSuffix)
          }
      )
      .map(new LedgerTestContext(_))
}

object LedgerSession {
  def apply(
      config: LedgerSessionConfiguration,
      participantSessionManager: ParticipantSessionManager,
  )(implicit executionContext: ExecutionContext): Future[LedgerSession] = {
    val endpointIdProvider =
      Identification.circularWithIndex(Identification.greekAlphabet)
    for {
      participantSessions <- Future
        .sequence(config.participants.map(hostAndPort =>
          participantSessionManager.getOrCreate(config.forParticipant(hostAndPort))))
        .map(_.map(endpointIdProvider() -> _))
    } yield
      new LedgerSession(
        shuffleParticipants = config.shuffleParticipants,
        participantSessions = participantSessions,
      )
  }
}
