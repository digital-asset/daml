// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSessionConfiguration,
  ParticipantSessionManager
}

import scala.concurrent.{ExecutionContext, Future}

private[testtool] final class LedgerSession(
    val config: LedgerSessionConfiguration,
    participantSessionManager: ParticipantSessionManager,
)(implicit val executionContext: ExecutionContext) {

  private[this] val endpointIdProvider =
    Identification.circularWithIndex(Identification.greekAlphabet)

  private[this] val participantSessions =
    Future
      .sequence(config.participants.map {
        case (host, port) =>
          participantSessionManager.getOrCreate(
            ParticipantSessionConfiguration(
              host,
              port,
              config.ssl,
              config.partyAllocation,
            ),
          )
      })
      .map(_.map(endpointIdProvider() -> _))

  private[testtool] def createTestContext(
      applicationId: String,
      identifierSuffix: String,
  ): Future[LedgerTestContext] =
    participantSessions.flatMap { sessions =>
      Future
        .sequence(
          (if (config.shuffleParticipants) scala.util.Random.shuffle(sessions) else sessions)
            .map {
              case (endpointId, session) =>
                session.createTestContext(endpointId, applicationId, identifierSuffix)
            }
        )
        .map(new LedgerTestContext(_))
    }

}
