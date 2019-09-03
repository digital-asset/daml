// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSessionConfiguration,
  ParticipantSessionManager
}

import scala.concurrent.{ExecutionContext, Future}

private[testtool] final class LedgerSession(
    val config: LedgerSessionConfiguration,
    participantSessionManager: ParticipantSessionManager)(
    implicit val executionContext: ExecutionContext) {

  private[this] val participantSessions =
    Future.sequence(config.participants.map {
      case (host, port) =>
        participantSessionManager.getOrCreate(
          ParticipantSessionConfiguration(host, port, config.ssl, config.commandTtlFactor))
    })

  private[testtool] def createTestContext(
      applicationId: String,
      identifierSuffix: String): Future[LedgerTestContext] =
    participantSessions.flatMap { sessions =>
      Future
        .sequence(sessions.map(_.createTestContext(applicationId, identifierSuffix)))
        .map(new LedgerTestContext(_))
    }

}
