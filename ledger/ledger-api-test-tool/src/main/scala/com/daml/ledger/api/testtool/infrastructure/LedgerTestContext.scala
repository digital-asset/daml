// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

import scala.concurrent.{ExecutionContext, Future}

private[testtool] final class LedgerTestContext private[infrastructure] (
    participants: Vector[ParticipantTestContext])(implicit ec: ExecutionContext) {

  private[this] val participantsRing = Iterator.continually(participants).flatten
  private[this] def nextParticipant(): ParticipantTestContext =
    participantsRing.synchronized { participantsRing.next() }

  def participant(): Future[ParticipantTestContext] =
    Future.successful(nextParticipant())

  def participants(n: Int): Future[Vector[ParticipantTestContext]] =
    Future.sequence(Vector.fill(n)(participant()))

}
