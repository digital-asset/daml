// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  ParticipantAllocation,
  Participants
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

import scala.concurrent.{ExecutionContext, Future}

private[testtool] final class LedgerTestContext private[infrastructure] (
    participants: Vector[ParticipantTestContext])(implicit ec: ExecutionContext) {

  private[this] val participantsRing = Iterator.continually(participants).flatten

  def provision(allocation: ParticipantAllocation): Future[Participants] =
    Future
      .sequence(allocation.partyCounts.map(partyCount => {
        val participant = nextParticipant()
        participant
          .allocateParties(partyCount.count)
          .map(parties => Participant(participant, parties: _*))
      }))
      .map(Participants(_: _*))

  private[this] def nextParticipant(): ParticipantTestContext =
    participantsRing.synchronized {
      participantsRing.next()
    }
}
