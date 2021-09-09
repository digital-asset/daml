// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  ParticipantAllocation,
  Participants,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

private[testtool] final class LedgerTestContext private[infrastructure] (
    val configuredParticipants: immutable.Seq[ParticipantTestContext]
)(implicit ec: ExecutionContext) {

  require(configuredParticipants.nonEmpty, "At least one participant must be provided.")

  private[this] val participantsRing = Iterator.continually(configuredParticipants).flatten

  /** This allocates participants and a specified number of parties for each participant.
    *
    * e.g. `allocate(ParticipantAllocation(SingleParty, Parties(3), NoParties, TwoParties))`
    * will eventually return:
    *
    * {{{
    * Participants(
    *   Participant(alpha: ParticipantTestContext, alice: Party),
    *   Participant(beta: ParticipantTestContext, bob: Party, barbara: Party, bernard: Party),
    *   Participant(gamma: ParticipantTestContext),
    *   Participant(delta: ParticipantTestContext, doreen: Party, dan: Party),
    * )
    * }}}
    *
    * Each test allocates participants, then deconstructs the result and uses the various ledgers
    * and parties throughout the test.
    */
  def allocate(allocation: ParticipantAllocation): Future[Participants] = {
    val participantAllocations = allocation.partyCounts.map(nextParticipant() -> _)
    val participantsUnderTest = participantAllocations.map(_._1)
    Future
      .sequence(participantAllocations.map { case (participant, partyCount) =>
        participant
          .preallocateParties(partyCount.count, participantsUnderTest)
          .map(parties => Participant(participant, parties: _*))
      })
      .map(allocatedParticipants => Participants(allocatedParticipants: _*))
  }

  private[this] def nextParticipant(): ParticipantTestContext =
    participantsRing.synchronized {
      participantsRing.next()
    }
}
