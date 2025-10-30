// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  PartyAllocation,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

private[testtool] final class LedgerTestContext private[infrastructure] (
    val configuredParticipants: immutable.Seq[ParticipantTestContext],
    val connectedSynchronizers: Int,
)(implicit ec: ExecutionContext) {

  require(configuredParticipants.nonEmpty, "At least one participant must be provided.")

  private[this] val participantsRing = Iterator.continually(configuredParticipants).flatten

  /** This allocates participants and a specified number of parties for each participant.
    *
    * e.g. `allocate(ParticipantAllocation(SingleParty, Parties(3), NoParties, TwoParties))` will
    * eventually return:
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
    * Each execution of a test case allocates parties on participants, then deconstructs the result
    * and uses the various participants and parties throughout the test.
    */
  def allocateParties(allocation: PartyAllocation): Future[Participants] = {
    val participantAllocations: Seq[(ParticipantTestContext, Allocation.PartyCount)] =
      allocation.partyCounts.map(nextParticipant() -> _)
    val participantsUnderTest: Seq[ParticipantTestContext] = participantAllocations.map(_._1)
    Future
      .sequence(participantAllocations.map {
        case (participant: ParticipantTestContext, partyCount: Allocation.PartyCount)
            if partyCount.isExternal =>
          participant
            .allocateExternalParties(partyCount.count, connectedSynchronizers)
            .map(parties => Participant(participant, parties))
        case (participant: ParticipantTestContext, partyCount: Allocation.PartyCount) =>
          participant
            .preallocateParties(
              partyCount.count,
              participantsUnderTest,
              connectedSynchronizers,
            )
            .map(parties => Participant(participant, parties))
      })
      .map(participants => Participants(connectedSynchronizers, participants*))
  }

  private[this] def nextParticipant(): ParticipantTestContext =
    participantsRing.synchronized {
      participantsRing.next()
    }
}
