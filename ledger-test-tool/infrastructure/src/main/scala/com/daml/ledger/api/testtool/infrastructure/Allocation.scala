// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.javaapi.data.Party

object Allocation {

  /** Specifies a sequence of party counts to be allocated on a sequence of participants.
    *
    * Number of party counts does not need to match the number of participants.
    * If there are fewer participants than party counts,
    * the participants will be reused in a circular fashion.
    */
  def allocate(firstPartyCount: PartyCount, partyCounts: PartyCount*): PartyAllocation =
    PartyAllocation(firstPartyCount +: partyCounts, minimumParticipantCount = 1)

  final case class PartyAllocation private (
      partyCounts: Seq[PartyCount],
      minimumParticipantCount: Int,
  ) {
    def expectingMinimumActualParticipantCount(
        minimumParticipantCount: Int
    ): PartyAllocation =
      copy(minimumParticipantCount = minimumParticipantCount)
  }

  /** Specifies the number of parties to allocate in a participant.
    *
    * NOTE: A single participant can be allocated parties from multiple party counts.
    */
  sealed trait PartyCount {
    val count: Int
  }

  case object NoParties extends PartyCount {
    override val count = 0
  }

  case object SingleParty extends PartyCount {
    override val count = 1
  }

  case object TwoParties extends PartyCount {
    override val count = 2
  }

  final case class Parties(override val count: Int) extends PartyCount

  /** Exposes information about configured participants and allocated parties to a test case.
    *
    * When using multiple participants, keep in mind that they do not update their view of the ledger synchronously.
    * E.g., after you create a contract through the command service of one participant,
    * other participants might not know about the contract yet.
    * Use `com.daml.ledger.api.testtool.infrastructure.Eventually` if you want to wait for a participant to catch up,
    * or `com.daml.ledger.api.testtool.infrastructure.Synchronize` to add a synchronization point between participants.
    */
  final case class Participants private[infrastructure] (participants: Participant*)

  final case class Participant private[infrastructure] (
      context: ParticipantTestContext,
      parties: Party*
  )

}
