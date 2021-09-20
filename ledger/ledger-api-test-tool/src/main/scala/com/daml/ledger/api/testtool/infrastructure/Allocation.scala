// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.client.binding.Primitive.Party

private[testtool] object Allocation {
  def allocate(firstPartyCount: PartyCount, partyCounts: PartyCount*): ParticipantAllocation =
    ParticipantAllocation(firstPartyCount +: partyCounts)

  final case class ParticipantAllocation private (partyCounts: Seq[PartyCount])

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

  final case class Participants private[infrastructure] (
      allocatedParticipants: Participant*
  )

  final case class Participant private[infrastructure] (
      ledger: ParticipantTestContext,
      parties: Party*
  )
}
