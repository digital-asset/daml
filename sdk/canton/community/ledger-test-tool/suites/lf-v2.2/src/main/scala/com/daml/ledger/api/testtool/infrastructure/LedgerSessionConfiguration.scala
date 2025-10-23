// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionConfiguration
import io.grpc.ManagedChannelBuilder

private[testtool] final class LedgerSessionConfiguration(
    participantChannelBuilders: Vector[ManagedChannelBuilder[_]],
    partyAllocation: PartyAllocationConfiguration,
    val shuffleParticipants: Boolean,
) {
  val participants: Vector[ParticipantSessionConfiguration] =
    for (participantChannelBuilder <- participantChannelBuilders)
      yield ParticipantSessionConfiguration(participantChannelBuilder, partyAllocation)
}
