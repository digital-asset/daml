// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionConfiguration
import io.grpc.ManagedChannelBuilder

private[testtool] final class LedgerSessionConfiguration(
    participantChannelBuilders: Vector[ManagedChannelBuilder[_]],
    partyAllocation: PartyAllocationConfiguration,
    val shuffleParticipants: Boolean,
) {
  participantChannelBuilders.foreach(println)
  val participants: Vector[ParticipantSessionConfiguration] =
    for (participantChannelBuilder <- participantChannelBuilders)
      yield ParticipantSessionConfiguration(participantChannelBuilder, partyAllocation)
}
