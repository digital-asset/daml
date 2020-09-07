// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionConfiguration
import com.daml.ledger.api.tls.TlsConfiguration

private[testtool] final case class LedgerSessionConfiguration(
    private val participantAddresses: Vector[(String, Int)],
    private val ssl: Option[TlsConfiguration],
    private val partyAllocation: PartyAllocationConfiguration,
    shuffleParticipants: Boolean,
) {
  lazy val participants: Vector[ParticipantSessionConfiguration] =
    participantAddresses.map {
      case (host, port) =>
        ParticipantSessionConfiguration(host, port, ssl, partyAllocation)
    }
}
