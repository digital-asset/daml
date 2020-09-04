// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionConfiguration
import com.daml.ledger.api.tls.TlsConfiguration

private[testtool] final case class LedgerSessionConfiguration(
    participantAddresses: Vector[(String, Int)],
    shuffleParticipants: Boolean,
    ssl: Option[TlsConfiguration],
    partyAllocation: PartyAllocationConfiguration,
) {
  lazy val participants: Vector[ParticipantSessionConfiguration] =
    participantAddresses.map(forParticipant)

  def forParticipant(hostAndPort: (String, Int)): ParticipantSessionConfiguration = {
    val (host, port) = hostAndPort
    ParticipantSessionConfiguration(host, port, ssl, partyAllocation)
  }
}
