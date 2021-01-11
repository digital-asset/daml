// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionConfiguration
import com.daml.ledger.api.tls.TlsConfiguration

private[testtool] final class LedgerSessionConfiguration(
    participantAddresses: Vector[(String, Int)],
    ssl: Option[TlsConfiguration],
    partyAllocation: PartyAllocationConfiguration,
    val shuffleParticipants: Boolean,
) {
  val participants: Vector[ParticipantSessionConfiguration] =
    for ((host, port) <- participantAddresses)
      yield ParticipantSessionConfiguration(host, port, ssl, partyAllocation)
}
