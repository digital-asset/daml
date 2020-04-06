// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.tls.TlsConfiguration

private[testtool] final case class LedgerSessionConfiguration(
    participants: Vector[(String, Int)],
    shuffleParticipants: Boolean,
    ssl: Option[TlsConfiguration],
    loadScaleFactor: Double,
    partyAllocation: PartyAllocationConfiguration,
)
