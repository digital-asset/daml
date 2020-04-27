// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.tls.TlsConfiguration

private[testtool] final case class ParticipantSessionConfiguration(
    host: String,
    port: Int,
    ssl: Option[TlsConfiguration],
    partyAllocation: PartyAllocationConfiguration,
)
