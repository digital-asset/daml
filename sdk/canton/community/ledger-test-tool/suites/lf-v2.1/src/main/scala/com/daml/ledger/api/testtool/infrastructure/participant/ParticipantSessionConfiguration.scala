// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import io.grpc.ManagedChannelBuilder

import scala.language.existentials

private[testtool] final case class ParticipantSessionConfiguration(
    participant: ManagedChannelBuilder[_],
    partyAllocation: PartyAllocationConfiguration,
)
