// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import io.grpc.ManagedChannelBuilder

import scala.language.existentials

private[testtool] final case class ParticipantSessionConfiguration(
    participant: ManagedChannelBuilder[_],
    partyAllocation: PartyAllocationConfiguration,
)
