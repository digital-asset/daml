// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

import java.util.concurrent.atomic.AtomicReference

/** InMemoryProcessorStore encapsulates the state of the source and target participants.
  */
object InMemoryProcessorStore {
  def sourceParticipant(): SourceParticipantStore = new SourceParticipantStore()

  def targetParticipant(): TargetParticipantStore = new TargetParticipantStore()
}

final class SourceParticipantStore {
  val contractOrdinalToSendUpToExclusive =
    new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  val sentContractsCount = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
}

final class TargetParticipantStore {
  val requestedContractsCount = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  val processedContractsCount = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
}
