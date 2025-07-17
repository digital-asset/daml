// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference

/** InMemoryProcessorStore encapsulates the state of the source and target participants.
  */
object InMemoryProcessorStore {
  def sourceParticipant(): SourceParticipantStore = new SourceParticipantStore()

  def targetParticipant(): TargetParticipantStore = new TargetParticipantStore()
}

final class SourceParticipantStore {
  private[party] val contractOrdinalToSendUpToExclusive =
    new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private[party] val sentContractsCount = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)

  @VisibleForTesting
  def getSentContractsCount: NonNegativeInt = sentContractsCount.get()
}

final class TargetParticipantStore {
  private[party] val requestedContractsCount =
    new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private[party] val processedContractsCount =
    new AtomicReference[NonNegativeInt](NonNegativeInt.zero)

  @VisibleForTesting
  def getProcessedContractsCount: NonNegativeInt = processedContractsCount.get()
}
