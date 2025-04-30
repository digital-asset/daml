// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class AvailabilityModuleConfig(
    maxRequestsInBatch: Short,
    maxBatchesPerProposal: Short,
    outputFetchTimeout: FiniteDuration,
    maxNonOrderedBatchesPerNode: Short = AvailabilityModuleConfig.MaxNonOrderedBatchesPerNode,
    emptyBlockCreationInterval: FiniteDuration = AvailabilityModuleConfig.EmptyBlockCreationInterval,
)

object AvailabilityModuleConfig {
  val EmptyBlockCreationInterval: FiniteDuration = 1000.milliseconds
  val MaxNonOrderedBatchesPerNode: Short = 1000
}
