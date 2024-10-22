// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final case class AvailabilityModuleConfig(
    maxBatchesPerProposal: Short,
    outputFetchTimeout: FiniteDuration,
    emptyBlockCreationInterval: FiniteDuration = AvailabilityModuleConfig.EmptyBlockCreationInterval,
)

object AvailabilityModuleConfig {
  val EmptyBlockCreationInterval: FiniteDuration = 1000.milliseconds
}
