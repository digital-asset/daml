// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveDouble}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.util.RateLimiter

import scala.collection.mutable

class BftNodeRateLimiter(
    clock: Clock,
    maxTasksPerSecond: NonNegativeNumeric[Double],
    maxBurstFactor: PositiveDouble,
) {
  private val rateLimiter: mutable.Map[BftNodeId, RateLimiter] = mutable.Map()

  def checkAndUpdateRate(node: BftNodeId): Boolean =
    rateLimiter
      .getOrElseUpdate(
        node,
        new RateLimiter(
          maxTasksPerSecond,
          maxBurstFactor,
          nanoTime = clock.now.toMicros * 1000,
        ),
      )
      .checkAndUpdateRate()
}
