// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.Clock

import java.util.concurrent.atomic.AtomicLong

/** To generate unique timestamps between many nodes without coordination we partition available timestamps by the
  * node index within a range of the total number of nodes.
  */
class PartitionedTimestampGenerator(clock: Clock, nodeIndex: Int, nodeCount: PositiveInt) {
  private val lastRef = new AtomicLong(
    // The `SequencerWriter` initialization waits after a restart until the clock has reached the highest watermark
    // written before a crash. So even if the clock jumps backwards across restarts,
    // it will have advanced sufficiently by the time the next timestamp is generated.
    clock.monotonicTime().toMicros
  )

  def generateNext: CantonTimestamp = {
    val nowMicros = clock.monotonicTime().toMicros
    val micros = lastRef.updateAndGet { latest =>
      val latestMicros = Math.max(nowMicros, latest + nodeCount.unwrap)
      val uniqueMicros = latestMicros - (latestMicros % nodeCount.unwrap) + nodeIndex
      uniqueMicros
    }
    CantonTimestamp.assertFromLong(micros)
  }

}
