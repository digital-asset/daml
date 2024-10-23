// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import org.scalatest.wordspec.AsyncWordSpec

class EpochMetricsAccumulatorTest extends AsyncWordSpec with BaseTest {
  private val seq1 = fakeSequencerId("sequencer1")
  private val seq2 = fakeSequencerId("sequencer2")
  private val seq3 = fakeSequencerId("sequencer3")

  "EpochMetricsAccumulator" should {
    "accumulate votes and views" in {
      val accumulator = new EpochMetricsAccumulator()

      accumulator.accumulate(3, Map(seq1 -> 3), Map(seq1 -> 2, seq2 -> 2))

      accumulator.viewsCount shouldBe 3
      accumulator.commitVotes shouldBe Map(seq1 -> 3)
      accumulator.prepareVotes shouldBe Map(seq1 -> 2, seq2 -> 2)

      accumulator.accumulate(2, Map(seq1 -> 2, seq2 -> 2), Map(seq3 -> 2, seq2 -> 2))

      accumulator.viewsCount shouldBe 5
      accumulator.commitVotes shouldBe Map(seq1 -> 5, seq2 -> 2)
      accumulator.prepareVotes shouldBe Map(seq1 -> 2, seq2 -> 4, seq3 -> 2)
    }
  }
}
