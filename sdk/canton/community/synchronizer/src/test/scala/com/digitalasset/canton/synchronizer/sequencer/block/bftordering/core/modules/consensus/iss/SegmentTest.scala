// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.SegmentTest.myId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import org.scalatest.wordspec.AnyWordSpec

class SegmentTest extends AnyWordSpec with BaseTest {

  "Segment helper functions" should {
    "work as expected" in {
      val segment =
        Segment(myId, NonEmpty(Seq, BlockNumber(100L), BlockNumber(104L), BlockNumber(108L)))

      segment.relativeBlockIndex(BlockNumber(104L)) shouldBe 1
      segment.relativeBlockIndex(BlockNumber(1500L)) shouldBe -1

      segment.firstBlockNumber shouldBe BlockNumber(100L)

      segment.isFirstInSegment(BlockNumber(100L)) shouldBe true
      segment.isFirstInSegment(BlockNumber(104L)) shouldBe false

      segment.previousBlockNumberInSegment(BlockNumber(104L)) shouldBe Some(BlockNumber(100L))
      segment.previousBlockNumberInSegment(BlockNumber(100L)) shouldBe None
    }
  }
}

object SegmentTest {
  private val myId = BftNodeId("self")
}
