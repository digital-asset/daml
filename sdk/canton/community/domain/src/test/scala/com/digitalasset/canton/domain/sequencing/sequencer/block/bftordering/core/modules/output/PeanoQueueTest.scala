// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import org.scalatest.wordspec.AnyWordSpec

class PeanoQueueTest extends AnyWordSpec with BaseTest {

  "PeanoQueue" should {
    "insert and poll" in {
      val peanoQueue = new PeanoQueue[String](BlockNumber.First)

      peanoQueue.head.v shouldBe BlockNumber.First

      peanoQueue.insertAndPoll(BlockNumber.First, "item0") shouldBe Seq("item0")
      peanoQueue.head.v shouldBe 1L

      peanoQueue.insertAndPoll(BlockNumber(2L), "item2") shouldBe empty
      peanoQueue.head.v shouldBe 1L

      peanoQueue.insertAndPoll(BlockNumber(1L), "item1") shouldBe Seq("item1", "item2")
      peanoQueue.head.v shouldBe 3L
    }
  }

  "insert before head with no effect" in {
    val peanoQueue = new PeanoQueue[String](BlockNumber(7L))
    peanoQueue.insertAndPoll(BlockNumber(5L), "item before head 1") shouldBe empty
    peanoQueue.head.v shouldBe 7L

    peanoQueue.insertAndPoll(BlockNumber(6L), "item before head 2") shouldBe empty
    peanoQueue.head.v shouldBe 7L

    peanoQueue.insertAndPoll(BlockNumber(7L), "head item") shouldBe Seq("head item")
  }
}
