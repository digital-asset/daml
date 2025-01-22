// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PeanoQueue
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import org.scalatest.wordspec.AnyWordSpec

class PeanoQueueTest extends AnyWordSpec with BaseTest {

  "PeanoQueue" should {
    "insert and poll" in {
      val peanoQueue = new PeanoQueue[String](BlockNumber.First)(abort = fail(_))

      peanoQueue.head.v shouldBe BlockNumber.First

      peanoQueue.insert(BlockNumber.First, "item0")
      peanoQueue.pollAvailable() shouldBe Seq("item0")
      peanoQueue.head.v shouldBe 1L

      peanoQueue.insert(BlockNumber(2L), "item2")
      peanoQueue.pollAvailable() shouldBe empty
      peanoQueue.head.v shouldBe 1L

      peanoQueue.insert(BlockNumber(1L), "item1")
      peanoQueue.pollAvailable() shouldBe Seq("item1", "item2")
      peanoQueue.head.v shouldBe 3L
    }

    "insert and not poll" in {
      val peanoQueue = new PeanoQueue[String](BlockNumber.First)(abort = fail(_))

      peanoQueue.insert(BlockNumber.First, "item0")
      peanoQueue.pollAvailable(pollWhile = _ => false) shouldBe Seq.empty
      peanoQueue.head.v shouldBe BlockNumber.First
    }

    "insert before head with no effect" in {
      val peanoQueue = new PeanoQueue[String](BlockNumber(7L))(abort = fail(_))
      peanoQueue.insert(BlockNumber(5L), "item before head 1")
      peanoQueue.pollAvailable() shouldBe empty
      peanoQueue.head.v shouldBe 7L

      peanoQueue.insert(BlockNumber(6L), "item before head 2")
      peanoQueue.pollAvailable() shouldBe empty
      peanoQueue.head.v shouldBe 7L

      peanoQueue.insert(BlockNumber(7L), "head item")
      peanoQueue.pollAvailable() shouldBe Seq("head item")
    }
  }
}
