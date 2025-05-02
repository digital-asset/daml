// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import org.scalatest.wordspec.AsyncWordSpec

class BatchDisseminationNodeQuotaTrackerTest extends AsyncWordSpec with BaseTest {
  val batchId1 = BatchId.createForTesting("hash1")
  val batchId2 = BatchId.createForTesting("hash2")
  val batchId3 = BatchId.createForTesting("hash3")
  val batchId4 = BatchId.createForTesting("hash4")
  val batchId5 = BatchId.createForTesting("hash5")
  val someBatchId = BatchId.createForTesting("someBatchId")

  val node1: BftNodeId = BftNodeId("node1")
  val node2: BftNodeId = BftNodeId("node2")

  val epoch1: EpochNumber = EpochNumber.First
  val epoch2: EpochNumber = EpochNumber(epoch1 + 1)
  val epoch3: EpochNumber = EpochNumber(epoch2 + 1)

  "BatchDisseminationNodeQuotaTracker" should {
    "keep track of how many batches have been accepted for a node" in {
      val quotaSize = 3
      val tracker = new BatchDisseminationNodeQuotaTracker()

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true

      tracker.addBatch(node1, batchId1, epoch1)
      tracker.addBatch(node1, batchId2, epoch1)
      tracker.addBatch(node1, batchId2, epoch1) // ignored double add

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true

      tracker.addBatch(node1, batchId3, epoch1)

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe false
    }

    "be able to remove batches from quota" in {
      val quotaSize = 2
      val tracker = new BatchDisseminationNodeQuotaTracker()

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true
      tracker.canAcceptForNode(node2, someBatchId, quotaSize) shouldBe true

      tracker.addBatch(node1, batchId1, epoch1)
      tracker.addBatch(node1, batchId2, epoch1)

      tracker.addBatch(node2, batchId3, epoch1)
      tracker.addBatch(node2, batchId4, epoch1)

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe false
      tracker.canAcceptForNode(node2, someBatchId, quotaSize) shouldBe false

      tracker.removeOrderedBatch(batchId1)
      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true
      tracker.canAcceptForNode(node2, someBatchId, quotaSize) shouldBe false

      tracker.removeOrderedBatch(batchId3)
      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true
      tracker.canAcceptForNode(node2, someBatchId, quotaSize) shouldBe true
    }

    "be able to remove batches based on epoch expiration from quota" in {
      val quotaSize = 4
      val tracker = new BatchDisseminationNodeQuotaTracker()

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true
      tracker.addBatch(node1, batchId1, epoch1)
      tracker.addBatch(node1, batchId2, epoch1)
      tracker.addBatch(node1, batchId3, epoch2)
      tracker.addBatch(node1, batchId4, epoch3)
      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe false

      tracker.expireEpoch(epoch2)
      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true

      // expiring epoch2 removes both all batches from epoch1 and epoch2
      tracker.addBatch(node1, batchId5, epoch3)
      tracker.canAcceptForNode(node1, someBatchId, 3) shouldBe true

      // there should be 2 batches left
      tracker.canAcceptForNode(node1, someBatchId, 2) shouldBe false
    }

    "still accept previously accepted batches even if quota is full" in {
      val quotaSize = 1
      val tracker = new BatchDisseminationNodeQuotaTracker()

      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe true
      tracker.addBatch(node1, batchId1, epoch1)
      tracker.canAcceptForNode(node1, someBatchId, quotaSize) shouldBe false

      tracker.canAcceptForNode(node1, batchId1, quotaSize) shouldBe true
    }
  }
}
