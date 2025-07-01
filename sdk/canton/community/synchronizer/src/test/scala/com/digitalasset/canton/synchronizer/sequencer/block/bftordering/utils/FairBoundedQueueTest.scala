// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FairBoundedQueue.{
  DeduplicationStrategy,
  EnqueueResult,
}
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import org.scalatest.wordspec.AnyWordSpec

class FairBoundedQueueTest extends AnyWordSpec with BaseTest {

  import FairBoundedQueueTest.*

  "FairBoundedQueueTest" should {
    "enqueue within per-node quota and maintain arrival order" in {
      val queue = new FairBoundedQueue[ItemType](maxQueueSize = 4, perNodeQuota = 2)
      queue.enqueue(node1, 1) shouldBe EnqueueResult.Success
      queue.enqueue(node2, 2) shouldBe EnqueueResult.Success
      queue.enqueue(node1, 3) shouldBe EnqueueResult.Success
      queue.dump shouldBe Seq(1, 2, 3)
      queue.dequeue() shouldBe Some(1)
      queue.dequeue() shouldBe Some(2)
      queue.dequeue() shouldBe Some(3)
      queue.dequeue() shouldBe None
    }

    "enqueue messages one by one, drop messages according to strategies, and dump" in {
      Table[Int, Int, DropStrategy, DeduplicationStrategy, Seq[Enqueue], Seq[ItemType]](
        (
          "max queue size",
          "per node quota",
          "drop strategy",
          "deduplication strategy",
          "items to enqueue and results",
          "final dump",
        ),
        (
          /*max queue size = */ 5,
          /*per-node quota = */ 2,
          DropStrategy.DropOldest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node2, 2, EnqueueResult.Success),
            Enqueue(node1, 3, EnqueueResult.Success),
            Enqueue(node1, 4, EnqueueResult.PerNodeQuotaExceeded(node1)),
          ),
          Seq(2, 3, 4),
        ),
        (
          /*max queue size = */ 3,
          /*per-node quota = */ 2,
          DropStrategy.DropOldest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node2, 2, EnqueueResult.Success),
            Enqueue(node1, 3, EnqueueResult.Success),
            Enqueue(node3, 4, EnqueueResult.TotalCapacityExceeded),
          ),
          Seq(1, 2, 3),
        ),
        (
          /*max queue size = */ 5,
          /*per-node quota = */ 2,
          DropStrategy.DropNewest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node2, 2, EnqueueResult.Success),
            Enqueue(node1, 3, EnqueueResult.Success),
            Enqueue(node1, 4, EnqueueResult.PerNodeQuotaExceeded(node1)),
            Enqueue(node2, 5, EnqueueResult.Success),
            Enqueue(node2, 6, EnqueueResult.PerNodeQuotaExceeded(node2)),
          ),
          Seq(1, 2, 3, 5),
        ),
        (
          /*max queue size = */ 3,
          /*per-node quota = */ 2,
          DropStrategy.DropNewest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node2, 2, EnqueueResult.Success),
            Enqueue(node1, 3, EnqueueResult.Success),
            Enqueue(node3, 4, EnqueueResult.TotalCapacityExceeded),
          ),
          Seq(1, 2, 3),
        ),
        (
          /*max queue size = */ 4,
          /*per-node quota = */ 1,
          DropStrategy.DropOldest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node2, 2, EnqueueResult.Success),
            Enqueue(node1, 3, EnqueueResult.PerNodeQuotaExceeded(node1)),
            Enqueue(node2, 4, EnqueueResult.PerNodeQuotaExceeded(node2)),
          ),
          Seq(3, 4),
        ),
        (
          /*max queue size = */ 3,
          /*per-node quota = */ 2,
          DropStrategy.DropOldest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node2, 2, EnqueueResult.Success),
            Enqueue(node1, 3, EnqueueResult.Success),
            Enqueue(node2, 4, EnqueueResult.TotalCapacityExceeded),
            Enqueue(node1, 5, EnqueueResult.PerNodeQuotaExceeded(node1)),
            Enqueue(node2, 6, EnqueueResult.TotalCapacityExceeded),
            Enqueue(node2, 7, EnqueueResult.TotalCapacityExceeded),
          ),
          Seq(3, 5, 7),
        ),
        (
          /*max queue size = */ 2,
          /*per-node quota = */ 2,
          DropStrategy.DropOldest,
          DeduplicationStrategy.Noop,
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node1, 1, EnqueueResult.Success),
          ),
          Seq(1, 1),
        ),
        (
          /*max queue size = */ 2,
          /*per-node quota = */ 2,
          DropStrategy.DropOldest,
          DeduplicationStrategy.PerNode(),
          Seq(
            Enqueue(node1, 1, EnqueueResult.Success),
            Enqueue(node1, 1, EnqueueResult.Duplicate(node1)),
          ),
          Seq(1),
        ),
      ).forEvery {
        (
            maxQueueSize,
            perNodeQuota,
            dropStrategy,
            deduplicationStrategy,
            itemsToEnqueue,
            finalDump,
        ) =>
          val queue = new FairBoundedQueue[ItemType](
            maxQueueSize,
            perNodeQuota,
            dropStrategy,
            deduplicationStrategy,
          )
          itemsToEnqueue.foreach(itemsToEnqueue =>
            queue.enqueue(itemsToEnqueue.nodeId, itemsToEnqueue.item) shouldBe itemsToEnqueue.result
          )
          queue.dump shouldBe finalDump
      }
    }

    "handle empty queue for dequeue" in {
      val queue = new FairBoundedQueue[ItemType](maxQueueSize = 2, perNodeQuota = 1)
      queue.dequeue() shouldBe None
    }

    "dequeue all with no matching elements" in {
      val queue = new FairBoundedQueue[ItemType](maxQueueSize = 2, perNodeQuota = 1)
      queue.enqueue(node1, 1) shouldBe EnqueueResult.Success
      queue.enqueue(node2, 2) shouldBe EnqueueResult.Success
      queue.dequeueAll(_ > 2) shouldBe Seq.empty
      queue.dump shouldBe Seq(1, 2)
    }

    "dequeue all with some matching elements, preserving order of remaining" in {
      val queue = new FairBoundedQueue[ItemType](maxQueueSize = 5, perNodeQuota = 3)
      queue.enqueue(node1, 1) shouldBe EnqueueResult.Success
      queue.enqueue(node2, 2) shouldBe EnqueueResult.Success
      queue.enqueue(node1, 3) shouldBe EnqueueResult.Success
      queue.enqueue(node2, 4) shouldBe EnqueueResult.Success
      queue.enqueue(node1, 5) shouldBe EnqueueResult.Success
      queue.dequeueAll(_ % 2 == 0) shouldBe Seq(2, 4)
      queue.dump shouldBe Seq(1, 3, 5)
    }

    "dequeue all with all matching elements" in {
      val queue = new FairBoundedQueue[ItemType](maxQueueSize = 3, perNodeQuota = 2)
      queue.enqueue(node1, 1) shouldBe EnqueueResult.Success
      queue.enqueue(node2, 2) shouldBe EnqueueResult.Success
      queue.enqueue(node1, 3) shouldBe EnqueueResult.Success
      queue.dequeueAll(_ => true) shouldBe Seq(1, 2, 3)
      queue.dump shouldBe Seq.empty
    }

    "dequeue all with an empty queue" in {
      val queue = new FairBoundedQueue[ItemType](maxQueueSize = 2, perNodeQuota = 1)
      queue.dequeueAll(_ => true) shouldBe Seq.empty
      queue.dump shouldBe Seq.empty
    }
  }
}

private object FairBoundedQueueTest {
  type ItemType = Short

  implicit val metricsContext: MetricsContext = MetricsContext.Empty

  private val node1 = BftNodeId("node1")
  private val node2 = BftNodeId("node2")
  private val node3 = BftNodeId("node3")

  final case class Enqueue(nodeId: BftNodeId, item: ItemType, result: EnqueueResult)
}
