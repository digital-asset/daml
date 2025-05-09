// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import org.scalatest.wordspec.AnyWordSpec

class BoundedQueueTest extends AnyWordSpec with BaseTest {

  "BoundedQueue" should {
    "enqueue messages one by one and drop the oldest message" in {
      val queue = new BoundedQueue[Short](maxQueueSize = 2)
      queue.enqueue(1)
      queue.enqueue(2)
      queue should contain theSameElementsInOrderAs Seq(1, 2)
      queue.enqueue(3)
      queue should contain theSameElementsInOrderAs Seq(2, 3)
    }

    "enqueue all messages and drop the newest one" in {
      val queue = new BoundedQueue[Short](maxQueueSize = 2, DropStrategy.DropNewest)
      queue.enqueueAll(Seq(1, 2, 3))
      queue should contain theSameElementsInOrderAs Seq(1, 2)
    }
  }
}
