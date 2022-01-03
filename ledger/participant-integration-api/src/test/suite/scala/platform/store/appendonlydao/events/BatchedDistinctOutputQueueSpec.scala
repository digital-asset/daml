// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class BatchedDistinctOutputQueueSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  behavior of "BatchedDistinctOutputQueue"

  it should "return an empty result if there is no input" in {
    val queue = new FilterTableACSReader.BatchedDistinctOutputQueue(2)

    queue.flushOutput shouldBe Vector.empty

    queue.flushPartialBatch()
    queue.flushOutput shouldBe Vector.empty
  }

  it should "handle a single partial batch" in {
    val queue = new FilterTableACSReader.BatchedDistinctOutputQueue(2)

    queue.push(1L)

    queue.flushOutput shouldBe Vector.empty

    queue.flushPartialBatch()
    queue.flushOutput shouldBe Vector(Vector(1L))
  }

  it should "handle a partial batch at the end" in {
    val queue = new FilterTableACSReader.BatchedDistinctOutputQueue(2)

    queue.push(1L)
    queue.push(2L)
    queue.push(3L)
    queue.push(4L)
    queue.push(5L)

    queue.flushOutput shouldBe Vector(Vector(1L, 2L), Vector(3L, 4L))

    queue.flushPartialBatch()
    queue.flushOutput shouldBe Vector(Vector(5L))
  }

  it should "deduplicate input" in {
    val queue = new FilterTableACSReader.BatchedDistinctOutputQueue(2)
    queue.push(1L)
    queue.push(1L)
    queue.push(1L)
    queue.push(2L)
    queue.push(2L)

    queue.flushOutput shouldBe Vector(Vector(1L, 2L))

    queue.flushPartialBatch()
    queue.flushOutput shouldBe Vector.empty
  }

  it should "only output distinct full batches when calling flushOutput" in {
    val queue = new FilterTableACSReader.BatchedDistinctOutputQueue(2)
    queue.push(1L)
    queue.push(2L)
    queue.push(3L)

    queue.flushOutput shouldBe Vector(Vector(1L, 2L))
    queue.flushOutput shouldBe Vector.empty

    queue.push(4L)

    queue.flushOutput shouldBe Vector(Vector(3L, 4L))

    queue.push(4L)
    queue.push(5L)

    queue.flushOutput shouldBe Vector.empty

    queue.flushPartialBatch()
    queue.flushOutput shouldBe Vector(Vector(5L))
  }

}
