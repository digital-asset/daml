// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class MergingTaskQueueSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  behavior of "MergingTaskQueue"

  it should "return None if there are no tasks" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List.empty
  }

  it should "return None if there is a single empty iterator" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List.empty -> "A")

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List.empty
  }

  it should "return None if there are two empty iterators" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List.empty -> "A")
    queue.push(List.empty -> "B")

    queue.runUntilATaskEmpty shouldBe None
    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List.empty
  }

  it should "run a single task with a single iterator" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List(1L, 3L, 5L) -> "A")

    queue.runUntilATaskEmpty shouldBe Some("A")
    results.toList shouldBe List(1L, 3L, 5L)

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List(1L, 3L, 5L)
  }

  it should "run a single task with two iterators" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List(1L, 3L, 5L) -> "A")
    queue.push(List(7L, 8L, 9L) -> "A")

    queue.runUntilATaskEmpty shouldBe Some("A")
    results.toList shouldBe List(1L, 3L, 5L)

    queue.runUntilATaskEmpty shouldBe Some("A")
    results.toList shouldBe List(1L, 3L, 5L, 7L, 8L, 9L)

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List(1L, 3L, 5L, 7L, 8L, 9L)
  }

  it should "run two tasks with non-overlapping ids" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List(1L, 3L) -> "A")
    queue.push(List(2L, 4L) -> "B")

    queue.runUntilATaskEmpty shouldBe Some("A")
    results.toList shouldBe List(1L, 2L, 3L)

    queue.runUntilATaskEmpty shouldBe Some("B")
    results.toList shouldBe List(1L, 2L, 3L, 4L)

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List(1L, 2L, 3L, 4L)
  }

  it should "run two tasks with identical ids" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List(1L, 2L, 3L) -> "A")
    queue.push(List(1L, 2L, 3L) -> "B")

    queue.runUntilATaskEmpty shouldBe Some("A")
    results.toList shouldBe List(1L, 1L, 2L, 2L, 3L)

    queue.runUntilATaskEmpty shouldBe Some("B")
    results.toList shouldBe List(1L, 1L, 2L, 2L, 3L, 3L)

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List(1L, 1L, 2L, 2L, 3L, 3L)
  }

  it should "ignore an empty iterator" in {
    val results = new collection.mutable.ListBuffer[Long]
    val queue = new FilterTableACSReader.MergingTaskQueue[String](results.addOne)

    queue.push(List(1L, 2L, 3L) -> "A")
    queue.push(List.empty -> "A")

    queue.runUntilATaskEmpty shouldBe Some("A")
    results.toList shouldBe List(1L, 2L, 3L)

    queue.runUntilATaskEmpty shouldBe None
    results.toList shouldBe List(1L, 2L, 3L)
  }
}
