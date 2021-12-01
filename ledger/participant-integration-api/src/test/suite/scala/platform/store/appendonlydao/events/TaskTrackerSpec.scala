// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class TaskTrackerSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  behavior of "TaskTracker"

  it should "handle a single full batch" in {
    val tracker = new FilterTableACSReader.TaskTracker[String](
      allTasks = List("A"),
      inputBatchSize = 2,
    )

    tracker.add("A", List(1L, 2L)) shouldBe Some(List(1L, 2L), "A") -> true

    tracker.finished("A") shouldBe None -> false
  }

  it should "handle a single partial batch" in {
    val tracker = new FilterTableACSReader.TaskTracker[String](
      allTasks = List("A"),
      inputBatchSize = 2,
    )

    tracker.add("A", List(1L)) shouldBe Some(List(1L), "A") -> true

    tracker.finished("A") shouldBe None -> true
  }

  it should "handle a single empty batch" in {
    val tracker = new FilterTableACSReader.TaskTracker[String](
      allTasks = List("A"),
      inputBatchSize = 2,
    )

    tracker.add("A", List.empty) shouldBe None -> true

    tracker.finished("A") shouldBe None -> false
  }

  it should "queue tasks with an empty task at the end" in {
    val tracker = new FilterTableACSReader.TaskTracker[String](
      allTasks = List("A"),
      inputBatchSize = 2,
    )

    tracker.add("A", List(1L, 2L)) shouldBe Some(List(1L, 2L), "A") -> true
    tracker.add("A", List(3L, 4L)) shouldBe None -> true
    tracker.add("A", List(5L, 6L)) shouldBe None -> true
    tracker.add("A", List.empty) shouldBe None -> true

    tracker.finished("A") shouldBe Some(List(3L, 4L), "A") -> true
    tracker.finished("A") shouldBe Some(List(5L, 6L), "A") -> true
    tracker.finished("A") shouldBe None -> true
  }

  it should "queue tasks with a partial task at the end" in {
    val tracker = new FilterTableACSReader.TaskTracker[String](
      allTasks = List("A"),
      inputBatchSize = 2,
    )

    tracker.add("A", List(1L, 2L)) shouldBe Some(List(1L, 2L), "A") -> true
    tracker.add("A", List(3L, 4L)) shouldBe None -> true
    tracker.add("A", List(5L)) shouldBe None -> true

    tracker.finished("A") shouldBe Some(List(3L, 4L), "A") -> true
    tracker.finished("A") shouldBe Some(List(5L), "A") -> true
    tracker.finished("A") shouldBe None -> true
  }

  it should "indicate that further merging is required if no tasks are idle" in {
    val tracker = new FilterTableACSReader.TaskTracker[String](
      allTasks = List("A", "B"),
      inputBatchSize = 2,
    )

    tracker.add("A", List(1L, 2L)) shouldBe Some(List(1L, 2L), "A") -> false
    tracker.add("B", List(1L, 2L)) shouldBe Some(List(1L, 2L), "B") -> true

    tracker.finished("A") shouldBe None -> false
    tracker.finished("B") shouldBe None -> false
  }

}