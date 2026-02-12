// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.syntax.parallel.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.*

class ProcessingQueueTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "ProcessingQueue" should {

    "execute actions for the same ID sequentially" in {
      val queue = new ShardedSequentialProcessingQueue[String]
      val id = "request-1"

      val firstActionStarted = PromiseUnlessShutdown.unsupervised[Unit]()
      val firstActionCanComplete = PromiseUnlessShutdown.unsupervised[Unit]()
      val secondActionStarted = new AtomicBoolean(false)

      // Enqueue first action
      val f1 = queue.enqueueForProcessing(id) {
        firstActionStarted.outcome_(())
        firstActionCanComplete.futureUS
      }

      // Ensure first action has started
      firstActionStarted.futureUS.flatMap { _ =>
        // Enqueue second action
        val f2 = queue.enqueueForProcessing(id) {
          secondActionStarted.set(true)
          FutureUnlessShutdown.unit
        }

        // Verify second action hasn't started yet because f1 is pending
        secondActionStarted.get() shouldBe false

        // Complete the first action
        firstActionCanComplete.outcome_(())

        for {
          _ <- f1
          _ <- f2
        } yield {
          secondActionStarted.get() shouldBe true
          queue.processingQueuePerRequest.get(id) shouldBe None
        }
      }.futureValueUS
    }

    "execute actions for different IDs concurrently" in {
      val queue = new ShardedSequentialProcessingQueue[String]
      val id1 = "id-1"
      val id2 = "id-2"

      val id1Started = PromiseUnlessShutdown.unsupervised[Unit]()
      val id1Blocked = PromiseUnlessShutdown.unsupervised[Unit]()

      // Start action for ID 1 (blocked)
      val f1 = queue.enqueueForProcessing(id1) {
        id1Started.outcome_(())
        id1Blocked.futureUS
      }

      id1Started.futureUS.futureValueUS
      // Action for ID 2 should be able to run and complete immediately even if ID 1 is blocked
      queue.enqueueForProcessing(id2)(FutureUnlessShutdown.unit).futureValueUS
      queue.processingQueuePerRequest.contains(id2) shouldBe false

      // ID 2 completed
      id1Blocked.outcome_(()) // Now release ID 1
      f1.futureValueUS
      queue.processingQueuePerRequest.contains(id1) shouldBe false
    }

    "clean up the map entry only if it's the last future in the chain" in {
      val queue = new ShardedSequentialProcessingQueue[String]
      val id = "cleanup-test"

      val p1 = PromiseUnlessShutdown.unsupervised[Unit]()
      val p2 = PromiseUnlessShutdown.unsupervised[Unit]()

      val f1 = queue.enqueueForProcessing(id)(p1.futureUS)
      val f2 = queue.enqueueForProcessing(id)(p2.futureUS)

      // The map should contain the future for f2 (the latest one)
      queue.processingQueuePerRequest.get(id) should not be empty

      p1.outcome_(())
      f1.futureValueUS
      // After f1 completes, the map should STILL contain an entry because f2 is pending
      queue.processingQueuePerRequest.get(id) should not be empty

      p2.outcome_(())
      f2.futureValueUS
      // After f2 completes, the map should be empty
      queue.processingQueuePerRequest.get(id) shouldBe None
    }

    "stop processing the queue if a previous action fails" in {
      val queue = new ShardedSequentialProcessingQueue[String]
      val id = "error-test"

      val firstActionStarted = PromiseUnlessShutdown.unsupervised[Unit]()
      val firstActionCompletes = PromiseUnlessShutdown.unsupervised[Unit]()

      val f1 = queue.enqueueForProcessing(id) {
        firstActionStarted.outcome_(())
        firstActionCompletes.futureUS
      }

      // Wait for the first action to start and then register another action
      firstActionStarted.futureUS.futureValueUS
      val f2executed = new AtomicBoolean(false)
      val f2 = queue.enqueueForProcessing(id) {
        FutureUnlessShutdown.unit.map(_ => f2executed.set(true))
      }
      val exception = new RuntimeException("Boom!")
      firstActionCompletes.failure(exception)

      f1.failed.futureValueUS shouldBe exception
      // f2 should fail with the same exception
      f1.failed.futureValueUS shouldBe f2.failed.futureValueUS
      // and f2 should not have been run
      f2executed.get() shouldBe false
      // but still the queue should be cleaned up.
      queue.processingQueuePerRequest.get(id) shouldBe None
    }
  }

  "not deadlock" in {
    val processingQueue = new ShardedSequentialProcessingQueue[Int]
    val counter = new AtomicLong(1000)
    val workF = (1 to counter.intValue()).toList.parTraverse { _ =>
      processingQueue.enqueueForProcessing(0) {
        FutureUnlessShutdown.unit.map(_ => counter.decrementAndGet().discard)
      }
    }
    workF.futureValueUS
    counter.get shouldBe 0L
  }

  "scale linearly with a large number of unique IDs" in {
    val queue = new ShardedSequentialProcessingQueue[Int]
    val numUniqueIds = 10000
    val actionsPerId = 5

    val startTime = System.nanoTime()

    // Dispatch 50,000 total actions across 10,000 unique IDs
    val allTasks = (1 to numUniqueIds).toList.parTraverse_ { id =>
      (1 to actionsPerId).toList.parTraverse_ { _ =>
        queue.enqueueForProcessing(id)(FutureUnlessShutdown.unit)
      }
    }

    allTasks.futureValueUS
    val duration = (System.nanoTime() - startTime).nanos

    // Basic validation: All map entries should be cleared
    queue.processingQueuePerRequest shouldBe empty

    // Optional: Log performance metrics
    logger.info(
      s"Processed ${numUniqueIds * actionsPerId} tasks across $numUniqueIds IDs in ${LoggerUtil
          .roundDurationForHumans(duration)}"
    )

    // Ensure it doesn't take an unreasonable amount of time (e.g., > 10s for 50k simple tasks)
    // On a Macbook Pro M2 Max, this takes <1 s
    duration should be < 10.seconds
  }

}
