// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.{ExecutionContext, Future, Await, Promise, TimeoutException}
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.jdk.CollectionConverters._

class LockSetTest extends AnyWordSpec with Matchers {

  val logger = ContextualizedLogger.get(getClass)
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val lc: LoggingContext = LoggingContext.ForTesting

  case object FakeException extends Throwable {
    def message = "ensure we can handle exceptions"
  }

  "withLocksOn" should {
    "synchronize without deadlock" in {
      val lockSet = new LockSet[Char](logger)
      val keys = ('a' to 'z').toSet
      val counters = new ConcurrentHashMap[Char, Long]()

      val numThreads = 50 // Concurrently executing threads updating counters
      val repetitions = 10 // How many times the step is repeated
      val numKeysToIncr = 5 // How many counters we attempt to increment each step

      val futures = (1 to numThreads).map { t =>
        Future {
          (1 to repetitions).foreach { rep =>
            val keysToIncr = Random.shuffle(keys.toVector).take(numKeysToIncr)

            val task = lockSet
              .withLocksOn(keysToIncr, 1.minute, s"$t:$rep") {
                Future {
                  keysToIncr.foreach { k =>
                    // Intentially racy.
                    val oldCount = counters.getOrDefault(k, 0L)
                    Thread.sleep(1) // Yield the thread and allow others to potentially interleave.
                    counters.put(k, oldCount + 1)
                  }
                  if (rep % 2 == 0) throw FakeException
                }
              }
              .recover { case FakeException => () }

            val _ = Await.result(task, 20.seconds)
          }
        }
      }

      val _ = Await.result(Future.sequence(futures), 30.seconds)

      counters.values.asScala.iterator.sum shouldBe (numThreads * numKeysToIncr * repetitions)
    }

    "handle timeouts gracefully" in {
      val lockSet = new LockSet[Char](logger)

      // Acquire and hold locks on 'x' and 'y'.
      val acquired = Promise[Unit]() // This completes after the locks are acquired
      val release = Promise[Unit]() // So we can tell it when to complete and thus release the locks
      val holdLock: Future[Unit] = lockSet.withLocksOn("xy", 1.second, "future 1") {
        val _ = acquired.success(())
        release.future
      }
      val _ = Await.result(acquired.future, 5.seconds) // Wait until the locks are acquired

      // Now attempt to acquire these (still held) locks again, and only allow only 1 second.
      val updated = new AtomicReference[Boolean](false) // Whether the block was run
      val timeout = new AtomicReference[Boolean](false) // Whether the attempt failed due to timeout
      val awaitLock: Future[Unit] =
        lockSet
          .withLocksOn("xy", 1.second, "future 2") { Future { updated.set(true) } }
          .recover { case _: TimeoutException => timeout.set(true) }

      // 2s later we allow the first future to complete and release the locks.
      // By this time the second future should have already timed out.
      Thread.sleep(2.seconds.toMillis)
      val _ = release.success(())

      val _ = Await.result(Future.sequence(Seq(holdLock, awaitLock)), 5.seconds)

      val _ = updated.get() shouldBe false
      val _ = timeout.get() shouldBe true
    }
  }
}
