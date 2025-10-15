// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.ConcurrentHashMap
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.jdk.CollectionConverters._

class LockSetTest extends AnyFlatSpec with Matchers {

  val logger = ContextualizedLogger.get(getClass)
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val lc: LoggingContext = LoggingContext.empty

  case object FakeException extends Throwable {
    def message = "ensure we can handle exceptions"
  }

  "withLocksOn" should "synchronize without deadlock" in {
    val lockSet = new LockSet[Char](logger)
    val keys = ('a' to 'z').toSet
    val counters = new ConcurrentHashMap[Char, Long]()

    val numThreads = 50 // Concurrently executing threads updating counters
    val repetitions = 10 // How many times the step is repeated
    val numKeysToIncr = 5 // How many counters we attempt to increment each step

    val futures = (1 to numThreads).map { _ =>
      Future {
        (1 to repetitions).foreach { i =>
          val keysToIncr = Random.shuffle(keys.toVector).take(numKeysToIncr)
          val task = lockSet
            .withLocksOn(keysToIncr) {
              Future {
                keysToIncr.foreach { k =>
                  // Intentially racy.
                  val oldCount = counters.getOrDefault(k, 0L)
                  Thread.sleep(1) // Yield the thread and allow others to potentially interleave.
                  counters.put(k, oldCount + 1)
                }
                if (i % 2 == 0) throw FakeException
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
}
