// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration.DurationInt
import scala.util.Random
import scala.jdk.CollectionConverters._

class LockSetTest 
    extends AnyFlatSpec
    with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global

  "withLocksOn" should "synchronize without deadlock" in {
    val lockSet = new LockSet[Char]()
    val keys = ('a' to 'z').toSet
    val counters = new ConcurrentHashMap[Char, Long]()

    val numThreads = 100 // Concurrently executing threads updating counters
    val repetitions = 20 // How many times the step is repeated
    val numKeysToIncr = 10 // How many counters we attempt to increment each step

    val futures = (1 to numThreads).map { _ =>
      Future {
        (1 to repetitions).foreach { _ =>
          val keysToIncr = Random.shuffle(keys.toVector).take(numKeysToIncr)
          lockSet.withLocksOn(keysToIncr) {
            keysToIncr.foreach { k =>
              counters.put(k, counters.getOrDefault(k, 0L) + 1) // Intentially racy.
            }
          }
        }
      }
    }

    val _ = Await.result(Future.sequence(futures), 10.seconds)

    counters.values.asScala.iterator.sum shouldBe (numThreads * numKeysToIncr * repetitions)
  }
}
