// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

final class GroupContiguousHeavySpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with AkkaBeforeAndAfterAll {

  behavior of "groupContiguous (heavy)"

  override def spanScaleFactor: Double = 100 // Very high timeout as we stress test this operator

  // This error condition is extremely difficult to trigger and currently
  // purely randomized data seems to do the trick.
  // See: https://github.com/digital-asset/daml/pull/8336
  it should "keep the order of the input even with items of widely skewed size" in {
    for (_ <- 1 to 50) {
      val pairs = Iterator.range(0, 1000).flatMap { outerKey =>
        Iterator.tabulate(Random.nextInt(100) + 10) { innerKey =>
          {
            val payload = 0.toChar.toString * (Random.nextInt(100) + 10)
            outerKey -> f"$outerKey%05d:$innerKey%05d:$payload:"
          }
        }
      }
      val grouped = groupContiguous(Source.fromIterator(() => pairs))(by = _._1)
      whenReady(grouped.runWith(Sink.seq[Vector[(Int, String)]])) {
        _.flatMap(_.map(_._2)) shouldBe sorted
      }
    }
  }

}
