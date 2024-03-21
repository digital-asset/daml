// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class LoadGaugeTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private class Fixture {
    val now = new AtomicLong(0)
    val gauge = new LoadGauge(1000.millis, now.get() * 1000000)

    def run(ms: Long): Unit = {
      Await.result(
        gauge.event(Future[Unit] {
          advance(ms)
        }),
        1.seconds,
      )
    }

    def advance(ms: Long): Unit = {
      now.updateAndGet(_ + ms)
    }

  }

  "load gauge" should {
    "report nicely if nothing happened" in {
      val f = new Fixture()
      assertResult(0.0)(f.gauge.getLoad)
    }

    "full load if load takes longer than interval" in {
      val f = new Fixture()
      f.run(2000)
      assert(f.gauge.getLoad === 1.0, f.gauge.getLoad)
    }

    "full load with multiple intervals" in {
      val f = new Fixture()
      f.run(500)
      f.run(500)
      assert(f.gauge.getLoad === 1.0, f.gauge.getLoad)
      f.run(500)
      assert(f.gauge.getLoad === 1.0, f.gauge.getLoad)
    }

    "comes down to and keeps on updating right load" in {
      val f = new Fixture()
      f.run(1000)
      f.advance(500)
      assert(f.gauge.getLoad === 0.5, f.gauge.getLoad)
      f.run(250)
      assert(f.gauge.getLoad === 0.5, f.gauge.getLoad)
      f.run(250)
      assert(f.gauge.getLoad === 0.5, f.gauge.getLoad)
      f.run(250)
      assert(f.gauge.getLoad === 0.75, f.gauge.getLoad)
      f.run(500)
      assert(f.gauge.getLoad === 1.0, f.gauge.getLoad)
      f.advance(1000)
      assert(f.gauge.getLoad === 0.0, f.gauge.getLoad)
    }

  }

}
