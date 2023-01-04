// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics.metrics

import java.time.{Clock, Duration, Instant, ZoneId}

import com.daml.clock.AdjustableClock
import com.daml.ledger.api.benchtool.metrics.metrics.TotalStreamRuntimeMetric.{
  MaxDurationObjective,
  Value,
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TotalStreamRuntimeMetricSpec extends AnyFlatSpec with Matchers {

  it should "keep track of total stream runtime" in {
    val startTime = Instant.EPOCH.plusMillis(1000)
    val clock = AdjustableClock(Clock.fixed(startTime, ZoneId.systemDefault()), Duration.ZERO)
    val objective = MaxDurationObjective(
      maxValue = Duration.ofMillis(103)
    )
    val tested = TotalStreamRuntimeMetric[Any](
      clock = clock,
      startTime = clock.instant,
      objective = objective,
    )
    val ignoredDuration = Duration.ofMillis(0)
    val item = new Object()

    tested.periodicValue(ignoredDuration) shouldBe (tested, Value(Duration.ZERO))

    clock.fastForward(Duration.ofMillis(15))
    tested.onNext(item)

    tested.periodicValue(ignoredDuration) shouldBe (tested, Value(Duration.ofMillis(15)))

    clock.fastForward(Duration.ofMillis(30))
    tested.onNext(item)

    tested.periodicValue(ignoredDuration) shouldBe (tested, Value(Duration.ofMillis(45)))
    tested.violatedFinalObjectives(ignoredDuration) shouldBe Nil
    tested.violatedPeriodicObjectives shouldBe Nil
    tested.finalValue(ignoredDuration) shouldBe Value(Duration.ofMillis(45))

    clock.fastForward(Duration.ofMillis(100))
    tested.onNext(item)

    tested.periodicValue(ignoredDuration) shouldBe (tested, Value(Duration.ofMillis(145)))
    tested.violatedPeriodicObjectives shouldBe Nil
    tested.violatedFinalObjectives(ignoredDuration) shouldBe List(
      objective -> Value(Duration.ofMillis(145))
    )
    tested.finalValue(ignoredDuration) shouldBe Value(Duration.ofMillis(145))

    clock.fastForward(Duration.ofMillis(100))
    tested.onNext(item)
    tested.violatedFinalObjectives(ignoredDuration) shouldBe List(
      objective -> Value(Duration.ofMillis(245))
    )
  }

}
