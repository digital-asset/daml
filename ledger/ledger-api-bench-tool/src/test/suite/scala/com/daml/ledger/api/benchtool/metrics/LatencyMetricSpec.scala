// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.LatencyMetric.MaxLatency
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.util.chaining._

import java.time.Duration

class LatencyMetricSpec extends AnyWordSpec with Matchers {
  private val dummyPeriod = Duration.ofSeconds(1L)

  LatencyMetric.getClass.getSimpleName should {
    "compute correct values on updates" in {
      LatencyMetric
        .empty(maxLatencyObjectiveMillis = 0L)
        .tap(_.finalValue(dummyPeriod) shouldBe LatencyMetric.Value(0L))
        .tap(
          _.periodicValue(dummyPeriod) shouldBe (LatencyMetric(
            0L,
            0,
            MaxLatency(0),
          ) -> LatencyMetric.Value(0L))
        )
        .onNext(1000L)
        .tap(_.finalValue(dummyPeriod) shouldBe LatencyMetric.Value(1000L))
        .tap(
          _.periodicValue(dummyPeriod) shouldBe (LatencyMetric(
            1000L,
            1,
            MaxLatency(0),
          ) -> LatencyMetric.Value(1000L))
        )
        .onNext(2000L)
        .tap(_.finalValue(dummyPeriod) shouldBe LatencyMetric.Value(1500L))
        .tap(
          _.periodicValue(dummyPeriod) shouldBe (LatencyMetric(
            3000L,
            2,
            MaxLatency(0),
          ) -> LatencyMetric.Value(1500L))
        )
    }
  }

  MaxLatency.getClass.getSimpleName should {
    "correctly report violated metric" in {
      val maxObjectiveMillis = 1000L
      LatencyMetric
        .empty(maxLatencyObjectiveMillis = maxObjectiveMillis)
        .onNext(nanosFromMillis(1000L))
        .tap(_.violatedFinalObjectives(dummyPeriod) shouldBe empty)
        .onNext(nanosFromMillis(2000L))
        .tap(
          _.violatedFinalObjectives(dummyPeriod) shouldBe List(
            MaxLatency(nanosFromMillis(1000L)) -> LatencyMetric.Value(nanosFromMillis(1500L))
          )
        )
    }
  }

  private def nanosFromMillis(millis: Long) = millis * 1000000L
}
