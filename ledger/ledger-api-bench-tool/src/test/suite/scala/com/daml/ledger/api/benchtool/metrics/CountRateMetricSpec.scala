// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.CountRateMetric
import com.daml.ledger.api.benchtool.metrics.CountRateMetric.Value
import com.daml.ledger.api.benchtool.metrics.objectives.MinRate
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.language.existentials

class CountRateMetricSpec extends AnyWordSpec with Matchers {
  CountRateMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(1)
      val metric: CountRateMetric[String] = anEmptyStringMetric()

      val (_, periodicValue) = metric.periodicValue(periodDuration)
      val finalValue = metric.finalValue(totalDuration)

      periodicValue shouldBe Value(0.0)
      finalValue shouldBe Value(0.0)
    }

    "compute values after processing elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: CountRateMetric[String] = anEmptyStringMetric()
      val elem1: String = "abc"
      val elem2: String = "defg"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2)
      periodicValue shouldBe Value(
        ratePerSecond = totalCount * 1000.0 / periodDuration.toMillis
      )
      finalValue shouldBe Value(
        ratePerSecond = totalCount / totalDuration.getSeconds.toDouble
      )
    }

    "correctly handle periods with no elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: CountRateMetric[String] = anEmptyStringMetric()
      val elem1: String = "abc"
      val elem2: String = "defg"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
        ._1
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2)
      periodicValue shouldBe Value(
        ratePerSecond = 0.0
      )
      finalValue shouldBe Value(
        ratePerSecond = totalCount / totalDuration.getSeconds.toDouble
      )
    }

    "correctly handle multiple periods with elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: CountRateMetric[String] = anEmptyStringMetric()
      val elem1: String = "abc"
      val elem2: String = "defg"
      val elem3: String = "hij"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
        ._1
        .onNext(elem3)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2) + stringLength(elem3)
      periodicValue shouldBe Value(
        ratePerSecond = stringLength(elem3) * 1000.0 / periodDuration.toMillis
      )
      finalValue shouldBe Value(
        ratePerSecond = totalCount / totalDuration.getSeconds.toDouble
      )
    }

    "compute violated minimum rate SLO and the corresponing violating value" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val minAllowedRatePerSecond = 2.0
      val objective = MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(Some(objective))

      val violatedObjective =
        metric
          .onNext("abc")
          .onNext("de")
          .periodicValue(periodDuration)
          ._1
          .onNext("f")
          .onNext("gh")
          // During this period we get 3 elements: f, g, h, which means that the rate is 1.5
          .periodicValue(periodDuration)
          ._1
          .onNext("ijklmn")
          .violatedObjective

      violatedObjective shouldBe Some(
        objective -> CountRateMetric.Value(1.5)
      )
    }

    "not report not violated objectives" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val minAllowedRatePerSecond = 2.0
      val objective = MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(Some(objective))

      val violatedObjective =
        metric
          .onNext("abc")
          .onNext("de")
          .periodicValue(periodDuration)
          ._1
          .onNext("f")
          .onNext("gh")
          .onNext("ijk")
          .periodicValue(periodDuration)
          ._1
          .onNext("lmnoprst")
          .violatedObjective

      violatedObjective shouldBe None
    }
  }

  private def stringLength(value: String): Int = value.length
  private def anEmptyStringMetric(objective: Option[MinRate] = None): CountRateMetric[String] =
    CountRateMetric.empty[String](countingFunction = stringLength, objective = objective)
}
