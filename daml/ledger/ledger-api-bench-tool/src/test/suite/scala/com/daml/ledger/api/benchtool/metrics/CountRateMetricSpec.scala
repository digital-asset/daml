// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.CountRateMetric._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.language.existentials

class CountRateMetricSpec extends AnyWordSpec with Matchers {
  "CountRateMetric" should {
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

    "compute violated minimum rate periodic SLO and the corresponing violating value" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val minAllowedRatePerSecond = 2.0
      val objective = RateObjective.MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(periodicObjectives = List(objective))

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
          .violatedPeriodicObjectives

      violatedObjective shouldBe List(
        objective -> Value(1.5)
      )
    }

    "not report not violated periodic min rate objectives" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val minAllowedRatePerSecond = 2.0
      val objective = RateObjective.MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(periodicObjectives = List(objective))

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
          .violatedPeriodicObjectives

      violatedObjective shouldBe Nil
    }

    "report violated min rate final objective" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val totalDuration: Duration = Duration.ofSeconds(6)
      val minAllowedRatePerSecond = 2.0
      val objective = RateObjective.MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(finalObjectives = List(objective))

      val violatedObjective =
        metric
          .onNext("abc")
          .periodicValue(periodDuration)
          ._1
          .onNext("def")
          .onNext("ghi")
          // total rate is (3 + 3 + 3) / 6.0
          .violatedFinalObjectives(totalDuration)

      violatedObjective shouldBe List(
        objective -> Value(1.5)
      )
    }

    "not report non-violated min rate final objective" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val totalDuration: Duration = Duration.ofSeconds(6)
      val minAllowedRatePerSecond = 2.0
      val objective = RateObjective.MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(finalObjectives = List(objective))

      val violatedObjective =
        metric
          .onNext("abc")
          .periodicValue(periodDuration)
          ._1
          .onNext("def")
          .onNext("ghi")
          .onNext("jklmno")
          // total rate is (3 + 3 + 3 + 6) / 6.0
          .violatedFinalObjectives(totalDuration)

      violatedObjective shouldBe Nil
    }

    "not report non-violated min rate final objective if the objective is violated only in a period" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val totalDuration: Duration = Duration.ofSeconds(3)
      val minAllowedRatePerSecond = 2.0
      val objective = RateObjective.MinRate(minAllowedRatePerSecond)
      val metric = anEmptyStringMetric(finalObjectives = List(objective))

      val violatedObjective =
        metric
          .onNext("abc")
          // periodic rate is 3 / 2.0 = 1.5
          .periodicValue(periodDuration)
          ._1
          .onNext("def")
          .onNext("ghi")
          // total rate is (3 + 3 + 3) / 3.0 = 3.0
          .violatedFinalObjectives(totalDuration)

      violatedObjective shouldBe Nil
    }

    "report violated max rate final objective" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val totalDuration: Duration = Duration.ofSeconds(3)
      val objective = RateObjective.MaxRate(3.0)
      val metric = CountRateMetric.empty[String](
        countingFunction = stringLength,
        periodicObjectives = Nil,
        finalObjectives = List(objective),
      )

      val violatedObjective =
        metric
          .onNext("abc")
          .periodicValue(periodDuration)
          ._1
          .onNext("def")
          .onNext("ghijkl")
          // total rate is (3 + 3 + 6) / 3.0 = 4.0
          .violatedFinalObjectives(totalDuration)

      violatedObjective shouldBe List(
        objective -> Value(4.0)
      )
    }

    "not report non-violated max rate final objective" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val totalDuration: Duration = Duration.ofSeconds(3)
      val objective = RateObjective.MaxRate(3.0)
      val metric = anEmptyStringMetric(finalObjectives = List(objective))

      val violatedObjective =
        metric
          .onNext("abc")
          .periodicValue(periodDuration)
          ._1
          .onNext("def")
          .onNext("ghi")
          // total rate is (3 + 3 + 3) / 3.0 = 3.0
          .violatedFinalObjectives(totalDuration)

      violatedObjective shouldBe Nil
    }

    "not report non-violated max rate final objective if the objective is violated only in a period" in {
      val periodDuration: Duration = Duration.ofSeconds(2)
      val totalDuration: Duration = Duration.ofSeconds(4)
      val objective = RateObjective.MaxRate(2.0)
      val metric = anEmptyStringMetric(finalObjectives = List(objective))

      val violatedObjective =
        metric
          .onNext("abcde")
          // periodic rate is 5 / 2.0 = 2.5
          .periodicValue(periodDuration)
          ._1
          .onNext("f")
          .onNext("gh")
          // total rate is (5 + 1 + 2) / 4.0 = 2.0
          .violatedFinalObjectives(totalDuration)

      violatedObjective shouldBe Nil
    }
  }

  private def stringLength(value: String): Int = value.length
  private def anEmptyStringMetric(
      periodicObjectives: List[CountRateMetric.RateObjective] = Nil,
      finalObjectives: List[CountRateMetric.RateObjective] = Nil,
  ): CountRateMetric[String] =
    CountRateMetric.empty[String](
      countingFunction = stringLength,
      periodicObjectives = periodicObjectives,
      finalObjectives = finalObjectives,
    )
}
