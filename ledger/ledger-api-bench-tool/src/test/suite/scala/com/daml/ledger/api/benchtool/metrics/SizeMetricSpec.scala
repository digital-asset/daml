// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class SizeMetricSpec extends AnyWordSpec with Matchers {
  SizeMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val totalDuration: Duration = Duration.ofSeconds(1)
      val periodDuration: Duration = Duration.ofMillis(100)
      val metric: SizeMetric[String] = anEmptySizeMetric()

      val (_, periodicValue) = metric.periodicValue(periodDuration)
      val finalValue = metric.finalValue(totalDuration)

      periodicValue shouldBe SizeMetric.Value(0.0)
      finalValue shouldBe SizeMetric.Value(0.0)
    }

    "compute values after processing elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: SizeMetric[String] = anEmptySizeMetric()
      val elem1: String = "abc"
      val elem2: String = "defghi"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val totalSizeMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      periodicValue shouldBe SizeMetric.Value(totalSizeMegabytes * 1000.0 / periodDuration.toMillis)
      finalValue shouldBe SizeMetric.Value(totalSizeMegabytes * 1000.0 / periodDuration.toMillis)
    }

    "correctly handle periods with no elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: SizeMetric[String] = anEmptySizeMetric()
      val elem1: String = "abc"
      val elem2: String = "defghi"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
        ._1
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val firstPeriodMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      val firstPeriodMean = firstPeriodMegabytes * 1000.0 / periodDuration.toMillis
      val secondPeriodMean = 0.0
      val totalMean = (firstPeriodMean + secondPeriodMean) / 2
      periodicValue shouldBe SizeMetric.Value(secondPeriodMean)
      finalValue shouldBe SizeMetric.Value(totalMean)
    }

    "correctly handle multiple periods with elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: SizeMetric[String] = anEmptySizeMetric()
      val elem1: String = "abc"
      val elem2: String = "defg"
      val elem3: String = "hij"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
        ._1
        .periodicValue(periodDuration)
        ._1
        .onNext(elem3)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val firstPeriodMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      val firstPeriodMean = firstPeriodMegabytes * 1000.0 / periodDuration.toMillis
      val secondPeriodMean = 0.0
      val thirdPeriodMegabytes = testSizingFunction(elem3).toDouble / 1024 / 1024
      val thirdPeriodMean = thirdPeriodMegabytes * 1000.0 / periodDuration.toMillis
      val totalMean = (firstPeriodMean + secondPeriodMean + thirdPeriodMean) / 3
      periodicValue shouldBe SizeMetric.Value(thirdPeriodMean)
      finalValue shouldBe SizeMetric.Value(totalMean)
    }
  }

  private def testSizingFunction(value: String): Long = value.length.toLong * 12345
  private def anEmptySizeMetric(): SizeMetric[String] =
    SizeMetric.empty[String](sizingFunction = testSizingFunction)
}
