// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.SizeMetric
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SizeMetricSpec extends AnyWordSpec with Matchers {
  SizeMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val totalDurationSeconds: Double = 1.0
      val metric: SizeMetric[String] = anEmptySizeMetric()

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(totalDurationSeconds)

      periodicValue shouldBe SizeMetric.Value(0.0)
      finalValue shouldBe SizeMetric.Value(0.0)
    }

    "compute values after processing elements" in {
      val periodMillis: Long = 100
      val totalDurationSeconds: Double = 5.0
      val metric: SizeMetric[String] = anEmptySizeMetric(periodMillis)
      val elem1: String = "abc"
      val elem2: String = "defghi"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val totalSizeMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      periodicValue shouldBe SizeMetric.Value(totalSizeMegabytes * 1000.0 / periodMillis)
      finalValue shouldBe SizeMetric.Value(totalSizeMegabytes * 1000.0 / periodMillis)
    }

    "correctly handle periods with no elements" in {
      val periodMillis: Long = 100
      val totalDurationSeconds: Double = 5.0
      val metric: SizeMetric[String] = anEmptySizeMetric(periodMillis)
      val elem1: String = "abc"
      val elem2: String = "defghi"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
        ._1
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val firstPeriodMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      val firstPeriodMean = firstPeriodMegabytes * 1000.0 / periodMillis
      val secondPeriodMean = 0.0
      val totalMean = (firstPeriodMean + secondPeriodMean) / 2
      periodicValue shouldBe SizeMetric.Value(secondPeriodMean)
      finalValue shouldBe SizeMetric.Value(totalMean)
    }

    "correctly handle multiple periods with elements" in {
      val periodMillis: Long = 100
      val totalDurationSeconds: Double = 5.0
      val metric: SizeMetric[String] = anEmptySizeMetric(periodMillis)
      val elem1: String = "abc"
      val elem2: String = "defg"
      val elem3: String = "hij"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
        ._1
        .periodicValue()
        ._1
        .onNext(elem3)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val firstPeriodMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      val firstPeriodMean = firstPeriodMegabytes * 1000.0 / periodMillis
      val secondPeriodMean = 0.0
      val thirdPeriodMegabytes = testSizingFunction(elem3).toDouble / 1024 / 1024
      val thirdPeriodMean = thirdPeriodMegabytes * 1000.0 / periodMillis
      val totalMean = (firstPeriodMean + secondPeriodMean + thirdPeriodMean) / 3
      periodicValue shouldBe SizeMetric.Value(thirdPeriodMean)
      finalValue shouldBe SizeMetric.Value(totalMean)
    }
  }

  private def testSizingFunction(value: String): Long = value.length.toLong * 12345
  private def anEmptySizeMetric(periodMillis: Long = 100): SizeMetric[String] =
    SizeMetric.empty[String](
      periodMillis = periodMillis,
      sizingFunction = testSizingFunction,
    )
}
