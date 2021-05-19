// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.Metric.SizeMetric
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class SizeMetricSpec extends AnyWordSpec with Matchers {
  SizeMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val metric: SizeMetric[String] = anEmptySizeMetric()

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(aPositiveDouble())

      periodicValue shouldBe Some(SizeMetric.Value(0.0))
      finalValue shouldBe None
    }

    "compute values after processing elements" in {
      val periodMillis: Long = aPositiveLong()
      val totalDurationSeconds: Double = aPositiveDouble()
      val metric: SizeMetric[String] = anEmptySizeMetric(periodMillis)
      val elem1: String = aString()
      val elem2: String = aString()

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val totalSizeMegabytes =
        (testSizingFunction(elem1) + testSizingFunction(elem2)).toDouble / 1024 / 1024
      periodicValue shouldBe Some(SizeMetric.Value(totalSizeMegabytes * 1000.0 / periodMillis))
      finalValue shouldBe Some(SizeMetric.Value(totalSizeMegabytes * 1000.0 / periodMillis))
    }

    "correctly handle periods with no elements" in {
      val periodMillis: Long = aPositiveLong()
      val totalDurationSeconds: Double = aPositiveDouble()
      val metric: SizeMetric[String] = anEmptySizeMetric(periodMillis)
      val elem1: String = aString()
      val elem2: String = aString()

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
      periodicValue shouldBe Some(SizeMetric.Value(secondPeriodMean))
      finalValue shouldBe Some(SizeMetric.Value(totalMean))
    }

    "correctly handle multiple periods with elements" in {
      val periodMillis: Long = aPositiveLong()
      val totalDurationSeconds: Double = aPositiveDouble()
      val metric: SizeMetric[String] = anEmptySizeMetric(periodMillis)
      val elem1: String = aString()
      val elem2: String = aString()
      val elem3: String = aString()

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
      periodicValue shouldBe Some(SizeMetric.Value(thirdPeriodMean))
      finalValue shouldBe Some(SizeMetric.Value(totalMean))
    }
  }

  private def testSizingFunction(value: String): Long = value.length.toLong * 12345
  private def anEmptySizeMetric(periodMillis: Long = aPositiveLong()): SizeMetric[String] =
    SizeMetric.empty[String](
      periodMillis = periodMillis,
      sizingFunction = testSizingFunction,
    )

  private def aString(): String = Random.nextString(Random.nextInt(50))
  private def aPositiveInt(): Int = Random.nextInt(100000)
  private def aPositiveLong(): Long = aPositiveInt().toLong
  private def aPositiveDouble(): Double = Random.nextDouble() * aPositiveInt()
}
