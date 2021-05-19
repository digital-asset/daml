// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.Metric.CountMetric
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class CountMetricSpec extends AnyWordSpec with Matchers {
  CountMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val metric: CountMetric[String] = anEmptyStringMetric()

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(aPositiveDouble())

      periodicValue shouldBe Some(CountMetric.Value(0, 0.0))
      finalValue shouldBe Some(CountMetric.Value(0, 0.0))
    }

    "compute values after processing elements" in {
      val periodMillis: Long = aPositiveLong()
      val totalDurationSeconds: Double = aPositiveDouble()
      val metric: CountMetric[String] = anEmptyStringMetric(periodMillis)
      val elem1: String = aString()
      val elem2: String = aString()

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2)
      periodicValue shouldBe Some(
        CountMetric.Value(
          totalCount = totalCount,
          ratePerSecond = totalCount * 1000.0 / periodMillis,
        )
      )
      finalValue shouldBe Some(
        CountMetric.Value(
          totalCount = totalCount,
          ratePerSecond = totalCount / totalDurationSeconds,
        )
      )
    }

    "correctly handle periods with no elements" in {
      val periodMillis: Long = aPositiveLong()
      val totalDurationSeconds: Double = aPositiveDouble()
      val metric: CountMetric[String] = anEmptyStringMetric(periodMillis)
      val elem1: String = aString()
      val elem2: String = aString()

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
        ._1
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2)
      periodicValue shouldBe Some(
        CountMetric.Value(
          totalCount = totalCount,
          ratePerSecond = 0.0,
        )
      )
      finalValue shouldBe Some(
        CountMetric.Value(
          totalCount = totalCount,
          ratePerSecond = totalCount / totalDurationSeconds,
        )
      )
    }

    "correctly handle multiple periods with elements" in {
      val periodMillis: Long = aPositiveLong()
      val totalDurationSeconds: Double = aPositiveDouble()
      val metric: CountMetric[String] = anEmptyStringMetric(periodMillis)
      val elem1: String = aString()
      val elem2: String = aString()
      val elem3: String = aString()

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
        ._1
        .onNext(elem3)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2) + stringLength(elem3)
      periodicValue shouldBe Some(
        CountMetric.Value(
          totalCount = totalCount,
          ratePerSecond = stringLength(elem3) * 1000.0 / periodMillis,
        )
      )
      finalValue shouldBe Some(
        CountMetric.Value(
          totalCount = totalCount,
          ratePerSecond = totalCount / totalDurationSeconds,
        )
      )
    }
  }

  private def stringLength(value: String): Int = value.length
  private def anEmptyStringMetric(periodMillis: Long = aPositiveLong()): CountMetric[String] =
    CountMetric.empty[String](
      periodMillis = periodMillis,
      countingFunction = stringLength,
    )

  private def aString(): String = Random.nextString(Random.nextInt(50))
  private def aPositiveInt(): Int = Random.nextInt(100000)
  private def aPositiveLong(): Long = aPositiveInt().toLong
  private def aPositiveDouble(): Double = Random.nextDouble() * aPositiveInt()
}
