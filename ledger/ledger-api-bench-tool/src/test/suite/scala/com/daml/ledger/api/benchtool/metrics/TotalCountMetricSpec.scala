// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.TotalCountMetric.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class TotalCountMetricSpec extends AnyWordSpec with Matchers {
  TotalCountMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(1)
      val metric: TotalCountMetric[String] = anEmptyStringMetric()

      val (_, periodicValue) = metric.periodicValue(periodDuration)
      val finalValue = metric.finalValue(totalDuration)

      periodicValue shouldBe Value(0)
      finalValue shouldBe Value(0)
    }

    "compute values after processing elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: TotalCountMetric[String] = anEmptyStringMetric()
      val elem1: String = "abc"
      val elem2: String = "defg"

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val totalCount: Int = stringLength(elem1) + stringLength(elem2)
      periodicValue shouldBe Value(totalCount)
      finalValue shouldBe Value(totalCount)
    }

    "correctly handle periods with no elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: TotalCountMetric[String] = anEmptyStringMetric()
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
      periodicValue shouldBe Value(totalCount)
      finalValue shouldBe Value(totalCount)
    }

    "correctly handle multiple periods with elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val metric: TotalCountMetric[String] = anEmptyStringMetric()
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
      periodicValue shouldBe Value(totalCount)
      finalValue shouldBe Value(totalCount)
    }
  }

  private def stringLength(value: String): Int = value.length
  private def anEmptyStringMetric(): TotalCountMetric[String] =
    TotalCountMetric.empty[String](stringLength)
}
