// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.Metric.ConsumptionSpeedMetric
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant}
import scala.util.Random

class ConsumptionSpeedMetricSpec extends AnyWordSpec with Matchers {
  ConsumptionSpeedMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val metric = ConsumptionSpeedMetric.empty[String](100, dummyRecordTimesFunction)

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(1.0)

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(0.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "compute values after processing elements" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "abc"
      val elem2: String = "defg"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = aRecordTimesList(testNow)
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = aRecordTimesList(recordTimes1.last)
      val periodMillis = 100L
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](
        periodMillis = periodMillis,
        recordTimeFunction = testRecordTimeFunction,
      )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val firstElementOfThePeriod = recordTimes1.head
      val lastElementOfThePeriod = recordTimes2.last
      val expectedSpeed =
        (lastElementOfThePeriod.getEpochSecond - firstElementOfThePeriod.getEpochSecond) * 1000.0 / periodMillis

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(expectedSpeed))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "correctly handle periods with a single record time" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "abc"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(testNow.minusSeconds(11))
      // The assumption made here is that each consecutive element has higher record times
      val periodMillis = 100L
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](
        periodMillis = periodMillis,
        recordTimeFunction = testRecordTimeFunction,
      )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(0.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "correctly handle periods with no elements" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "abc"
      val elem2: String = "defg"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = aRecordTimesList(testNow)
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = aRecordTimesList(recordTimes1.last)
      val periodMillis = 100L
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](
        periodMillis = periodMillis,
        recordTimeFunction = testRecordTimeFunction,
      )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue()
        ._1
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(0.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "correctly handle multiple periods with elements" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "abc"
      val elem2: String = "defghj"
      val elem3: String = "klm"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = aRecordTimesList(testNow)
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = aRecordTimesList(recordTimes1.last)
      val recordTimes3 = aRecordTimesList(recordTimes2.last)
      val periodMillis = 100L
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
          elem3 -> recordTimes3,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](
        periodMillis = periodMillis,
        recordTimeFunction = testRecordTimeFunction,
      )

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

      val firstElementOfThePeriod = recordTimes3.head
      val lastElementOfThePeriod = recordTimes3.last
      val expectedSpeed =
        (lastElementOfThePeriod.getEpochSecond - firstElementOfThePeriod.getEpochSecond) * 1000.0 / periodMillis

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(expectedSpeed))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }
  }

  private def aRecordTimesList(beforeInstant: Instant): List[Instant] =
    (2 to 2 + Random.nextInt(10)).toList.map { _ =>
      beforeInstant.minusSeconds(Random.nextLong(1000))
    }.sorted

  private def recordTimeFunctionFromMap(
      map: Map[String, List[Instant]]
  )(str: String): List[Timestamp] =
    map
      .map { case (k, v) => k -> v.map(instantToTimestamp) }
      .getOrElse(str, throw new RuntimeException(s"Unexpected record function argument: $str"))

  private def instantToTimestamp(instant: Instant): Timestamp =
    Timestamp.of(instant.getEpochSecond, instant.getNano)

  private def dummyRecordTimesFunction(str: String): List[Timestamp] =
    str.map(_ => Timestamp.of(100, 0)).toList
}
