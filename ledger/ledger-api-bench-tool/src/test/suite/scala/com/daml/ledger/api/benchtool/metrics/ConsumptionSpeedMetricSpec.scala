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
      val metric = ConsumptionSpeedMetric.empty[String](aPositiveLong(), dummyRecordTimesFunction)

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(aPositiveDouble())

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(0.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "compute values after processing elements" in {
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val elem2: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = aRecordTimesList(testNow)
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = aRecordTimesList(recordTimes1.last)
      val periodMillis = aPositiveLong()
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
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(testNow.minusSeconds(aPositiveLong()))
      // The assumption made here is that each consecutive element has higher record times
      val periodMillis = aPositiveLong()
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
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val elem2: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = aRecordTimesList(testNow)
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = aRecordTimesList(recordTimes1.last)
      val periodMillis = aPositiveLong()
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
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val elem2: String = aString()
      val elem3: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = aRecordTimesList(testNow)
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = aRecordTimesList(recordTimes1.last)
      val recordTimes3 = aRecordTimesList(recordTimes2.last)
      val periodMillis = aPositiveLong()
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
      beforeInstant.minusSeconds(aPositiveLong())
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
    str.map(_ => Timestamp.of(aPositiveLong(), 0)).toList

  private def aString(): String = Random.nextString(Random.nextInt(50))
  private def aPositiveInt(): Int = Random.nextInt(100000)
  private def aPositiveLong(): Long = aPositiveInt().toLong
  private def aPositiveDouble(): Double = Random.nextDouble() * aPositiveInt()
}
