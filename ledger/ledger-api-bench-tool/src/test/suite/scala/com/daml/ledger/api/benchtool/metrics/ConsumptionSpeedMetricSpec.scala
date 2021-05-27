// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.ConsumptionSpeedMetric
import com.daml.ledger.api.benchtool.metrics.objectives.MinConsumptionSpeed
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant}
import scala.language.existentials

class ConsumptionSpeedMetricSpec extends AnyWordSpec with Matchers {
  ConsumptionSpeedMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val metric = ConsumptionSpeedMetric.empty[String](100, _ => List.empty, List.empty)

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(1.0)

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(0.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "compute values after processing elements" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "a"
      val elem2: String = "d"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(
        testNow.minusSeconds(100),
        testNow.minusSeconds(50),
      )
      val recordTimes2 = List(
        testNow.minusSeconds(20)
      )
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
        objectives = List.empty,
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

    "correctly handle initial periods with a single record time" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "a"
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
        objectives = List.empty,
      )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(0.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "correctly handle non-initial periods with a single record time" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "a"
      val elem2: String = "b"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(
        testNow.minusSeconds(100),
        testNow.minusSeconds(50),
      )
      val recordTimes2 = List(
        testNow.minusSeconds(20)
      )
      // The assumption made here is that each consecutive element has higher record times
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
        objectives = List.empty,
      )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .periodicValue()
        ._1
        .onNext(elem2)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(300.0))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "correctly handle periods with no elements" in {
      val totalDurationSeconds: Double = 3.0
      val elem1: String = "a"
      val elem2: String = "b"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(
        testNow.minusSeconds(100)
      )
      val recordTimes2 = List(
        testNow.minusSeconds(90)
      )
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
        objectives = List.empty,
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
      val elem1: String = "a"
      val elem2: String = "b"
      val elem3: String = "c"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(
        testNow.minusSeconds(100),
        testNow.minusSeconds(90),
      )
      // The assumption made here is that each consecutive element has higher record times
      val recordTimes2 = List(
        testNow.minusSeconds(70),
        testNow.minusSeconds(40),
      )
      val recordTimes3 = List(
        testNow.minusSeconds(20),
        testNow.minusSeconds(15),
      )
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
        objectives = List.empty,
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

      val first = recordTimes2.last
      val last = recordTimes3.last
      val expectedSpeed =
        (last.getEpochSecond - first.getEpochSecond) * 1000.0 / periodMillis

      periodicValue shouldBe ConsumptionSpeedMetric.Value(Some(expectedSpeed))
      finalValue shouldBe ConsumptionSpeedMetric.Value(None)
    }

    "compute violated min speed SLO and the minimum speed" in {
      val testNow = Clock.systemUTC().instant()
      val minAllowedSpeed = 2.0
      val periodMillis = 100L
      val elem1 = "a"
      val elem2 = "b"
      val elem3 = "c"
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> List(
            testNow.minusMillis(5000),
            testNow.minusMillis(4500),
            testNow.minusMillis(4000),
          ), // ok, speed = 10.0
          elem2 -> List(
            testNow.minusMillis(3990),
            testNow.minusMillis(3980),
            testNow.minusMillis(3920), // not ok, speed 0.8
          ),
          elem3 -> List(
            testNow.minusMillis(3900),
            testNow.minusMillis(3800),
            testNow.minusMillis(3770), // not ok, speed 1.5
          ),
        )
      )

      val objective = MinConsumptionSpeed(minAllowedSpeed)
      val metric: ConsumptionSpeedMetric[String] =
        ConsumptionSpeedMetric.empty[String](
          periodMillis = periodMillis,
          recordTimeFunction = testRecordTimeFunction,
          objectives = List(objective),
        )

      val violatedObjectives =
        metric
          .onNext(elem1)
          .periodicValue()
          ._1
          .onNext(elem2)
          .periodicValue()
          ._1
          .onNext(elem3)
          .periodicValue()
          ._1
          .violatedObjectives

      violatedObjectives shouldBe Map(
        objective -> ConsumptionSpeedMetric.Value(Some(0.8))
      )
    }
  }

  private def recordTimeFunctionFromMap(
      map: Map[String, List[Instant]]
  )(str: String): List[Timestamp] =
    map
      .map { case (k, v) => k -> v.map(instantToTimestamp) }
      .getOrElse(str, throw new RuntimeException(s"Unexpected record function argument: $str"))

  private def instantToTimestamp(instant: Instant): Timestamp =
    Timestamp.of(instant.getEpochSecond, instant.getNano)
}
