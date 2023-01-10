// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.ConsumptionSpeedMetric._
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Duration, Instant}
import scala.language.existentials

class ConsumptionSpeedMetricSpec extends AnyWordSpec with Matchers {
  ConsumptionSpeedMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val metric = ConsumptionSpeedMetric.empty[String](_ => List.empty)

      val (_, periodicValue) = metric.periodicValue(Duration.ofMillis(100))
      val finalValue = metric.finalValue(Duration.ofSeconds(1))

      periodicValue shouldBe Value(Some(0.0))
      finalValue shouldBe Value(None)
    }

    "compute values after processing elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(3)
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
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](testRecordTimeFunction)

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val firstElementOfThePeriod = recordTimes1.head
      val lastElementOfThePeriod = recordTimes2.last
      val expectedSpeed =
        (lastElementOfThePeriod.getEpochSecond - firstElementOfThePeriod.getEpochSecond) * 1000.0 / periodDuration.toMillis

      periodicValue shouldBe Value(Some(expectedSpeed))
      finalValue shouldBe Value(None)
    }

    "correctly handle initial periods with a single record time" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(3)
      val elem1: String = "a"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(testNow.minusSeconds(11))
      // The assumption made here is that each consecutive element has higher record times
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](testRecordTimeFunction)

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      periodicValue shouldBe Value(Some(0.0))
      finalValue shouldBe Value(None)
    }

    "correctly handle non-initial periods with a single record time" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(3)
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
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](testRecordTimeFunction)

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .periodicValue(periodDuration)
        ._1
        .onNext(elem2)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      periodicValue shouldBe Value(Some(300.0))
      finalValue shouldBe Value(None)
    }

    "correctly handle periods with no elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(3)
      val elem1: String = "a"
      val elem2: String = "b"
      val testNow = Clock.systemUTC().instant()
      val recordTimes1 = List(
        testNow.minusSeconds(100)
      )
      val recordTimes2 = List(
        testNow.minusSeconds(90)
      )
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](testRecordTimeFunction)

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
        ._1
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      periodicValue shouldBe Value(Some(0.0))
      finalValue shouldBe Value(None)
    }

    "correctly handle multiple periods with elements" in {
      val period1Duration: Duration = Duration.ofMillis(100)
      val period2Duration: Duration = Duration.ofMillis(120)
      val period3Duration: Duration = Duration.ofMillis(110)
      val totalDuration: Duration = Duration.ofSeconds(3)
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
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> recordTimes1,
          elem2 -> recordTimes2,
          elem3 -> recordTimes3,
        )
      )

      val metric = ConsumptionSpeedMetric.empty[String](testRecordTimeFunction)

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(period1Duration)
        ._1
        .periodicValue(period2Duration)
        ._1
        .onNext(elem3)
        .periodicValue(period3Duration)
      val finalValue = newMetric.finalValue(totalDuration)

      val first = recordTimes2.last
      val last = recordTimes3.last
      val expectedSpeed =
        (last.getEpochSecond - first.getEpochSecond) * 1000.0 / period3Duration.toMillis

      periodicValue shouldBe Value(Some(expectedSpeed))
      finalValue shouldBe Value(None)
    }

    "compute violated min speed SLO and the minimum speed" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val testNow = Clock.systemUTC().instant()
      val minAllowedSpeed = 2.0
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
          recordTimeFunction = testRecordTimeFunction,
          objective = Some(objective),
        )

      val violatedObjectives =
        metric
          .onNext(elem1)
          .periodicValue(periodDuration)
          ._1
          .onNext(elem2)
          .periodicValue(periodDuration)
          ._1
          .onNext(elem3)
          .periodicValue(periodDuration)
          ._1
          .violatedPeriodicObjectives

      violatedObjectives shouldBe List(
        objective -> Value(Some(0.8))
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
