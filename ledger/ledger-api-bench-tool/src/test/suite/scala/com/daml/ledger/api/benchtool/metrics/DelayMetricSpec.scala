// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.DelayMetric._
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Duration, Instant, ZoneId}
import scala.language.existentials

class DelayMetricSpec extends AnyWordSpec with Matchers {
  DelayMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val metric: DelayMetric[String] = anEmptyDelayMetric(Clock.systemUTC())

      val (_, periodicValue) = metric.periodicValue(periodDuration)
      val totalDuration: Duration = Duration.ofSeconds(1)
      val finalValue = metric.finalValue(totalDuration)

      periodicValue shouldBe Value(None)
      finalValue shouldBe Value(None)
    }

    "compute values after processing elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val elem1: String = "abc"
      val elem2: String = "defgh"
      val testNow = Clock.systemUTC().instant()
      val recordTime1 = testNow.minusSeconds(11)
      val recordTime2 = testNow.minusSeconds(22)
      val recordTime3 = testNow.minusSeconds(33)
      val delay1 = secondsBetween(recordTime1, testNow)
      val delay2 = secondsBetween(recordTime2, testNow)
      val delay3 = secondsBetween(recordTime3, testNow)
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> List(recordTime1, recordTime2),
          elem2 -> List(recordTime3),
        )
      )
      val clock = Clock.fixed(testNow, ZoneId.of("UTC"))
      val metric: DelayMetric[String] =
        DelayMetric.empty[String](
          recordTimeFunction = testRecordTimeFunction,
          clock = clock,
        )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      val expectedMean = (delay1 + delay2 + delay3) / 3
      periodicValue shouldBe Value(Some(expectedMean))
      finalValue shouldBe Value(None)
    }

    "correctly handle periods with no elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val elem1: String = "abc"
      val elem2: String = "defg"
      val testNow = Clock.systemUTC().instant()
      val recordTime1 = testNow.minusSeconds(11)
      val recordTime2 = testNow.minusSeconds(22)
      val recordTime3 = testNow.minusSeconds(33)
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> List(recordTime1, recordTime2),
          elem2 -> List(recordTime3),
        )
      )
      val clock = Clock.fixed(testNow, ZoneId.of("UTC"))
      val metric: DelayMetric[String] =
        DelayMetric.empty[String](
          recordTimeFunction = testRecordTimeFunction,
          clock = clock,
        )

      val (newMetric, periodicValue) = metric
        .onNext(elem1)
        .onNext(elem2)
        .periodicValue(periodDuration)
        ._1
        .periodicValue(periodDuration)
      val finalValue = newMetric.finalValue(totalDuration)

      periodicValue shouldBe Value(None)
      finalValue shouldBe Value(None)
    }

    "correctly handle multiple periods with elements" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val totalDuration: Duration = Duration.ofSeconds(5)
      val elem1: String = "abc"
      val elem2: String = "defg"
      val elem3: String = "hij"
      val testNow = Clock.systemUTC().instant()
      val recordTime1 = testNow.minusSeconds(11)
      val recordTime2 = testNow.minusSeconds(22)
      val recordTime3 = testNow.minusSeconds(33)
      val recordTime4 = testNow.minusSeconds(44)
      val recordTime5 = testNow.minusSeconds(55)
      val delay4 = secondsBetween(recordTime4, testNow)
      val delay5 = secondsBetween(recordTime5, testNow)
      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> List(recordTime1, recordTime2),
          elem2 -> List(recordTime3),
          elem3 -> List(recordTime4, recordTime5),
        )
      )
      val clock = Clock.fixed(testNow, ZoneId.of("UTC"))
      val metric: DelayMetric[String] =
        DelayMetric.empty[String](
          recordTimeFunction = testRecordTimeFunction,
          clock = clock,
        )

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

      val expectedMean = (delay4 + delay5) / 2
      periodicValue shouldBe Value(Some(expectedMean))
      finalValue shouldBe Value(None)
    }

    "compute violated max delay SLO with the most extreme value" in {
      val periodDuration: Duration = Duration.ofMillis(100)
      val maxAllowedDelaySeconds: Long = 1000
      val elem1: String = "abc"
      val elem2: String = "defg"
      val elem3: String = "hijkl"
      val elem4: String = "mno"
      val testNow = Clock.systemUTC().instant()

      // first period
      val recordTime1 =
        testNow.minusSeconds(maxAllowedDelaySeconds - 100) // allowed record time

      // second period
      val recordTime2A =
        testNow.minusSeconds(maxAllowedDelaySeconds + 100) // not allowed record time
      val recordTime2B =
        testNow.minusSeconds(maxAllowedDelaySeconds + 200) // not allowed record time
      val delay2A = durationBetween(recordTime2A, testNow)
      val delay2B = durationBetween(recordTime2B, testNow)
      val meanInPeriod2 = delay2A.plus(delay2B).dividedBy(2).getSeconds

      // third period - a period with record times higher than anywhere else,
      // the mean delay from this period should be provided by the metric as the most violating value
      val recordTime3A = testNow.minusSeconds(
        maxAllowedDelaySeconds + 1100
      ) // not allowed record time
      val recordTime3B = testNow.minusSeconds(
        maxAllowedDelaySeconds + 1200
      ) // not allowed record time
      val delay3A = durationBetween(recordTime3A, testNow)
      val delay3B = durationBetween(recordTime3B, testNow)
      val meanInPeriod3 = delay3A.plus(delay3B).dividedBy(2).getSeconds

      // fourth period
      val recordTime4 =
        testNow.minusSeconds(maxAllowedDelaySeconds + 300) // not allowed record time
      val delay4 = durationBetween(recordTime4, testNow)
      val meanInPeriod4 = delay4.getSeconds

      val maxDelay = List(meanInPeriod2, meanInPeriod3, meanInPeriod4).max

      def testRecordTimeFunction: String => List[Timestamp] = recordTimeFunctionFromMap(
        Map(
          elem1 -> List(recordTime1),
          elem2 -> List(recordTime2A, recordTime2B),
          elem3 -> List(recordTime3A, recordTime3B),
          elem4 -> List(recordTime4),
        )
      )
      val expectedViolatedObjective = MaxDelay(maxAllowedDelaySeconds)
      val clock = Clock.fixed(testNow, ZoneId.of("UTC"))
      val metric: DelayMetric[String] =
        DelayMetric.empty[String](
          recordTimeFunction = testRecordTimeFunction,
          clock = clock,
          objective = Some(expectedViolatedObjective),
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
          .onNext(elem4)
          .periodicValue(periodDuration)
          ._1
          .violatedPeriodicObjectives

      violatedObjectives shouldBe List(
        expectedViolatedObjective -> Value(Some(maxDelay))
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

  private def durationBetween(first: Instant, second: Instant): Duration =
    Duration.between(first, second)

  private def secondsBetween(first: Instant, second: Instant): Long =
    Duration.between(first, second).getSeconds

  private def dummyRecordTimesFunction(str: String): List[Timestamp] =
    str.map(_ => Timestamp.of(100, 0)).toList

  private def anEmptyDelayMetric(clock: Clock): DelayMetric[String] =
    DelayMetric.empty[String](dummyRecordTimesFunction, clock)
}
