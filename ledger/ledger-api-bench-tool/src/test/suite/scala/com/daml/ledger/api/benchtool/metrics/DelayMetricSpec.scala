// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.Metric.DelayMetric
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Duration, Instant, ZoneId}
import scala.util.Random

class DelayMetricSpec extends AnyWordSpec with Matchers {
  DelayMetric.getClass.getSimpleName should {
    "correctly handle initial state" in {
      val metric: DelayMetric[String] = anEmptyDelayMetric(Clock.systemUTC())

      val (_, periodicValue) = metric.periodicValue()
      val finalValue = metric.finalValue(aPositiveDouble())

      periodicValue shouldBe None
      finalValue shouldBe None
    }

    "compute values after processing elements" in {
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val elem2: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTime1 = testNow.minusSeconds(aPositiveLong())
      val recordTime2 = testNow.minusSeconds(aPositiveLong())
      val recordTime3 = testNow.minusSeconds(aPositiveLong())
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
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val expectedMean = (delay1 + delay2 + delay3) / 3
      periodicValue shouldBe Some(DelayMetric.Value(expectedMean))
      finalValue shouldBe None
    }

    "correctly handle periods with no elements" in {
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val elem2: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTime1 = testNow.minusSeconds(aPositiveLong())
      val recordTime2 = testNow.minusSeconds(aPositiveLong())
      val recordTime3 = testNow.minusSeconds(aPositiveLong())
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
        .periodicValue()
        ._1
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      periodicValue shouldBe None
      finalValue shouldBe None
    }

    "correctly handle multiple periods with elements" in {
      val totalDurationSeconds: Double = aPositiveDouble()
      val elem1: String = aString()
      val elem2: String = aString()
      val elem3: String = aString()
      val testNow = Clock.systemUTC().instant()
      val recordTime1 = testNow.minusSeconds(aPositiveLong())
      val recordTime2 = testNow.minusSeconds(aPositiveLong())
      val recordTime3 = testNow.minusSeconds(aPositiveLong())
      val recordTime4 = testNow.minusSeconds(aPositiveLong())
      val recordTime5 = testNow.minusSeconds(aPositiveLong())
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
        .periodicValue()
        ._1
        .periodicValue()
        ._1
        .onNext(elem3)
        .periodicValue()
      val finalValue = newMetric.finalValue(totalDurationSeconds)

      val expectedMean = (delay4 + delay5) / 2
      periodicValue shouldBe Some(DelayMetric.Value(expectedMean))
      finalValue shouldBe None
    }
  }

  private def recordTimeFunctionFromMap(
      map: Map[String, List[Instant]]
  )(str: String): List[Timestamp] =
    map.view
      .mapValues(_.map(instantToTimestamp))
      .getOrElse(str, throw new RuntimeException(s"Unexpected record function argument: $str"))

  private def instantToTimestamp(instant: Instant): Timestamp =
    Timestamp.of(instant.getEpochSecond, instant.getNano)

  private def secondsBetween(first: Instant, second: Instant): Long =
    Duration.between(first, second).getSeconds

  private def dummyRecordTimesFunction(str: String): List[Timestamp] =
    str.map(_ => Timestamp.of(aPositiveLong(), 0)).toList

  private def anEmptyDelayMetric(clock: Clock): DelayMetric[String] =
    DelayMetric.empty[String](dummyRecordTimesFunction, clock)

  private def aString(): String = Random.nextString(Random.nextInt(50))
  private def aPositiveInt(): Int = Random.nextInt(100000)
  private def aPositiveLong(): Long = aPositiveInt().toLong
  private def aPositiveDouble(): Double = Random.nextDouble() * aPositiveInt()
}
