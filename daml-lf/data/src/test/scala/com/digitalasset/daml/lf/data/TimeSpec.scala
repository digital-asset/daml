// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.{Duration, Instant, LocalDate}
import java.util.concurrent.TimeUnit

import com.daml.lf.data.Time._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class TimeSpec extends FreeSpec with Matchers with TableDrivenPropertyChecks {

  "Date operations" - {

    "There is 0 days since Date.Epoch" in {
      Date.Epoch.days shouldBe 0
    }

    "Date.Epoch is 1970-01-01" in {
      Date.Epoch shouldBe Date.assertFromString("1970-01-01")
    }

    "Date.fromLong fails if it overflows" in {
      val max = Date.MaxValue.days
      val min = Date.MinValue.days
      Date.fromDaysSinceEpoch(max) shouldBe 'right
      Date.fromDaysSinceEpoch(max + 1) shouldBe 'left
      Date.fromDaysSinceEpoch(min) shouldBe 'right
      Date.fromDaysSinceEpoch(min - 1) shouldBe 'left
    }

    "Date.fromString fails if it overflows" in {
      val max = Date.MaxValue.toString
      val min = Date.MinValue.toString
      Date.fromString(max) shouldBe 'right
      Date.fromString(LocalDate.parse(max).plusDays(1).toString) shouldBe 'left
      Date.fromString(min) shouldBe 'right
      Date.fromString(LocalDate.parse(min).plusDays(-1).toString) shouldBe 'left
    }

    "toString produces an ISO 8601 compliant string" in {
      val testCases = Table(
        "days",
        Date.MinValue,
        Date.Epoch,
        Date.assertFromString("2001-01-01"),
        Date.MaxValue
      )

      forEvery(testCases) { date =>
        DateTimeFormatter.ISO_DATE
          .parse(date.toString)
          .getLong(ChronoField.EPOCH_DAY) shouldBe date.days
      }
    }
  }

  "Timestamp operations" - {
    "There is 0 micros since Timestamp.Epoch" in {
      Timestamp.Epoch.micros shouldBe 0
    }

    "Timestamp.Epoch is 1970-01-01T00:00:00Z" in {
      Timestamp.Epoch shouldBe Timestamp.assertFromString("1970-01-01T00:00:00Z")
    }

    "Timestamp.fromLong fails if it overflows" in {
      val max = Timestamp.MaxValue.micros
      val min = Timestamp.MinValue.micros
      Timestamp.fromLong(max) shouldBe 'right
      Timestamp.fromLong(max + 1) shouldBe 'left
      Timestamp.fromLong(min) shouldBe 'right
      Timestamp.fromLong(min - 1) shouldBe 'left
    }

    "Timestamp.fromString fails if it overflows" in {
      val max = Timestamp.MaxValue.toString
      val min = Timestamp.MinValue.toString
      Timestamp.fromString(max) shouldBe 'right
      Timestamp.fromString(Instant.parse(max).plusMillis(1).toString) shouldBe 'left
      Timestamp.fromString(min) shouldBe 'right
      Timestamp.fromString(Instant.parse(min).plusMillis(-1).toString) shouldBe 'left
    }

    "add increments the timestamp" in {
      val timestamp = Timestamp.assertFromString("2019-04-04T08:33:38.123456Z")
      val incrementedTimestamp = timestamp.add(Duration.ofNanos(1234567000))
      incrementedTimestamp.toString shouldBe "2019-04-04T08:33:39.358023Z"
    }

    "add increments the timestamp even when the duration can't be turned into nanoseconds" in {
      val timestamp = Timestamp.Epoch
      val incrementedTimestamp = timestamp.add(Duration.ofNanos(Long.MaxValue).plusNanos(1))
      incrementedTimestamp.toString shouldBe "2262-04-11T23:47:16.854775Z"
    }

    "addMicros increments the timestamp" in {
      val timestamp = Timestamp.assertFromString("2019-04-04T08:33:38.123456Z")
      val incrementedTimestamp = timestamp.addMicros(1234567)
      incrementedTimestamp.toString shouldBe "2019-04-04T08:33:39.358023Z"
    }

    "addMicros throws an error if it overflows" in {
      Timestamp.MaxValue.addMicros(0) // should not throw an exception
      an[IllegalArgumentException] should be thrownBy Timestamp.MaxValue.addMicros(1)
    }

    "toString produces an ISO 8601 compliant string" in {
      val testCases = Table(
        "timeStamp",
        Timestamp.MinValue,
        Timestamp.Epoch,
        Timestamp.assertFromString("1969-07-20T20:17:00Z"),
        Timestamp.assertFromString("1969-07-20T20:17:00.1Z"),
        Timestamp.assertFromString("1969-07-20T20:17:00.12Z"),
        Timestamp.assertFromString("1969-07-20T20:17:00.123Z"),
        Timestamp.assertFromString("1969-07-20T20:17:00.1234Z"),
        Timestamp.assertFromString("1969-07-20T20:17:00.12345Z"),
        Timestamp.assertFromString("1969-07-20T20:17:00.123456Z"),
        Timestamp.MaxValue
      )

      forEvery(testCases) { date =>
        val i = Instant.parse(date.toString)
        val micros =
          TimeUnit.SECONDS.toMicros(i.getEpochSecond) +
            TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)

        micros shouldBe date.micros
      }
    }
  }

}
