// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CronTest extends AnyWordSpec with BaseTest {
  "Valid crons accepted" in {
    Cron.tryCreate("/10 * * * * ? *")
    Cron.tryCreate("10-20 0 * * * ? *")
    Cron.tryCreate("* 0-10 7,19 * * ? *")
    Cron.tryCreate("* * * * * ?")

    clue("Apparently no such thing as too many fields") {
      Cron.create("* * * * * ? * foobar goo moo").map(_.toString) shouldBe Right(
        "* * * * * ? * FOOBAR GOO MOO"
      )
    }
  }

  "Invalid crons rejected with reasonable error message" in {
    Cron.create("obviously wrong") shouldBe Left(
      "Invalid cron expression \"obviously wrong\": Illegal characters for this position: 'OBV'"
    )

    clue("too few fields") {
      Cron.create("* * * * *") shouldBe Left(
        "Invalid cron expression \"* * * * *\": Unexpected end of expression."
      )
    }

    clue("last field messed up") {
      Cron.create("* * * * * ? foo") shouldBe Left(
        "Invalid cron expression \"* * * * * ? foo\": Illegal characters for this position: 'FOO'"
      )
    }

    Cron.create("* * * * * * *") shouldBe Left(
      "Invalid cron expression \"* * * * * * *\": Support for specifying both a day-of-week AND a day-of-month parameter is not implemented."
    )

    val tooLong = "1234567890" * 30 + "too long"
    Cron.create(tooLong).leftOrFail("cron expression too long") should startWith
    s"Invalid cron expression \"${tooLong}\": requirement failed: The given string has a maximum length of 300 but a string of length 308"

  }

  "Valid crons produce reasonable future next valid times" in {
    // Run every 10 minutes, on the hours of 8am/pm, in December 2022
    val cron = Cron.tryCreate("0 /10 8,20 * 12 ? 2022")

    // Another way to express the same schedule building a cartesian product of day, hour, and minute
    val expectedScheduleTimes =
      for { dayOfMonth <- 1 to 31; hour <- Seq(8, 20); minute <- Range(0, 60, 10) } yield date(
        dayOfMonth,
        hour,
        minute,
      )

    // Zip with next time to juxtapose reference time with next expected schedule time
    expectedScheduleTimes.zip(expectedScheduleTimes.drop(1)).foreach {
      case (referenceTime, nextScheduledExpected) =>
        val scheduledActual =
          cron
            .getNextValidTimeAfter(referenceTime)
            .valueOr(err => fail(s"expect valid date but got err: ${err.message}"))
        scheduledActual shouldBe nextScheduledExpected
    }
  }

  "A valid cron that ends at a certain point yields no next valid time thereafter" in {
    // Only run in 2021 and not in subsequent years
    val cron = Cron.tryCreate("* * * * * ? 2021")

    val dateAfterCronWindow = date(1, 12, 0)
    val never = cron.getNextValidTimeAfter(dateAfterCronWindow)
    never shouldBe Left(Cron.NoNextValidTimeAfter(dateAfterCronWindow))
  }

  private def date(dayOfMonth: Int, hour: Int, minute: Int): CantonTimestamp = {
    def withLeadingZero(i: Int) = "%02d".format(i)

    CantonTimestamp
      .fromInstant(
        Instant.parse(
          s"2022-12-${withLeadingZero(dayOfMonth)}T${withLeadingZero(hour)}:${withLeadingZero(minute)}:00Z"
        )
      )
      .valueOrFail("valid date expected")
  }
}
