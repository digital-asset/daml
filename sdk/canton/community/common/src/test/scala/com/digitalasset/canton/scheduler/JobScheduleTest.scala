// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.syntax.option.*
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.scheduler.JobSchedule.NextRun
import com.digitalasset.canton.scheduler.JobScheduler.*
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds, SimClock}

import java.time.Instant

class JobScheduleTest extends BaseTestWordSpec {
  private val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
  private val dummyRetention = PositiveSeconds.tryOfSeconds(7L)
  private val hourlyScheduleForTenMinutes = new PruningCronSchedule(
    Cron.tryCreate(s"0 0 /1 * * ? *"),
    PositiveSeconds.tryOfMinutes(10L),
    dummyRetention,
    clock,
    logger,
  )

  "An interval schedule" when {

    "schedule according to interval" in {
      val interval = PositiveSeconds.tryOfHours(2L)
      val schedule =
        JobSchedule.fromPruningSchedule(None, interval.some, clock, logger).value
      val NextRun(wait, sameSchedule) = schedule.determineNextRun(Done).value

      wait.duration shouldBe interval.duration
      sameSchedule shouldBe schedule
    }

    "not schedule when interval is infinite" in {
      val schedule = JobSchedule
        .fromPruningSchedule(None, None, clock, logger)
      schedule shouldBe None
    }
  }

  "A cron schedule" when {
    // See additional coverage in CronTest
    "schedule according to cron" in {
      val hoursFromMidnight = 7L
      val schedule =
        new PruningCronSchedule(
          Cron.tryCreate(s"0 0 $hoursFromMidnight * * ? *"),
          PositiveSeconds.tryOfSeconds(5L),
          dummyRetention,
          clock,
          logger,
        )

      val NextRun(wait, sameSchedule) = schedule.determineNextRun(Done).value
      wait shouldBe NonNegativeFiniteDuration.tryOfHours(hoursFromMidnight)
      sameSchedule shouldBe schedule
    }

    def durationUntilNextRun(
        resultFromPreviousRun: ScheduledRunResult,
        datetime: String,
    ): Option[NonNegativeFiniteDuration] =
      hourlyScheduleForTenMinutes.waitDurationUntilNextRun(
        resultFromPreviousRun,
        CantonTimestamp.assertFromInstant(Instant.parse(datetime)),
        logger,
      )

    "schedule work at the beginning of next window if work in current window is done" in {
      durationUntilNextRun(
        Done,
        "2024-05-17T10:05:00.00Z",
      ) shouldBe NonNegativeFiniteDuration.tryOfMinutes(55L).some
    }

    "schedule work immediately if current window is still active" in {
      durationUntilNextRun(
        MoreWorkToPerform,
        "2024-05-17T10:09:56.00Z",
      ) shouldBe NonNegativeFiniteDuration.Zero.some
    }

    "schedule work at the beginning of next window if current window no longer active" in {
      durationUntilNextRun(
        MoreWorkToPerform,
        "2024-05-17T10:10:00.00Z",
      ) shouldBe NonNegativeFiniteDuration.tryOfMinutes(50L).some
    }

    "schedule retry after backoff if current window is still active after backoff" in {
      durationUntilNextRun(
        Error(
          "enough time to retry in current window",
          backoff = NonNegativeFiniteDuration.tryOfSeconds(3L),
          logAsInfo = true,
        ),
        "2024-05-17T10:09:56.00Z",
      ) shouldBe NonNegativeFiniteDuration.tryOfSeconds(3L).some
    }

    "schedule retry in next window if current window no longer active after backoff" in {
      durationUntilNextRun(
        Error(
          "cannot retry in current window",
          backoff = NonNegativeFiniteDuration.tryOfMinutes(1L),
          logAsInfo = true,
        ),
        "2024-05-17T10:09:56.00Z",
      ) shouldBe (NonNegativeFiniteDuration.tryOfMinutes(50L) + NonNegativeFiniteDuration
        .tryOfSeconds(
          4L
        )).some
    }

    "schedule retry in next window even if backoff is larger than gaps between windows" in {
      durationUntilNextRun(
        Error(
          "enough time to retry in current window",
          backoff = NonNegativeFiniteDuration.tryOfHours(6L),
          logAsInfo = true,
        ),
        "2024-05-17T10:09:56.00Z",
      ) shouldBe (NonNegativeFiniteDuration.tryOfMinutes(50L) + NonNegativeFiniteDuration
        .tryOfSeconds(
          4L
        )).some
    }

    "schedule work in next window if current time is outside of any window on any result" in {
      Seq[ScheduledRunResult](
        Done,
        MoreWorkToPerform,
        Error("outside of any window", backoff = NonNegativeFiniteDuration.Zero, logAsInfo = true),
      ).foreach { result =>
        clue(s"Trying result $result") {
          durationUntilNextRun(
            result,
            "2024-05-17T10:20:00.00Z",
          ) shouldBe NonNegativeFiniteDuration.tryOfMinutes(40L).some
        }
      }
    }

    "schedule infinite wait on expired cron" in {
      val schedule =
        new PruningCronSchedule(
          Cron.tryCreate(s"0 0 * * * ? 1969"), // before epoch
          PositiveSeconds.tryOfSeconds(5L),
          dummyRetention,
          clock,
          logger,
        )
      val nextRun = schedule.determineNextRun(Done)
      nextRun shouldBe None
    }
  }

  "A compound schedule" when {

    "pick the earlier cron schedule" in {
      checkCompoundSchedule(7L, 8L)
    }

    "pick the earlier interval schedule" in {
      checkCompoundSchedule(8L, 6L)
    }

    "pick the earliest cron schedule" in {
      val fiveHoursFromNow = hoursFromNowCronSchedule(5L)
      val compoundSchedule =
        JobSchedule(
          List(hoursFromNowCronSchedule(6L), fiveHoursFromNow, hoursFromNowCronSchedule(7L))
        ).value
      val NextRun(wait, firstSchedule) = compoundSchedule.determineNextRun(Done).value

      wait shouldBe NonNegativeFiniteDuration.tryOfHours(5L)
      firstSchedule shouldBe fiveHoursFromNow
    }

    "pick no next run time on all expired schedules" in {
      val compoundSchedule =
        JobSchedule(
          List(neverAnymoreSchedule(1967), neverAnymoreSchedule(1968), neverAnymoreSchedule(1969))
        ).value
      val nextRun = compoundSchedule.determineNextRun(Done)
      nextRun shouldBe None
    }
  }

  private def hoursFromNowCronSchedule(hoursUntilCron: Long): IndividualSchedule =
    new PruningCronSchedule(
      Cron.tryCreate(s"0 0 $hoursUntilCron * * ? *"),
      PositiveSeconds.tryOfSeconds(5L),
      dummyRetention,
      clock,
      logger,
    )

  private def neverAnymoreSchedule(previousYear: Long): IndividualSchedule = {
    require(previousYear < 1970)
    new PruningCronSchedule(
      Cron.tryCreate(s"0 0 0 * * ? $previousYear"),
      PositiveSeconds.tryOfSeconds(5L),
      dummyRetention,
      clock,
      logger,
    )
  }

  private def checkCompoundSchedule(hoursUntilCron: Long, hoursInterval: Long) = {
    val cronSchedule = hoursFromNowCronSchedule(hoursUntilCron)
    val intervalSchedule = new IntervalSchedule(PositiveSeconds.tryOfHours(hoursInterval))

    val compoundSchedule =
      new CompoundSchedule(NonEmptyUtil.fromUnsafe(Set(cronSchedule, intervalSchedule)))

    val NextRun(wait, firstSchedule) = compoundSchedule.determineNextRun(Done).value

    // Expect the earlier time to be returned.
    wait shouldBe NonNegativeFiniteDuration.tryOfHours(
      Math.min(hoursUntilCron, hoursInterval)
    )

    // Expect the schedule with the earlier time to be returned.
    firstSchedule shouldBe (if (hoursUntilCron < hoursInterval) cronSchedule else intervalSchedule)
  }
}
