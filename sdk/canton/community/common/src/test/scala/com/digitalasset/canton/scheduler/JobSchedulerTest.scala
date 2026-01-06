// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.time.{PositiveSeconds, WallClock}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, config}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/** Test two things:
  *   1. behavior of scheduler in response to ScheduledRunResult's
  *   1. resilience of scheduler to various job exceptions
  */
class JobSchedulerTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with IgnoresTransientSchedulerErrors {

  private val clock = new WallClock(timeouts, loggerFactory)

  private val everyThreeSeconds = new PruningCronSchedule(
    Cron.create("*/3 * * * * ? *").getOrElse(fail("bad cron")),
    PositiveSeconds.tryOfSeconds(1L),
    PositiveSeconds.tryOfSeconds(8L), // retention not used in non-pruning test
    clock,
    logger,
  )

  def runScheduler[T](schedule: PruningCronSchedule, duration: config.NonNegativeDuration)(
      job: IndividualSchedule => Future[
        (JobScheduler.ScheduledRunResult, T)
      ]
  ): Seq[(JobScheduler.ScheduledRunResult, T)] = {
    val runs = ListBuffer.empty[(JobScheduler.ScheduledRunResult, T)]
    val scheduler = new JobTestScheduler(
      job(_).map { case tuple @ (result, _) =>
        runs.append(tuple)
        result
      },
      clock,
      timeouts,
      loggerFactory,
    )

    timeouts.unbounded
      .await[List[(JobScheduler.ScheduledRunResult, T)]]("letting scheduler run")((for {
        _configured <- scheduler.setSchedule(schedule)
        _started <- scheduler.start()
        _sleptLettingSchedulerRun = Threading.sleep(duration.duration.toMillis)
      } yield runs.result()).transform { t =>
        scheduler.stop()
        t
      })
  }

  private def runTask(result: JobScheduler.ScheduledRunResult, waitInMillis: Int) = {
    val startedAt = clock.now
    Threading.sleep(waitInMillis.toLong)
    logger.info(s"Returning $result started at $startedAt")
    Future.successful(result -> startedAt)
  }

  "A scheduler" when {

    "only schedule done tasks once per window" in {
      val events = runScheduler(
        everyThreeSeconds,
        config.NonNegativeDuration.ofSeconds(10L),
      )(_ => runTask(JobScheduler.Done, 200))

      // 3 or 4 runs spaced out 3 seconds expected in a 10 second period
      events.size should be >= 3
      events.size should be <= 4
    }

    "repeatedly schedule tasks when more work to perform" in {
      val events = runScheduler(
        everyThreeSeconds,
        config.NonNegativeDuration.ofSeconds(10L),
      )(_ => runTask(JobScheduler.MoreWorkToPerform, 100))

      // Should see at least 12 runs somewhat conservatively to minimize flakiness
      events.size should be >= 12
    }

    "repeatedly schedule tasks when errors" in {
      val events = ignoreTransientSchedulerErrors("JobSchedulerTest") {
        runScheduler(
          everyThreeSeconds,
          config.NonNegativeDuration.ofSeconds(10L),
        )(_ => runTask(JobScheduler.Error("caught by scheduler job task"), 100))
      }

      // Should see at least 3 runs showing that thrown exceptions don't interrupt the scheduler
      events.size should be >= 3

      // The backoff after an error is expected to push out the next execution to the subsequent window
      events.size should be <= 4
    }

    "reschedule in spite of non-fatal exceptions" in {
      var throwOrNot = false
      val events = ignoreTransientSchedulerErrors("JobSchedulerTest") {
        runScheduler(
          everyThreeSeconds,
          config.NonNegativeDuration.ofSeconds(10L),
        )(_ =>
          Future {
            val result = JobScheduler.MoreWorkToPerform
            val startedAt = clock.now
            Threading.sleep(100)
            if (throwOrNot) {
              throwOrNot = false
              throw new IllegalStateException("Thrown non-fatal exception")
            } else {
              throwOrNot = true
              logger.info(s"Returning $result started at $startedAt")
            }
            result -> startedAt
          }
        )
      }

      // Should see at least 3 runs showing that thrown exceptions don't interrupt the scheduler
      events.size should be >= 3

      // The backoff after an exception is expected to push out the next execution to the subsequent window
      events.size should be <= 4
    }

    "tolerate extremely long running tasks" in {
      var sleep = 1000L * 15L
      val events = runScheduler(
        everyThreeSeconds,
        config.NonNegativeDuration.ofSeconds(20L),
      )(_ =>
        Future {
          val result = JobScheduler.Done
          val startedAt = clock.now
          Threading.sleep(sleep)
          sleep = 200L
          logger.info(s"Returning $result started at $startedAt")
          result -> startedAt
        }
      )

      events.size should be >= 1
    }

  }
}
