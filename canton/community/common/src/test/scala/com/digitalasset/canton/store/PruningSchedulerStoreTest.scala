// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.time.PositiveSeconds
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PruningSchedulerStoreTest {
  this: AsyncWordSpec & BaseTest =>

  protected def pruningSchedulerStore(mk: () => PruningSchedulerStore): Unit = {

    val schedule1 = PruningSchedule(
      Cron.tryCreate("* /10 * * * ? *"),
      PositiveSeconds.tryOfSeconds(1),
      PositiveSeconds.tryOfSeconds(30),
    )
    val schedule2 = PruningSchedule(
      Cron.tryCreate("* * 7,19 * * ? *"),
      PositiveSeconds.tryOfHours(8),
      PositiveSeconds.tryOfDays(14),
    )

    assert(schedule1.cron != schedule2.cron)
    assert(schedule1.maxDuration != schedule2.maxDuration)
    assert(schedule1.retention != schedule2.retention)

    "be able to set, clear and get schedules" in {
      val store = mk()

      for {
        emptySchedule <- change(store, _.clearSchedule())
        schedule1Queried1 <- change(store, _.setSchedule(schedule1))
        emptySchedule2 <- change(store, _.clearSchedule())
        emptySchedule3 <- change(store, _.clearSchedule())
        schedule1Queried2 <- change(store, _.setSchedule(schedule1))
        schedule2Queried <- change(store, _.setSchedule(schedule2))
      } yield {
        emptySchedule shouldBe None
        schedule1Queried1 shouldBe Some(schedule1)
        emptySchedule2 shouldBe None
        emptySchedule3 shouldBe None
        schedule1Queried2 shouldBe Some(schedule1)
        schedule2Queried shouldBe Some(schedule2)
      }
    }

    "be able to update individual fields of schedules" in {
      val store = mk()

      for {
        _ <- store.clearSchedule()
        schedule1Queried1 <- change(store, _.setSchedule(schedule1))
        scheduleChangedCron <- changeET(store, _.updateCron(schedule2.cron), "update cron")
        scheduleChangedMaxDuration <- changeET(
          store,
          _.updateMaxDuration(schedule2.maxDuration),
          "update max_duration",
        )
        scheduleChangedRetention <- changeET(
          store,
          _.updateRetention(schedule2.retention),
          "update retention",
        )
      } yield {
        schedule1Queried1 shouldBe Some(schedule1)
        scheduleChangedCron shouldBe Some(schedule1.copy(cron = schedule2.cron))
        scheduleChangedMaxDuration shouldBe Some(schedule2.copy(retention = schedule1.retention))
        scheduleChangedRetention shouldBe Some(schedule2)
      }
    }

    "refuse to update fields on non-existing schedules" in {
      val store = mk()

      for {
        emptySchedule <- change(store, _.clearSchedule())
        _ <- expectFailureET(
          "cron",
          store.updateCron(schedule2.cron),
          "update cron of non-existing schedule",
        )
        _ <- expectFailureET(
          "max_duration",
          store.updateMaxDuration(schedule2.maxDuration),
          "update max_duration of non-existing schedule",
        )
        _ <- expectFailureET(
          "retention",
          store.updateRetention(schedule2.retention),
          "update retention of non-existing schedule",
        )
        emptySchedule2 <- store.getSchedule()
      } yield {
        emptySchedule shouldBe None
        emptySchedule2 shouldBe None
      }

    }
  }

  protected def change(
      store: PruningSchedulerStore,
      modify: PruningSchedulerStore => Future[Unit],
  ): Future[Option[PruningSchedule]] = for {
    _ <- modify(store)
    schedule <- store.getSchedule()
  } yield schedule

  private def changeET(
      store: PruningSchedulerStore,
      modify: PruningSchedulerStore => EitherT[Future, String, Unit],
      clue: String,
  ): Future[Option[PruningSchedule]] =
    change(store, store => valueOrFail(modify(store))(clue))

  private def expectFailureET(
      field: String,
      change: => EitherT[Future, String, Unit],
      clue: String,
  ): Future[Unit] = for {
    err <- leftOrFail(change)(clue)
    _errorMatchesExpected <-
      if (
        err.equals(
          s"Attempt to update ${field} of a schedule that has not been previously configured. Use set_schedule instead."
        )
      ) Future.unit
      else Future.failed(new RuntimeException(s"Wrong error message: ${err}"))
  } yield ()
}
