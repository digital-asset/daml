// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.scheduler.ParticipantPruningSchedule
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.store.PruningSchedulerStoreTest
import com.digitalasset.canton.time.PositiveSeconds
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait ParticipantPruningSchedulerStoreTest extends PruningSchedulerStoreTest {
  this: AsyncWordSpec & BaseTest =>
  protected def participantPruningSchedulerStore(
      mk: () => ParticipantPruningSchedulerStore
  ): Unit = {
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

    "be able to set, clear and get participant schedules" in {
      val store = mk()
      val participantSchedule1 = ParticipantPruningSchedule(schedule1, pruneInternallyOnly = true)
      val participantSchedule2 = ParticipantPruningSchedule(schedule2, pruneInternallyOnly = false)

      for {
        emptySchedule <- change(store, _.clearSchedule())
        schedule1Queried1 <- changeAndGetParticipantSchedule(
          store,
          _.setParticipantSchedule(participantSchedule1),
        )
        emptySchedule2 <- change(store, _.clearSchedule())
        emptySchedule3 <- change(store, _.clearSchedule())
        schedule1Queried2 <- changeAndGetParticipantSchedule(
          store,
          _.setParticipantSchedule(participantSchedule1),
        )
        schedule2Queried <- changeAndGetParticipantSchedule(
          store,
          _.setParticipantSchedule(participantSchedule2),
        )
        schedule2Plain <- store.getSchedule()
        _ <- change(store, _.setSchedule(participantSchedule1.schedule))
        schedule1RegularPruning <- store.getParticipantSchedule()
      } yield {
        emptySchedule shouldBe None
        schedule1Queried1 shouldBe Some(participantSchedule1)
        emptySchedule2 shouldBe None
        emptySchedule3 shouldBe None
        schedule1Queried2 shouldBe Some(participantSchedule1)
        schedule2Queried shouldBe Some(participantSchedule2)
        schedule2Plain shouldBe Some(schedule2)
        // Setting the schedule, using the "plain" set_schedule method clears
        // the prune_internally_only flag as the default is "regular" pruning.
        schedule1RegularPruning shouldBe Some(
          participantSchedule1.copy(pruneInternallyOnly = false)
        )
      }
    }
  }

  private def changeAndGetParticipantSchedule(
      store: ParticipantPruningSchedulerStore,
      modify: ParticipantPruningSchedulerStore => Future[Unit],
  ): Future[Option[ParticipantPruningSchedule]] = for {
    _ <- modify(store)
    participantSchedule <- store.getParticipantSchedule()
  } yield participantSchedule

}
