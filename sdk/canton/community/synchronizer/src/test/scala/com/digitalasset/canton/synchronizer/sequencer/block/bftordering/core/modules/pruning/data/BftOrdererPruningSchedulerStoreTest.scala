// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data

import com.digitalasset.canton.FailOnShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.store.PruningSchedulerStoreTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.BftOrdererPruningSchedule
import org.scalatest.wordspec.AsyncWordSpec

trait BftOrdererPruningSchedulerStoreTest extends PruningSchedulerStoreTest with FailOnShutdown {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  private val bftOrdererSchedule1 = BftOrdererPruningSchedule(schedule1, minBlocksToKeep = 60)
  private val bftOrdererSchedule2 = BftOrdererPruningSchedule(schedule2, minBlocksToKeep = 50)

  protected def bftOrdererPruningSchedulerStore(
      mk: () => BftOrdererPruningSchedulerStore[PekkoEnv]
  ): Unit = {
    "be able to set, clear and get bft orderer schedules" in {
      val store = mk()
      for {
        emptySchedule <- change(store, _.clearSchedule())
        schedule1Queried1 <- changeAndGetBftOrdererSchedule(
          store,
          _.setBftOrdererSchedule(bftOrdererSchedule1),
        )
        emptySchedule2 <- change(store, _.clearSchedule())
        emptySchedule3 <- change(store, _.clearSchedule())
        schedule1Queried2 <- changeAndGetBftOrdererSchedule(
          store,
          _.setBftOrdererSchedule(bftOrdererSchedule1),
        )
        schedule2Queried <- changeAndGetBftOrdererSchedule(
          store,
          _.setBftOrdererSchedule(bftOrdererSchedule2),
        )
        schedule2Plain <- store.getSchedule()
        _ <- change(store, _.setSchedule(bftOrdererSchedule1.schedule))
        schedule1RegularPruning <- store.getBftOrdererSchedule().futureUnlessShutdown()
      } yield {
        emptySchedule shouldBe None
        schedule1Queried1 shouldBe Some(bftOrdererSchedule1)
        emptySchedule2 shouldBe None
        emptySchedule3 shouldBe None
        schedule1Queried2 shouldBe Some(bftOrdererSchedule1)
        schedule2Queried shouldBe Some(bftOrdererSchedule2)
        schedule2Plain shouldBe Some(schedule2)
        // Setting the schedule, using the "plain" set_schedule method clears
        // the min_blocks_to_keep parameter to default value.
        schedule1RegularPruning shouldBe Some(
          bftOrdererSchedule1.copy(minBlocksToKeep =
            BftOrdererPruningSchedule.DefaultMinNumberOfBlocksToKeep
          )
        )
      }
    }

    "be able to configure minBlocksToKeep individually" in {
      val store = mk()
      for {
        schedule1Queried1 <- changeAndGetBftOrdererSchedule(
          store,
          _.setBftOrdererSchedule(bftOrdererSchedule1),
        )
        _ <- store.updateMinBlocksToKeep(25).value
        schedule1Queried2 <- store.getBftOrdererSchedule().futureUnlessShutdown()
      } yield schedule1Queried2 shouldBe schedule1Queried1.map(
        _.copy(minBlocksToKeep = 25)
      )
    }
  }

  private def changeAndGetBftOrdererSchedule(
      store: BftOrdererPruningSchedulerStore[PekkoEnv],
      modify: BftOrdererPruningSchedulerStore[PekkoEnv] => PekkoFutureUnlessShutdown[Unit],
  ): FutureUnlessShutdown[Option[BftOrdererPruningSchedule]] = for {
    _ <- modify(store).futureUnlessShutdown()
    bftOrdererSchedule <- store.getBftOrdererSchedule().futureUnlessShutdown()
  } yield bftOrdererSchedule
}
