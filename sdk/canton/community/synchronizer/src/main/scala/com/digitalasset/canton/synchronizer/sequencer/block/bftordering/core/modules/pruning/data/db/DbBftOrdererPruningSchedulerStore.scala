// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String1
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.BftOrdererPruningSchedule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class DbBftOrdererPruningSchedulerStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends BftOrdererPruningSchedulerStore[PekkoEnv]
    with DbStore {
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue: String1 = String1.fromChar('X')

  private val profile = storage.profile

  override def setBftOrdererSchedule(bftOrdererSchedule: BftOrdererPruningSchedule)(implicit
      tc: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = setBftOrdererScheduleActionName(bftOrdererSchedule)
    val schedule = bftOrdererSchedule.schedule
    val query = profile match {
      case _: Postgres =>
        sqlu"""insert into ord_pruning_schedules (cron, max_duration, retention, min_blocks_to_keep)
                     values (${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention}, ${bftOrdererSchedule.minBlocksToKeep})
                     on conflict (lock) do
                       update set cron = ${schedule.cron}, max_duration = ${schedule.maxDuration}, retention = ${schedule.retention},
                                  min_blocks_to_keep = ${bftOrdererSchedule.minBlocksToKeep}
                  """
      case _: H2 =>
        sqlu"""merge into ord_pruning_schedules (lock, cron, max_duration, retention, min_blocks_to_keep)
                     values ($singleRowLockValue, ${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention}, ${bftOrdererSchedule.minBlocksToKeep})
                  """
    }
    val future = () => storage.update_(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getBftOrdererSchedule()(implicit
      tc: TraceContext
  ): PekkoFutureUnlessShutdown[Option[BftOrdererPruningSchedule]] = {
    val name = getBftOrdererScheduleName
    val query =
      sql"""select cron, max_duration, retention, min_blocks_to_keep from ord_pruning_schedules"""
        .as[(Cron, PositiveSeconds, PositiveSeconds, Int)]
        .headOption
    val future = () =>
      storage
        .query(query, functionFullName)
        .map(_.map { case (cron, maxDuration, retention, minBlocksToKeep) =>
          BftOrdererPruningSchedule(
            PruningSchedule(
              cron,
              maxDuration,
              retention,
            ),
            minBlocksToKeep,
          )
        })
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getSchedule()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Option[PruningSchedule]] =
    getBftOrdererSchedule().futureUnlessShutdown().map(_.map(_.schedule))

  override def setSchedule(schedule: PruningSchedule)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] =
    setBftOrdererSchedule(BftOrdererPruningSchedule.fromPruningSchedule(schedule))
      .futureUnlessShutdown()

  override def clearSchedule()(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""delete from ord_pruning_schedules""",
      functionFullName,
    )

  override def updateCron(cron: Cron)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT {
    storage
      .update(
        sqlu"""update ord_pruning_schedules set cron = $cron""",
        functionFullName,
      )
      .map(errorOnUpdateOfMissingSchedule("cron"))
  }

  override def updateMaxDuration(maxDuration: PositiveSeconds)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT {
    storage
      .update(
        sqlu"""update ord_pruning_schedules set max_duration = $maxDuration""",
        functionFullName,
      )
      .map(errorOnUpdateOfMissingSchedule("max_duration"))
  }

  override def updateRetention(retention: PositiveSeconds)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update ord_pruning_schedules set retention = $retention""",
          functionFullName,
        )
        .map(errorOnUpdateOfMissingSchedule("retention"))
    }

  override def updateMinBlocksToKeep(minBlocksToKeep: Int)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT {
    storage
      .update(
        sqlu"""update ord_pruning_schedules set min_blocks_to_keep = $minBlocksToKeep""",
        functionFullName,
      )
      .map(errorOnUpdateOfMissingSchedule("min_blocks_to_keep"))
  }

  private def errorOnUpdateOfMissingSchedule(
      field: String
  )(rowCount: Int): Either[String, Unit] =
    Either.cond(
      rowCount > 0,
      (),
      s"Attempt to update $field of a schedule that has not been previously configured. Use set_schedule or set_bft_orderer_schedule instead.",
    )
}
