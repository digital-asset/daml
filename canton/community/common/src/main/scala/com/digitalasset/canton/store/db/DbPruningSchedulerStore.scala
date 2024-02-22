// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String3
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final class DbPruningSchedulerStore(
    nodeCode: String3, // short 3-character node code "MED", or "SEQ" as sequencer and mediator can share a db
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends PruningSchedulerStore
    with DbStore
    with NamedLogging {
  import storage.api.*

  override def setSchedule(schedule: PruningSchedule)(implicit
      tc: TraceContext
  ): Future[Unit] = {
    storage.update_(
      storage.profile match {
        case _: Profile.Postgres =>
          sqlu"""insert into pruning_schedules (node_type, cron, max_duration, retention)
                     values ($nodeCode, ${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention})
                     on conflict (node_type) do
                       update set cron = ${schedule.cron}, max_duration = ${schedule.maxDuration}, retention = ${schedule.retention}
                  """
        case _: Profile.Oracle | _: Profile.H2 =>
          sqlu"""merge into pruning_schedules using dual
                     on (node_type = $nodeCode)
                     when matched then
                       update set cron = ${schedule.cron}, max_duration = ${schedule.maxDuration}, retention = ${schedule.retention}
                     when not matched then
                       insert (node_type, cron, max_duration, retention)
                       values ($nodeCode, ${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention})
                  """
      },
      functionFullName,
    )
  }

  override def clearSchedule()(implicit tc: TraceContext): Future[Unit] =
    storage.update_(
      sqlu"""delete from pruning_schedules where node_type = $nodeCode""",
      functionFullName,
    )

  override def getSchedule()(implicit
      tc: TraceContext
  ): Future[Option[PruningSchedule]] = {
    storage
      .query(
        sql"""select cron, max_duration, retention
             from pruning_schedules where node_type = $nodeCode"""
          .as[(Cron, PositiveSeconds, PositiveSeconds)]
          .headOption,
        functionFullName,
      )
      .map(_.map { case (cron, maxDuration, retention) =>
        // Don't expect to find non-parsable cron expressions or non-positive durations in the db as they are
        // checked upon setting.
        PruningSchedule(
          cron,
          maxDuration,
          retention,
        )
      })
  }

  override def updateCron(cron: Cron)(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update pruning_schedules set cron = $cron where node_type = $nodeCode""",
          functionFullName,
        )
        .map(errorOnUpdateOfMissingSchedule("cron"))
    }

  override def updateMaxDuration(
      maxDuration: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update pruning_schedules set max_duration = $maxDuration where node_type = $nodeCode""",
          functionFullName,
        )
        .map(errorOnUpdateOfMissingSchedule("max_duration"))
    }

  override def updateRetention(
      retention: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update pruning_schedules set retention = $retention where node_type = $nodeCode""",
          functionFullName,
        )
        .map(errorOnUpdateOfMissingSchedule("retention"))
    }

  private def errorOnUpdateOfMissingSchedule(
      field: String
  )(rowCount: Int): Either[String, Unit] =
    Either.cond(
      rowCount > 0,
      (),
      s"Attempt to update ${field} of a schedule that has not been previously configured. Use set_schedule instead.",
    )
}
