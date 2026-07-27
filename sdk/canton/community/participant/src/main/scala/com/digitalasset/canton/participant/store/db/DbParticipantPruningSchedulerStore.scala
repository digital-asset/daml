// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String1
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.scheduler.ParticipantPruningSchedule
import com.digitalasset.canton.participant.store.ParticipantPruningSchedulerStore
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

final class DbParticipantPruningSchedulerStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ParticipantPruningSchedulerStore
    with DbStore
    with NamedLogging {
  import storage.api.*

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue: String1 = String1.fromChar('X')

  override def setParticipantSchedule(participantSchedule: ParticipantPruningSchedule)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val schedule = participantSchedule.schedule

    storage.update_(
      storage.profile match {
        case _: Profile.Postgres =>
          sqlu"""insert into par_pruning_schedules (cron, max_duration, retention, prune_internally_only)
                     values (${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention}, ${participantSchedule.pruneInternallyOnly})
                     on conflict (lock) do
                       update set cron = ${schedule.cron}, max_duration = ${schedule.maxDuration}, retention = ${schedule.retention},
                                  prune_internally_only = ${participantSchedule.pruneInternallyOnly}
                  """
        case _: Profile.H2 =>
          sqlu"""merge into par_pruning_schedules (lock, cron, max_duration, retention, prune_internally_only)
                     values ($singleRowLockValue, ${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention}, ${participantSchedule.pruneInternallyOnly})
                  """
      },
      functionFullName,
    )
  }

  override def clearSchedule()(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""delete from par_pruning_schedules""",
      functionFullName,
    )

  override def getParticipantSchedule()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Option[ParticipantPruningSchedule]] =
    storage
      .query(
        sql"""select cron, max_duration, retention, prune_internally_only from par_pruning_schedules"""
          .as[(Cron, PositiveSeconds, PositiveSeconds, Boolean)]
          .headOption,
        functionFullName,
      )
      .map(_.map { case (cron, maxDuration, retention, pruneInternallyOnly) =>
        // Don't expect to find non-parsable cron expressions or non-positive durations in the db as they are
        // checked upon setting.
        ParticipantPruningSchedule(
          PruningSchedule(
            cron,
            maxDuration,
            retention,
          ),
          pruneInternallyOnly,
        )
      })

  override def updateCron(
      cron: Cron
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update par_pruning_schedules set cron = $cron""",
          functionFullName,
        )
        .map(errorOnUpdateOfMissingSchedule("cron"))
    }

  override def updateMaxDuration(
      maxDuration: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update par_pruning_schedules set max_duration = $maxDuration""",
          functionFullName,
        )
        .map(errorOnUpdateOfMissingSchedule("max_duration"))
    }

  override def updateRetention(
      retention: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT {
      storage
        .update(
          sqlu"""update par_pruning_schedules set retention = $retention""",
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
      s"Attempt to update $field of a schedule that has not been previously configured. Use set_schedule or set_participant_schedule instead.",
    )
}
