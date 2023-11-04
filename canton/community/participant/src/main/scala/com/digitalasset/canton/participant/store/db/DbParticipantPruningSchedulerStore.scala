// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String1
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.scheduler.ParticipantPruningSchedule
import com.digitalasset.canton.participant.store.ParticipantPruningSchedulerStore
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final class DbParticipantPruningSchedulerStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ParticipantPruningSchedulerStore
    with DbStore
    with NamedLogging {
  import storage.api.*

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("pruning-scheduler-store")

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue: String1 = String1.fromChar('X')

  override def setParticipantSchedule(participantSchedule: ParticipantPruningSchedule)(implicit
      tc: TraceContext
  ): Future[Unit] = {
    val schedule = participantSchedule.schedule
    processingTime
      .event {
        storage.update_(
          storage.profile match {
            case _: Profile.Postgres =>
              sqlu"""insert into participant_pruning_schedules (cron, max_duration, retention, prune_internally_only)
                     values (${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention}, ${participantSchedule.pruneInternallyOnly})
                     on conflict (lock) do
                       update set cron = ${schedule.cron}, max_duration = ${schedule.maxDuration}, retention = ${schedule.retention},
                                  prune_internally_only = ${participantSchedule.pruneInternallyOnly}
                  """
            case _: Profile.H2 =>
              sqlu"""merge into participant_pruning_schedules (lock, cron, max_duration, retention, prune_internally_only)
                     values (${singleRowLockValue}, ${schedule.cron}, ${schedule.maxDuration}, ${schedule.retention}, ${participantSchedule.pruneInternallyOnly})
                  """
            case _: Profile.Oracle =>
              sqlu"""merge into participant_pruning_schedules pps
                       using (
                         select ${schedule.cron} cron,
                                ${schedule.maxDuration} max_duration,
                                ${schedule.retention} retention,
                                ${participantSchedule.pruneInternallyOnly} prune_internally_only
                         from dual
                       ) excluded
                     on (pps."LOCK" = 'X')
                     when matched then
                       update set pps.cron = excluded.cron, max_duration = excluded.max_duration,
                                  retention = excluded.retention, prune_internally_only = excluded.prune_internally_only
                     when not matched then
                       insert (cron, max_duration, retention, prune_internally_only)
                       values (excluded.cron, excluded.max_duration, excluded.retention, excluded.prune_internally_only)
                  """
          },
          functionFullName,
        )
      }
  }

  override def clearSchedule()(implicit tc: TraceContext): Future[Unit] = processingTime
    .event {
      storage.update_(
        sqlu"""delete from participant_pruning_schedules""",
        functionFullName,
      )
    }

  override def getParticipantSchedule()(implicit
      tc: TraceContext
  ): Future[Option[ParticipantPruningSchedule]] = processingTime.event {
    storage
      .query(
        sql"""select cron, max_duration, retention, prune_internally_only from participant_pruning_schedules"""
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
  }

  override def updateCron(cron: Cron)(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    EitherT(
      processingTime
        .event {
          storage
            .update(
              sqlu"""update participant_pruning_schedules set cron = $cron""",
              functionFullName,
            )
            .map(errorOnUpdateOfMissingSchedule("cron"))
        }
    )

  override def updateMaxDuration(
      maxDuration: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    EitherT(
      processingTime
        .event {
          storage
            .update(
              sqlu"""update participant_pruning_schedules set max_duration = $maxDuration""",
              functionFullName,
            )
            .map(errorOnUpdateOfMissingSchedule("max_duration"))
        }
    )

  override def updateRetention(
      retention: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    EitherT(
      processingTime
        .event {
          storage
            .update(
              sqlu"""update participant_pruning_schedules set retention = $retention""",
              functionFullName,
            )
            .map(errorOnUpdateOfMissingSchedule("retention"))
        }
    )

  private def errorOnUpdateOfMissingSchedule(
      field: String
  )(rowCount: Int): Either[String, Unit] =
    Either.cond(
      rowCount > 0,
      (),
      s"Attempt to update ${field} of a schedule that has not been previously configured. Use set_schedule or set_participant_schedule instead.",
    )
}
