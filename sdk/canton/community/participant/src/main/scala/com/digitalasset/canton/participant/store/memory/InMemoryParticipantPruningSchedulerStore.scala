// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.scheduler.ParticipantPruningSchedule
import com.digitalasset.canton.participant.store.ParticipantPruningSchedulerStore
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

final class InMemoryParticipantPruningSchedulerStore(
    val loggerFactory: NamedLoggerFactory
)(implicit
    val ec: ExecutionContext
) extends ParticipantPruningSchedulerStore
    with NamedLogging {

  private val schedule: AtomicReference[Option[ParticipantPruningSchedule]] =
    new AtomicReference[Option[ParticipantPruningSchedule]](None)

  override def setParticipantSchedule(scheduleToSet: ParticipantPruningSchedule)(implicit
      tc: TraceContext
  ): Future[Unit] =
    Future.successful(schedule.set(Some(scheduleToSet)))

  override def clearSchedule()(implicit tc: TraceContext): Future[Unit] =
    Future.successful(schedule.set(None))

  override def getParticipantSchedule()(implicit
      tc: TraceContext
  ): Future[Option[ParticipantPruningSchedule]] =
    Future.successful(schedule.get())

  override def updateCron(cron: Cron)(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    update("cron", _.focus(_.cron).replace(cron))

  override def updateMaxDuration(
      maxDuration: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    update("max_duration", _.focus(_.maxDuration).replace(maxDuration))

  override def updateRetention(
      retention: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[Future, String, Unit] =
    update("retention", _.focus(_.retention).replace(retention))

  private def update(
      field: String,
      f: PruningSchedule => PruningSchedule,
  ): EitherT[Future, String, Unit] =
    EitherT.fromEither[Future] {
      schedule
        .updateAndGet(
          _.map(participantSchedule =>
            participantSchedule.copy(schedule = f(participantSchedule.schedule))
          )
        )
        .toRight(
          s"Attempt to update ${field} of a schedule that has not been previously configured. Use set_schedule instead."
        )
        .map(_ => ())
    }

  override def close(): Unit = ()
}
