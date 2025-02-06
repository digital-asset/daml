// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

final class InMemoryPruningSchedulerStore(
    val loggerFactory: NamedLoggerFactory
)(implicit
    val ec: ExecutionContext
) extends PruningSchedulerStore
    with NamedLogging {

  private val schedule: AtomicReference[Option[PruningSchedule]] =
    new AtomicReference[Option[PruningSchedule]](None)

  override def setSchedule(scheduleToSet: PruningSchedule)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(schedule.set(Some(scheduleToSet)))

  override def clearSchedule()(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(schedule.set(None))

  override def getSchedule()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Option[PruningSchedule]] =
    FutureUnlessShutdown.pure(schedule.get())

  override def updateCron(cron: Cron)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    update("cron", _.focus(_.cron).replace(cron))

  override def updateMaxDuration(
      maxDuration: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    update("max_duration", _.focus(_.maxDuration).replace(maxDuration))

  override def updateRetention(
      retention: PositiveSeconds
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    update("retention", _.focus(_.retention).replace(retention))

  private def update(
      field: String,
      f: PruningSchedule => PruningSchedule,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.fromEither[FutureUnlessShutdown] {
      schedule
        .updateAndGet(_.map(f))
        .toRight(
          s"Attempt to update $field of a schedule that has not been previously configured. Use set_schedule instead."
        )
        .map(_ => ())
    }

  override def close(): Unit = ()
}
