// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.data.EitherT
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.{Clock, PositiveSeconds}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class JobTestScheduler(
    job: IndividualSchedule => Future[
      JobScheduler.ScheduledRunResult
    ],
    clock: Clock,
    processingTimeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextIdlenessExecutorService)
    extends JobScheduler(
      "test-scheduler",
      processingTimeouts,
      loggerFactory,
    ) {
  private val schedule = new AtomicReference[Option[PruningCronSchedule]](None)

  override def schedulerJob(schedule: IndividualSchedule)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[JobScheduler.ScheduledRunResult] =
    FutureUnlessShutdown.outcomeF(job(schedule))

  override def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] = Future.successful(schedule.get)

  override def close(): Unit = stop()(TraceContext.todo)

  def setSchedule(newSchedule: PruningCronSchedule)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    updateAndRestart(schedule.set(Some(newSchedule)))

  override def clearSchedule()(implicit traceContext: TraceContext): Future[Unit] =
    updateAndRestart(schedule.set(None))

  override def updateCron(cron: Cron)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    updateAndRestartET(
      schedule.updateAndGet(
        _.map(old => new PruningCronSchedule(cron, old.maxDuration, old.retention, clock, logger))
      )
    )

  override def updateMaxDuration(maxDuration: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = updateAndRestartET(
    schedule.updateAndGet(
      _.map(old => new PruningCronSchedule(old.cron, maxDuration, old.retention, clock, logger))
    )
  )

  private def updateAndRestart[T](
      update: => T
  )(implicit traceContext: TraceContext): Future[Unit] = {
    update
    reactivateSchedulerIfActive()
  }

  private def updateAndRestartET[T](
      update: => T
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    update
    reactivateSchedulerIfActiveET()
  }
}
