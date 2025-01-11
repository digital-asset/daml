// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait HasPruningSchedulerStore extends PruningScheduler with FlagCloseable {
  this: JobScheduler =>

  protected def pruningSchedulerStore: PruningSchedulerStore

  override def clearSchedule()(implicit traceContext: TraceContext): Future[Unit] =
    updateScheduleAndReactivateIfActive(
      pruningSchedulerStore
        .clearSchedule()
        .failOnShutdownToAbortException("HasPruningSchedulerStore.clearSchedule")
    )

  override def updateCron(
      cron: Cron
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    for {
      _updated <- pruningSchedulerStore
        .updateCron(cron)
        .failOnShutdownToAbortException("HasPruningSchedulerStore.updateCron")
      _reactivated <- reactivateSchedulerIfActiveET()
    } yield ()

  override def updateMaxDuration(maxDuration: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    for {
      _updated <- pruningSchedulerStore
        .updateMaxDuration(maxDuration)
        .failOnShutdownToAbortException("HasPruningSchedulerStore.updateMaxDuration")
      _reactivated <- reactivateSchedulerIfActiveET()
    } yield ()

  override def updateRetention(retention: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = for {
    _updated <- pruningSchedulerStore
      .updateRetention(retention)
      .failOnShutdownToAbortException("HasPruningSchedulerStore.updateRetention")
    _reactivated <- reactivateSchedulerIfActiveET()
  } yield ()

  override def setSchedule(schedule: PruningSchedule)(implicit
      traceContext: TraceContext
  ): Future[Unit] = updateScheduleAndReactivateIfActive(
    pruningSchedulerStore
      .setSchedule(schedule)
      .failOnShutdownToAbortException("HasPruningSchedulerStore.setSchedule")
  )

  override def getSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[PruningSchedule]] = pruningSchedulerStore
    .getSchedule()
    .failOnShutdownToAbortException("HasPruningSchedulerStore.getSchedule")

  override def onClosed(): Unit = pruningSchedulerStore.close()

}
