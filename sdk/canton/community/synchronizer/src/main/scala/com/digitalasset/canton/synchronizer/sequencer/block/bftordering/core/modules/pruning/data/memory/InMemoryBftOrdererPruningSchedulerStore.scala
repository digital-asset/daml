// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.memory

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.BftOrdererPruningSchedule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

abstract class GenericInMemoryBftOrdererPruningSchedulerStore[E <: Env[E]]
    extends BftOrdererPruningSchedulerStore[E] {
  protected def createFuture[T](action: String)(value: () => Try[T]): E#FutureUnlessShutdownT[T]

  private val schedule: AtomicReference[Option[BftOrdererPruningSchedule]] =
    new AtomicReference[Option[BftOrdererPruningSchedule]](None)

  override def setBftOrdererSchedule(scheduleToSet: BftOrdererPruningSchedule)(implicit
      tc: TraceContext
  ): E#FutureUnlessShutdownT[Unit] =
    createFuture(setBftOrdererScheduleActionName(scheduleToSet))(() =>
      Success(schedule.set(Some(scheduleToSet)))
    )

  override def getSchedule()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Option[PruningSchedule]] =
    FutureUnlessShutdown.pure(schedule.get().map(_.schedule))

  override def setSchedule(scheduleToSet: PruningSchedule)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(
      schedule.set(Some(BftOrdererPruningSchedule.fromPruningSchedule(scheduleToSet)))
    )

  override def getBftOrdererSchedule()(implicit
      tc: TraceContext
  ): E#FutureUnlessShutdownT[Option[BftOrdererPruningSchedule]] =
    createFuture(getBftOrdererScheduleName)(() => Success(schedule.get()))

  override def clearSchedule()(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(schedule.set(None))

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

  def updateMinBlocksToKeep(minBlocksToKeep: Int)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.fromEither[FutureUnlessShutdown] {
      schedule
        .updateAndGet(
          _.map(bftOrdererSchedule => bftOrdererSchedule.copy(minBlocksToKeep = minBlocksToKeep))
        )
        .toRight(
          s"Attempt to update min_blocks_to_keep of a schedule that has not been previously configured. Use set_schedule or set_bft_schedule instead."
        )
        .map(_ => ())
    }

  private def update(
      field: String,
      f: PruningSchedule => PruningSchedule,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT.fromEither[FutureUnlessShutdown] {
      schedule
        .updateAndGet(
          _.map(bftOrdererSchedule =>
            bftOrdererSchedule.copy(schedule = f(bftOrdererSchedule.schedule))
          )
        )
        .toRight(
          s"Attempt to update $field of a schedule that has not been previously configured. Use set_schedule or set_bft_schedule instead."
        )
        .map(_ => ())
    }

}

class InMemoryBftOrdererPruningSchedulerStore(
    override val loggerFactory: NamedLoggerFactory
)(implicit val ec: ExecutionContext)
    extends GenericInMemoryBftOrdererPruningSchedulerStore[PekkoEnv]
    with NamedLogging {

  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): PekkoFutureUnlessShutdown[T] =
    PekkoFutureUnlessShutdown(action, () => FutureUnlessShutdown.fromTry(value()))

  override def close(): Unit = ()
}
