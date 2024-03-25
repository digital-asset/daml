// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.data.EitherT
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Trait for an individual scheduler
  */
trait Scheduler extends StartStoppable with AutoCloseable {
  def clearSchedule()(implicit traceContext: TraceContext): Future[Unit]

  def updateCron(cron: Cron)(implicit traceContext: TraceContext): EitherT[Future, String, Unit]

  def updateMaxDuration(maxDuration: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]

}

/** Extends the Scheduler with the concept of retention used for the pruning cut-off
  */
trait PruningScheduler extends Scheduler {

  /** Updates the pruning retention, i.e. the age of the newest piece of data to be pruned
    * relative to the node's clock.
    * @param retention retention as a duration
    */
  def updateRetention(retention: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]

  def setSchedule(schedule: PruningSchedule)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def getSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[PruningSchedule]]

}

/** Trait to start and stop individual or multiple schedulers
  */
trait StartStoppable {

  /** Start scheduler(s). */
  def start()(implicit traceContext: TraceContext): Future[Unit]

  /** Stop scheduler(s). */
  def stop()(implicit traceContext: TraceContext): Unit

  /** Convenience method for restart */
  def restart()(implicit traceContext: TraceContext): Future[Unit] = {
    stop()
    start()
  }
}
