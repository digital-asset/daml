// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.store.db.DbPruningSchedulerStore
import com.digitalasset.canton.store.memory.InMemoryPruningSchedulerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.MemberCode
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Stores for the pruning scheduler parameters such as the cron schedule
  * and pruning retention period
  */
trait PruningSchedulerStore extends AutoCloseable {

  implicit def ec: ExecutionContext

  /** Inserts or updates the pruning scheduler's cron with associated maximum duration and retention */
  def setSchedule(schedule: PruningSchedule)(implicit
      tc: TraceContext
  ): Future[Unit]

  /** Clears the pruning scheduler's cron schedule deactivating background pruning */
  def clearSchedule()(implicit tc: TraceContext): Future[Unit]

  /** Queries the pruning scheduler's schedule and retention */
  def getSchedule()(implicit tc: TraceContext): Future[Option[PruningSchedule]]

  /** Updates the cron */
  def updateCron(cron: Cron)(implicit tc: TraceContext): EitherT[Future, String, Unit]

  /** Updates the maximum duration */
  def updateMaxDuration(maxDuration: PositiveSeconds)(implicit
      tc: TraceContext
  ): EitherT[Future, String, Unit]

  /** Updates the pruning retention */
  def updateRetention(retention: PositiveSeconds)(implicit
      tc: TraceContext
  ): EitherT[Future, String, Unit]
}

object PruningSchedulerStore {
  def create(
      nodeCode: MemberCode,
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): PruningSchedulerStore = {
    storage match {
      case _: MemoryStorage =>
        new InMemoryPruningSchedulerStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbPruningSchedulerStore(nodeCode.threeLetterId, dbStorage, timeouts, loggerFactory)
    }
  }

}
