// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.scheduler.ParticipantPruningSchedule
import com.digitalasset.canton.participant.store.db.DbParticipantPruningSchedulerStore
import com.digitalasset.canton.participant.store.memory.InMemoryParticipantPruningSchedulerStore
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.scheduler.PruningSchedule
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Store for the participant pruning scheduler parameters such as the cron schedule,
  * pruning retention period, and whether to only prune "internal" canton stores.
  */
trait ParticipantPruningSchedulerStore extends PruningSchedulerStore {

  /** Inserts or updates the pruning scheduler along with participant-specific settings */
  def setParticipantSchedule(schedule: ParticipantPruningSchedule)(implicit
      tc: TraceContext
  ): Future[Unit]

  /** Queries the pruning scheduler's schedule including pruning mode */
  def getParticipantSchedule()(implicit
      tc: TraceContext
  ): Future[Option[ParticipantPruningSchedule]]

  override def setSchedule(schedule: PruningSchedule)(implicit tc: TraceContext): Future[Unit] =
    setParticipantSchedule(ParticipantPruningSchedule.fromPruningSchedule(schedule))

  override def getSchedule()(implicit tc: TraceContext): Future[Option[PruningSchedule]] =
    getParticipantSchedule().map(_.map(_.schedule))
}

object ParticipantPruningSchedulerStore {
  def create(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ParticipantPruningSchedulerStore = {
    storage match {
      case _: MemoryStorage =>
        new InMemoryParticipantPruningSchedulerStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbParticipantPruningSchedulerStore(dbStorage, timeouts, loggerFactory)
    }
  }

}
