// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.Instance.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.BftOrdererPruningSchedule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.db.DbBftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.memory.InMemoryBftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait BftOrdererPruningSchedulerStore[E <: Env[E]] extends PruningSchedulerStore {

  /** Inserts or updates the pruning scheduler along with bft-orderer-specific settings */
  def setBftOrdererSchedule(schedule: BftOrdererPruningSchedule)(implicit
      tc: TraceContext
  ): E#FutureUnlessShutdownT[Unit]

  protected final def setBftOrdererScheduleActionName(schedule: BftOrdererPruningSchedule): String =
    s"insert bft orderer pruning schedule $schedule"

  def getBftOrdererSchedule()(implicit
      tc: TraceContext
  ): E#FutureUnlessShutdownT[Option[BftOrdererPruningSchedule]]

  def updateMinBlocksToKeep(minBlocksToKeep: Int)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]

  protected final def getBftOrdererScheduleName: String = "get bft orderer pruning schedule"
}

object BftOrdererPruningSchedulerStore {
  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): BftOrdererPruningSchedulerStore[PekkoEnv] =
    storage match {
      case _: MemoryStorage => new InMemoryBftOrdererPruningSchedulerStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbBftOrdererPruningSchedulerStore(dbStorage, timeouts, loggerFactory)
    }
}
