// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.db.DbAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.memory.InMemoryAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

import AvailabilityStore.FetchBatchesResult

trait AvailabilityStore[E <: Env[E]] extends AutoCloseable {
  def addBatch(batchId: BatchId, batch: OrderingRequestBatch)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]
  protected def addBatchActionName(batchId: BatchId): String = s"Add batch $batchId"

  def fetchBatches(batches: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[FetchBatchesResult]
  protected val fetchBatchesActionName: String = "Fetch batches"

  def gc(staleBatchIds: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): Unit
}

object AvailabilityStore {
  sealed trait FetchBatchesResult

  final case class MissingBatches(batchIds: Set[BatchId]) extends FetchBatchesResult

  final case class AllBatches(batches: Seq[(BatchId, OrderingRequestBatch)])
      extends FetchBatchesResult

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): AvailabilityStore[PekkoEnv] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryAvailabilityStore()
      case dbStorage: DbStorage =>
        new DbAvailabilityStore(dbStorage, timeouts, loggerFactory)(executionContext)
    }
}
