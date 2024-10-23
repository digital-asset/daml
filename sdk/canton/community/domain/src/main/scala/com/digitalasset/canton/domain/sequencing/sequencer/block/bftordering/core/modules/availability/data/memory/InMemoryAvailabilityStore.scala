// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.data.memory

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.util.Try

abstract class GenericInMemoryAvailabilityStore[E <: Env[E]](
    allKnownBatchesById: mutable.Map[BatchId, OrderingRequestBatch] = mutable.Map.empty
) extends AvailabilityStore[E] {

  def createFuture[A](action: String)(x: () => Try[A]): E#FutureUnlessShutdownT[A]

  override def addBatch(batchId: BatchId, batch: OrderingRequestBatch)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit] =
    createFuture(addBatchActionName(batchId)) { () =>
      Try {
        val _ = allKnownBatchesById.updateWith(batchId) {
          case Some(value) => Some(value)
          case None => Some(batch)
        }
      }
    }

  override def fetchBatches(
      batches: Seq[BatchId]
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[AvailabilityStore.FetchBatchesResult] =
    createFuture(fetchBatchesActionName) { () =>
      Try {
        val keys = allKnownBatchesById.keySet
        val missing = batches.filterNot(batchId => keys.contains(batchId))
        if (missing.isEmpty) {
          AvailabilityStore.AllBatches(batches.map(id => id -> allKnownBatchesById(id)))
        } else {
          AvailabilityStore.MissingBatches(missing.toSet)
        }
      }
    }

  override def gc(staleBatchIds: Seq[BatchId])(implicit
      traceContext: TraceContext
  ): Unit =
    staleBatchIds.foreach { staleBatchId =>
      val _ = allKnownBatchesById.remove(staleBatchId)
    }

  @VisibleForTesting
  def isEmpty: Boolean = allKnownBatchesById.isEmpty

  @VisibleForTesting
  def size: Int = allKnownBatchesById.size

  @VisibleForTesting
  def keys: Iterable[BatchId] = allKnownBatchesById.keys
}

final class InMemoryAvailabilityStore(
    allKnownBatchesById: mutable.Map[BatchId, OrderingRequestBatch] = mutable.Map.empty
) extends GenericInMemoryAvailabilityStore[PekkoEnv](allKnownBatchesById) {
  override def createFuture[A](action: String)(x: () => Try[A]): PekkoFutureUnlessShutdown[A] =
    PekkoFutureUnlessShutdown(action, FutureUnlessShutdown.fromTry(x()))
}
