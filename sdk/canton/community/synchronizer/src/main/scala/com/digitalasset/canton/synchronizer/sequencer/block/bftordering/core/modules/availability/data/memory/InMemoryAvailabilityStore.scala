// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.util.{Success, Try}

abstract class GenericInMemoryAvailabilityStore[E <: Env[E]](
    allKnownBatchesById: TrieMap[BatchId, OrderingRequestBatch] = TrieMap.empty
) extends AvailabilityStore[E] {

  def createFuture[A](action: String)(x: () => Try[A]): E#FutureUnlessShutdownT[A]

  override def addBatch(batchId: BatchId, batch: OrderingRequestBatch)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit] =
    createFuture(addBatchActionName(batchId)) { () =>
      Try {
        allKnownBatchesById.putIfAbsent(batchId, batch).discard
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
  ): E#FutureUnlessShutdownT[Unit] =
    createFuture(gcName) { () =>
      staleBatchIds.foreach { staleBatchId =>
        val _ = allKnownBatchesById.remove(staleBatchId)
      }
      Success(())
    }

  @VisibleForTesting
  def isEmpty: Boolean = allKnownBatchesById.isEmpty

  @VisibleForTesting
  def size: Int = allKnownBatchesById.size

  @VisibleForTesting
  def keys: Iterable[BatchId] = allKnownBatchesById.keys

  override def loadNumberOfRecords(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[AvailabilityStore.NumberOfRecords] =
    createFuture(loadNumberOfRecordsName) { () =>
      Success(AvailabilityStore.NumberOfRecords(allKnownBatchesById.size.toLong))
    }

  override def prune(epochNumberExclusive: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[AvailabilityStore.NumberOfRecords] =
    createFuture(pruneName(epochNumberExclusive)) { () =>
      val batchesToDelete = allKnownBatchesById.filter(_._2.epochNumber < epochNumberExclusive).keys
      batchesToDelete.foreach(allKnownBatchesById.remove(_).discard)
      Success(AvailabilityStore.NumberOfRecords(batchesToDelete.size.toLong))
    }

}

final class InMemoryAvailabilityStore(
    allKnownBatchesById: TrieMap[BatchId, OrderingRequestBatch] = TrieMap.empty
) extends GenericInMemoryAvailabilityStore[PekkoEnv](allKnownBatchesById) {
  override def createFuture[A](action: String)(x: () => Try[A]): PekkoFutureUnlessShutdown[A] =
    PekkoFutureUnlessShutdown(action, () => FutureUnlessShutdown.fromTry(x()))
  override def close(): Unit = ()
}
