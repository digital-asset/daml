// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Counter}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.BatchTag
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore.TimestampedBlock
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.v1 as proto
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Mutex

import scala.concurrent.{ExecutionContext, blocking}

trait ReferenceBlockOrderingStore extends AutoCloseable {

  def insertRequest(request: BlockFormat.OrderedRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def maxBlockHeight()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Long]]

  def queryBlocks(initialHeight: Long, maxQueryBlockCount: Int)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TimestampedBlock]]
}

object ReferenceBlockOrderingStore {
  case object CounterDiscriminator
  type CounterDiscriminator = CounterDiscriminator.type
  type BlockCounter = Counter[CounterDiscriminator]
  val BlockCounter: Long => Counter[CounterDiscriminator] = Counter[CounterDiscriminator]

  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): ReferenceBlockOrderingStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryReferenceSequencerDriverStore()
      case dbStorage: DbStorage =>
        new DbReferenceBlockOrderingStore(dbStorage, timeouts, loggerFactory)
    }

  final case class TimestampedBlock(
      block: BlockFormat.Block,
      timestamp: CantonTimestamp,
      lastTopologyTimestamp: CantonTimestamp,
  )
}

class InMemoryReferenceSequencerDriverStore extends ReferenceBlockOrderingStore {
  import java.util.concurrent.ConcurrentLinkedDeque

  private val deque = new ConcurrentLinkedDeque[Traced[BlockFormat.OrderedRequest]]()
  private val lock = Mutex()

  override def insertRequest(
      request: BlockFormat.OrderedRequest
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    insertRequestInternal(request)
    FutureUnlessShutdown.unit
  }

  private def insertRequestInternal(
      request: BlockFormat.OrderedRequest
  )(implicit traceContext: TraceContext): Unit =
    blocking(lock.exclusive {
      deque.add(Traced(request)).discard
    })

  override def maxBlockHeight()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Long]] =
    FutureUnlessShutdown.pure(Option.when(!deque.isEmpty)(deque.size().toLong - 1))

  /** Query available blocks starting with the specified initial height. The blocks need to be
    * returned in consecutive block-height order i.e. contain no "gaps".
    */
  override def queryBlocks(initialHeight: Long, maxQueryBlockCount: Int)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TimestampedBlock]] =
    FutureUnlessShutdown.pure(queryBlocksInternal(initialHeight, maxQueryBlockCount))

  private[sequencer] def queryBlocksInternal(
      initialHeight: Long,
      maxQueryBlockCount: Int,
  ): Seq[TimestampedBlock] =
    if (initialHeight >= 0)
      blocking(
        lock.exclusive {
          // Get the last elements up until initial height
          val iterator = deque.descendingIterator()
          val initial = math.max(initialHeight, 0)
          val limit = math.min(initial + maxQueryBlockCount.toLong, deque.size().toLong)

          val requestsWithTimestampsAndLastTopologyTimestamps =
            (initial until limit)
              .map(_ => iterator.next())
              .reverse
              .map { request =>
                if (request.value.tag == BatchTag) {
                  val tracedBatchedBlockOrderingRequests = proto.TracedBatchedBlockOrderingRequests
                    .parseFrom(request.value.body.toByteArray)
                  val batchedRequests = tracedBatchedBlockOrderingRequests.requests
                    .map(DbReferenceBlockOrderingStore.fromProto)
                  (
                    request.value.microsecondsSinceEpoch,
                    batchedRequests,
                    CantonTimestamp.ofEpochMicro(
                      tracedBatchedBlockOrderingRequests.lastTopologyTimestampEpochMicros
                    ),
                  )
                } else
                  (
                    request.value.microsecondsSinceEpoch,
                    Seq(request),
                    SignedTopologyTransaction.InitialTopologySequencingTime,
                  )
              }
          requestsWithTimestampsAndLastTopologyTimestamps.zip(LazyList.from(initial.toInt)).map {
            case ((blockTimestamp, tracedRequests, lastTopologyTimestamp), blockHeight) =>
              TimestampedBlock(
                BlockFormat.Block(blockHeight.toLong, blockTimestamp, tracedRequests),
                CantonTimestamp.ofEpochMicro(blockTimestamp),
                lastTopologyTimestamp,
              )
          }
        }
      )
    else Seq.empty

  override def close(): Unit = ()
}
