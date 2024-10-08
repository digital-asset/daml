// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Counter, PeanoTreeQueue}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.BlockFormat
import com.digitalasset.canton.domain.block.BlockFormat.BatchTag
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore.{
  BlockCounter,
  CounterDiscriminator,
  TimestampedBlock,
}
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.v1 as proto
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import scala.concurrent.{ExecutionContext, Future, blocking}

trait ReferenceBlockOrderingStore extends AutoCloseable {

  def insertRequest(request: BlockFormat.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def insertRequestWithHeight(blockHeight: Long, request: BlockFormat.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def maxBlockHeight()(implicit
      traceContext: TraceContext
  ): Future[Option[Long]]

  def queryBlocks(initialHeight: Long)(implicit
      traceContext: TraceContext
  ): Future[Seq[TimestampedBlock]]
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
  private val peanoQueue =
    new PeanoTreeQueue[CounterDiscriminator, BlockFormat.OrderedRequest](BlockCounter(0L))

  override def insertRequest(
      request: BlockFormat.OrderedRequest
  )(implicit traceContext: TraceContext): Future[Unit] = {
    insertRequestInternal(request)
    Future.unit
  }

  private def insertRequestInternal(
      request: BlockFormat.OrderedRequest
  )(implicit traceContext: TraceContext): Unit =
    blocking(deque.synchronized {
      deque.add(Traced(request)).discard
    })

  def insertRequestWithHeight(blockHeight: Long, request: BlockFormat.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {

    if (!peanoQueue.alreadyInserted(BlockCounter(blockHeight)))
      peanoQueue.insert(BlockCounter(blockHeight), request): Unit

    val blocks = LazyList
      .continually(peanoQueue.poll())
      .takeWhile(_.isDefined)
      .collect { case Some((_, block)) =>
        block
      }
      .toList

    blocks.foreach(insertRequestInternal)

    Future.unit
  }

  override def maxBlockHeight()(implicit
      traceContext: TraceContext
  ): Future[Option[Long]] = Future.successful(Option.when(!deque.isEmpty)(deque.size().toLong - 1))

  /** Query available blocks starting with the specified initial height.
    * The blocks need to be returned in consecutive block-height order i.e. contain no "gaps".
    */
  override def queryBlocks(initialHeight: Long)(implicit
      traceContext: TraceContext
  ): Future[Seq[TimestampedBlock]] = Future.successful(queryBlocksInternal(initialHeight))

  private[sequencer] def queryBlocksInternal(initialHeight: Long): Seq[TimestampedBlock] =
    if (initialHeight >= 0)
      blocking(
        deque.synchronized {
          // Get the last elements up until initial height
          val iterator = deque.descendingIterator()
          val initial = math.max(initialHeight, 0)
          val requestsWithTimestampsAndLastTopologyTimestamps =
            (initial until deque.size().toLong)
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
                BlockFormat.Block(blockHeight.toLong, tracedRequests),
                CantonTimestamp.ofEpochMicro(blockTimestamp),
                lastTopologyTimestamp,
              )
          }
        }
      )
    else Seq.empty

  override def close(): Unit = ()
}
