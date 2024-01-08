// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference.store

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{Counter, PeanoTreeQueue}
import com.digitalasset.canton.domain.block.BlockOrderer
import com.digitalasset.canton.domain.block.BlockOrderingSequencer.BatchTag
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore.{
  BlockCounter,
  CounterDiscriminator,
}
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.v0 as proto
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import scala.concurrent.{ExecutionContext, Future, blocking}

trait ReferenceBlockOrderingStore {

  def insertRequest(request: BlockOrderer.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def insertRequestWithHeight(blockHeight: Long, request: BlockOrderer.OrderedRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def countBlocks()(implicit
      traceContext: TraceContext
  ): Future[Long]

  def queryBlocks(initialHeight: Long)(implicit
      traceContext: TraceContext
  ): Future[Seq[BlockOrderer.Block]]
}

object ReferenceBlockOrderingStore {
  case object CounterDiscriminator
  type CounterDiscriminator = CounterDiscriminator.type
  type BlockCounter = Counter[CounterDiscriminator]
  val BlockCounter = Counter[CounterDiscriminator]

  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): ReferenceBlockOrderingStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryReferenceSequencerDriverStore()
      case dbStorage: DbStorage =>
        new DbReferenceBlockOrderingStore(dbStorage, timeouts, loggerFactory)
    }
}

class InMemoryReferenceSequencerDriverStore extends ReferenceBlockOrderingStore {
  import java.util.concurrent.ConcurrentLinkedDeque

  private val deque = new ConcurrentLinkedDeque[Seq[Traced[BlockOrderer.OrderedRequest]]]()
  private val peanoQueue =
    new PeanoTreeQueue[CounterDiscriminator, BlockOrderer.OrderedRequest](BlockCounter(0L))

  override def insertRequest(
      request: BlockOrderer.OrderedRequest
  )(implicit traceContext: TraceContext): Future[Unit] = {
    insertRequestInternal(request)
    Future.unit
  }

  private def insertRequestInternal(
      request: BlockOrderer.OrderedRequest
  )(implicit traceContext: TraceContext): Unit = blocking(deque.synchronized {

    val blockRequests =
      if (request.tag == BatchTag)
        proto.TracedBatchedBlockOrderingRequests
          .parseFrom(
            request.body.toByteArray
          )
          .requests
          .map(DbReferenceBlockOrderingStore.fromProto)
      else Seq(Traced(request))

    deque.add(blockRequests).discard
  })

  def insertRequestWithHeight(blockHeight: Long, request: BlockOrderer.OrderedRequest)(implicit
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

  override def countBlocks()(implicit
      traceContext: TraceContext
  ): Future[Long] = Future.successful(deque.size().toLong)

  /** Query available blocks starting with the specified initial height.
    * The blocks need to be returned in consecutive block-height order i.e. contain no "gaps".
    */
  override def queryBlocks(initialHeight: Long)(implicit
      traceContext: TraceContext
  ): Future[Seq[BlockOrderer.Block]] = Future.successful(queryBlocksInternal(initialHeight))

  private[sequencer] def queryBlocksInternal(initialHeight: Long): Seq[BlockOrderer.Block] =
    if (initialHeight >= 0)
      blocking(
        deque.synchronized {
          // Get the last elements up until initial height
          val iterator = deque.descendingIterator()
          val initial = math.max(initialHeight, 0)
          val requests = (initial until deque.size().toLong).map(_ => iterator.next()).reverse
          requests.zip(LazyList.from(initial.toInt)).map { case (tracedRequest, height) =>
            BlockOrderer
              .Block(
                height.toLong,
                tracedRequest,
              )
          }
        }
      )
    else Seq()
}
