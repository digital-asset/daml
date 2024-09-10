// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.ForFailedOps

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** This batch aggregator exposes a [[BatchAggregatorUS.run]] method
  * that allows for batching [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] computations,
  * defined by a [[BatchAggregatorUS.ProcessorUS]].
  *
  * Note: it is required that `getter` and `batchGetter` do not throw an exception.
  * If they do, the number of in-flight requests could fail to be decremented which
  * would result in degraded performance or even prevent calls to the getters.
  */

trait BatchAggregatorUS[A, B] {

  /** Runs the processor of this aggregator for the given item,
    * possibly batching several items.
    *
    * @return The [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] completes with the processor's response to this item,
    *         after the batch of items has finished. If the processor fails with an exception for
    *         some item in a batch, the exception may propagate to the [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]]s
    *         of all items in the batch.
    */
  def run(item: A)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B]
}

object BatchAggregatorUS {
  def apply[A, B](
      processor: ProcessorUS[A, B],
      config: BatchAggregatorConfig,
  ): BatchAggregatorUS[A, B] = config match {
    case BatchAggregatorConfig.Batching(maximumInFlight, maximumBatchSize) =>
      new BatchAggregatorUSImpl[A, B](
        processor,
        maximumInFlight = maximumInFlight.unwrap,
        maximumBatchSize = maximumBatchSize.unwrap,
      )

    case BatchAggregatorConfig.NoBatching =>
      new NoOpBatchAggregatorUS[A, B](processor.executeSingle(_)(_, _, _))
  }

  /** Processor that defines the computation that a [[BatchAggregatorUS]] batches. */
  trait ProcessorUS[A, B] {

    /** Human-readable description of the kind of items that can be batched */
    def kind: String

    /** Logger to be used by the [[com.digitalasset.canton.util.BatchAggregatorUS]] */
    def logger: TracedLogger

    /** Computation for a single item.
      * Should be equivalent to
      * {{{
      *   executeBatch(NonEmpty.from(Seq(Traced(item)))).map(_.head)
      * }}}
      */
    def executeSingle(
        item: A
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[B] =
      executeBatch(NonEmpty(Seq, Traced(item))).flatMap(_.headOption match {
        case Some(value) => FutureUnlessShutdown.pure(value)
        case None =>
          val error = s"executeBatch returned an empty sequence of results"
          logger.error(error)
          FutureUnlessShutdown.failed(new RuntimeException(error))
      })

    /** Computation for a batch of items.
      *
      * @return The responses for the items in the correct order.
      *         Must have the same length
      */
    def executeBatch(items: NonEmpty[Seq[Traced[A]]])(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[Iterable[B]]

    /** Pretty printer for items */
    def prettyItem: Pretty[A]

    case class AbortedDueToShutdownException()
        extends RuntimeException({
          show"Aborted due to shutdown."
        })

    case class NoResponseForAggregatedItemException(item: A)
        extends RuntimeException({
          implicit val prettyA: Pretty[A] = prettyItem
          show"No response for $kind $item"
        })
  }
}

class NoOpBatchAggregatorUS[A, B](
    executeSingle: (A, ExecutionContext, TraceContext, CloseContext) => FutureUnlessShutdown[B]
) extends BatchAggregatorUS[A, B] {
  override def run(item: A)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B] =
    executeSingle(item, ec, traceContext, callerCloseContext)
}

class BatchAggregatorUSImpl[A, B](
    processor: BatchAggregatorUS.ProcessorUS[A, B],
    private val maximumInFlight: Int,
    private val maximumBatchSize: Int,
) extends BatchAggregatorUS[A, B] {

  private val inFlight = new AtomicInteger(0)
  private type QueueType = (Traced[A], PromiseUnlessShutdown[B])

  private val queuedRequests: ConcurrentLinkedQueue[QueueType] =
    new ConcurrentLinkedQueue[QueueType]()

  override def run(item: A)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B] =
    maybeMeasureTime {
      val oldInFlight = inFlight.getAndUpdate(v => (v + 1).min(maximumInFlight))

      if (oldInFlight < maximumInFlight) { // issue single request
        runSingleWithoutIncrement(item)
      } else { // add to the queue
        val promise = new PromiseUnlessShutdown[B]("run-batch", FutureSupervisor.Noop)(
          ErrorLoggingContext.fromTracedLogger(processor.logger)
        )
        queuedRequests.add((Traced(item), promise)).discard[Boolean]
        maybeRunQueuedQueries()
        promise.futureUS
      }
    }

  private def maybeMeasureTime(f: => FutureUnlessShutdown[B]): FutureUnlessShutdown[B] = f

  private def runSingleWithoutIncrement(
      item: A
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B] =
    FutureUnlessShutdown.fromTry(Try(processor.executeSingle(item))).flatten.thereafter { result =>
      inFlight.decrementAndGet().discard[Int]
      maybeRunQueuedQueries()
      result.forFailed {
        implicit val prettyItem: Pretty[A] = processor.prettyItem
        processor.logger.error(show"Failed to process ${processor.kind.unquoted} $item", _)
      }
    }

  /*
    If possible (i.e., if the number of in-flight items is not too big) and
    if the queue is non-empty, execute a batch of items.
   */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def maybeRunQueuedQueries()(implicit
      ec: ExecutionContext,
      callerCloseContext: CloseContext,
  ): Unit = {
    val oldInFlight = inFlight.getAndUpdate(v => (v + 1).min(maximumInFlight))

    if (oldInFlight < maximumInFlight) {
      val queueItems = pollItemsFromQueue()

      NonEmpty.from(queueItems) match {
        case Some(queueItemsNE) =>
          if (queueItemsNE.lengthCompare(1) == 0) {
            val (tracedItem, promise) = queueItemsNE.head1
            tracedItem.withTraceContext { implicit traceContext => item =>
              promise
                .completeWith(runSingleWithoutIncrement(item))
                .discard[PromiseUnlessShutdown[B]]
            }
          } else {
            val items = queueItemsNE.map(_._1)
            val batchTraceContext = TraceContext.ofBatch(items.toList)(processor.logger)

            FutureUnlessShutdown
              .fromTry(Try(processor.executeBatch(items)(batchTraceContext, callerCloseContext)))
              .flatten
              .onComplete { result =>
                inFlight.decrementAndGet()
                maybeRunQueuedQueries()

                result match {
                  case Success(UnlessShutdown.Outcome(responses)) =>
                    val responseIterator = responses.iterator
                    val queueItemIterator = queueItems.iterator

                    while (queueItemIterator.hasNext && responseIterator.hasNext) {
                      val (_item, promise) = queueItemIterator.next()
                      val response = responseIterator.next()
                      promise.success(UnlessShutdown.Outcome(response))
                    }

                    // Complain about too few responses
                    queueItemIterator.foreach { case (tracedItem, promise) =>
                      tracedItem.withTraceContext[Unit] { implicit traceContext => item =>
                        val noResponseError = processor.NoResponseForAggregatedItemException(item)
                        processor.logger.error(ErrorUtil.internalErrorMessage, noResponseError)
                        promise.failure(noResponseError)
                      }
                    }

                    // Complain about too many responses
                    val excessResponseCount = responseIterator.length
                    if (excessResponseCount > 0) {
                      processor.logger.error(
                        s"Received $excessResponseCount excess responses for ${processor.kind} batch"
                      )(batchTraceContext)
                    }
                  case Success(UnlessShutdown.AbortedDueToShutdown) =>
                    queueItems.foreach { case (_, promise) =>
                      promise.success(UnlessShutdown.AbortedDueToShutdown)
                    }
                  case Failure(ex) =>
                    implicit val prettyItem = processor.prettyItem
                    processor.logger
                      .error(
                        show"Batch request failed for ${processor.kind.unquoted}s ${items.map(_.value).toList}",
                        ex,
                      )(
                        batchTraceContext
                      )
                    queueItems.foreach { case (_, promise) => promise.failure(ex) }
                }
              }
          }
        case None => inFlight.decrementAndGet().discard[Int]
      }

    } else ()
  }

  // Return at most maximumBatchSize requests from the queue
  private def pollItemsFromQueue(): Seq[QueueType] = {
    val polledItems = new mutable.ArrayDeque[QueueType](maximumBatchSize)

    @tailrec def go(remaining: Int): Unit =
      Option(queuedRequests.poll()) match {
        case Some(queueItem) =>
          polledItems.addOne(queueItem)
          if (remaining > 0) go(remaining - 1)
        case None => ()
      }

    go(maximumBatchSize)
    polledItems.toSeq
  }
}
