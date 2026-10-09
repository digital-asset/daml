// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.BatchAggregatorConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.FutureUnlessShutdownThereafterContent
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.BatchAggregatorImpl.ItemsAndCompletionPromise
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.ForFailedOps

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.{IterableOps, immutable}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** This batch aggregator exposes a [[BatchAggregator.run]] method that allows for batching
  * [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] computations, defined by a
  * [[BatchAggregator.Processor]].
  *
  * Note: it is required that `getter` and `batchGetter` do not throw an exception. If they do, the
  * number of in-flight requests could fail to be decremented which would result in degraded
  * performance or even prevent calls to the getters.
  */
trait BatchAggregator[A, B] {

  protected def maximumBatchSize: PositiveInt

  /** Runs the processor of this aggregator for the given item, possibly batching several items.
    *
    * This method can be used as the `mappingFunction` of a Scaffeine async cache.
    *
    * @return
    *   The [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] completes with the
    *   processor's response to this item, after the batch of items has finished. If the processor
    *   fails with an exception for some item in a batch, the exception may propagate to the
    *   [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]]s of all items in the batch.
    */
  def run(item: A)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B]

  /** Runs the processor of this aggregator for the given item, possibly batching several items, but
    * without splitting them up into multiple batches.
    *
    * This is useful when multiple items need to end up in the same batch, such as batched DB insert
    * in which all rows need to be part of the same transaction, for example for CFT purposes.
    *
    * @return
    *   The [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] complete with the processor's
    *   response to these items, after the batch containing them has finished. If the processor
    *   fails with an exception for some item in a batch, the exception may propagate to the
    *   [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]]s of all items in the batch.
    */
  final def runInSameBatch(items: NonEmpty[Seq[Traced[A]]])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Either[PositiveInt, FutureUnlessShutdown[immutable.Iterable[B]]] =
    Either.cond(items.sizeIs <= maximumBatchSize.value, runTogether(items), maximumBatchSize)

  protected def runTogether(items: NonEmpty[Seq[Traced[A]]])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[immutable.Iterable[B]]
}

object BatchAggregator {
  def apply[A, B](
      processor: Processor[A, B],
      config: BatchAggregatorConfig,
  ): BatchAggregator[A, B] = config match {
    case BatchAggregatorConfig.AutoBatching(maximumInFlight, maximumBatchSize) =>
      new BatchAggregatorImpl[A, B](
        processor,
        maximumInFlight = maximumInFlight.unwrap,
        maximumBatchSize = maximumBatchSize,
      )

    case BatchAggregatorConfig.NoAutoBatching(maximumBatchSize) =>
      new NoOpBatchAggregator[A, B](
        processor.executeSingle(_)(_, _, _),
        processor.executeBatch(_)(_, _),
        maximumBatchSize,
      )
  }

  /** Processor that defines the computation that a [[BatchAggregator]] batches. */
  trait Processor[A, B] {

    /** Human-readable description of the kind of items that can be batched */
    def kind: String

    /** Logger to be used by the [[com.digitalasset.canton.util.BatchAggregator]] */
    def logger: TracedLogger

    /** Computation for a single item. Should be equivalent to
      * {{{
      *   executeBatch(Seq(Traced(item))).map(_.head)
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
      * @return
      *   The responses for the items in the correct order. Must have the same length
      */
    def executeBatch(items: NonEmpty[Seq[Traced[A]]])(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[immutable.Iterable[B]]

    /** Pretty printer for items */
    def prettyItem: Pretty[A]

    case class NoResponseForAggregatedItemException(item: A)
        extends RuntimeException({
          implicit val prettyA: Pretty[A] = prettyItem
          show"No response for $kind $item"
        })
  }
}

class NoOpBatchAggregator[A, B](
    executeSingle: (A, ExecutionContext, TraceContext, CloseContext) => FutureUnlessShutdown[B],
    executeBatch: (
        NonEmpty[Seq[Traced[A]]],
        TraceContext,
        CloseContext,
    ) => FutureUnlessShutdown[immutable.Iterable[B]],
    override protected val maximumBatchSize: PositiveInt,
) extends BatchAggregator[A, B] {
  override def run(item: A)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B] =
    executeSingle(item, ec, traceContext, callerCloseContext)

  override def runTogether(items: NonEmpty[Seq[Traced[A]]])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[immutable.Iterable[B]] =
    executeBatch(items, traceContext, callerCloseContext)
}

class BatchAggregatorImpl[A, B](
    processor: BatchAggregator.Processor[A, B],
    private val maximumInFlight: Int,
    protected override val maximumBatchSize: PositiveInt,
) extends BatchAggregator[A, B] {

  private val batcher =
    new BestFittingBatcher[ItemsAndCompletionPromise[A, B]](
      maxBatchSize = maximumBatchSize
    )

  private val inFlight = new AtomicInteger(0)

  override def run(item: A)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B] = {
    implicit val prettyA: Pretty[A] = processor.prettyItem
    runTogether(NonEmpty(Seq, Traced(item))).map(
      _.headOption.getOrElse {
        val msg = show"BatchAggregatorImpl.runTogether returned an empty Iterable for item $item"
        val error = new IllegalStateException(msg)
        processor.logger.error(ErrorUtil.internalErrorMessage, error)
        throw error
      }
    )
  }

  override def runTogether(items: NonEmpty[Seq[Traced[A]]])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[immutable.Iterable[B]] = {
    val oldInFlight = inFlight.getAndUpdate(v => (v + 1).min(maximumInFlight))

    if (oldInFlight < maximumInFlight) { // issue single request
      if (items.sizeIs == 1) {
        val item = items.head1
        implicit val traceContext: TraceContext = item.traceContext
        runSingleWithoutIncrement(item.value).map(immutable.Iterable(_))
      } else {
        runBatchWithoutIncrement(items)
      }
    } else { // add to the queue
      val promise = PromiseUnlessShutdown.unsupervised[immutable.Iterable[B]]()
      batcher.add(ItemsAndCompletionPromise(items.toVector, promise)).discard
      maybeRunQueuedQueries()
      promise.futureUS
    }
  }

  private def runSingleWithoutIncrement(
      item: A
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[B] =
    FutureUnlessShutdown
      .fromTry(Try(processor.executeSingle(item)))
      .flatten
      .thereafter(maybeRunAfterProcessing(NonEmpty(Seq, item)))

  private def runBatchWithoutIncrement(
      items: NonEmpty[Seq[Traced[A]]]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[immutable.Iterable[B]] =
    FutureUnlessShutdown
      .fromTry(Try(processor.executeBatch(items)))
      .flatten
      .thereafter(maybeRunAfterProcessing(processed = items.map(_.value)))

  private def maybeRunAfterProcessing(
      processed: NonEmpty[Seq[A]]
  )(result: FutureUnlessShutdownThereafterContent[?])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Unit = {
    inFlight.decrementAndGet().discard
    maybeRunQueuedQueries()
    result.forFailed {
      implicit val prettyItem: Pretty[A] = processor.prettyItem
      processor.logger.error(
        show"Failed to process ${processor.kind.unquoted} $processed",
        _,
      )
    }
  }

  /*
    If possible (i.e., if the number of in-flight items is not too big) and
    if the queue is non-empty, execute a batch of items.
   */
  @SuppressWarnings(Array("org.wartremover.warts.While", "org.wartremover.warts.Var"))
  private def maybeRunQueuedQueries()(implicit
      ec: ExecutionContext,
      callerCloseContext: CloseContext,
  ): Unit = {
    val oldInFlight = inFlight.getAndUpdate(v => (v + 1).min(maximumInFlight))

    if (oldInFlight < maximumInFlight) {
      batcher.poll() match {
        case Some(itemsAndCompletionPromisesNE) =>
          if (
            itemsAndCompletionPromisesNE.sizeIs == 1 && itemsAndCompletionPromisesNE.head1.items.sizeIs == 1
          ) {
            val ItemsAndCompletionPromise(items, promise) = itemsAndCompletionPromisesNE.head1
            val tracedItem = items.head1
            tracedItem.withTraceContext { implicit traceContext => item =>
              promise
                .completeWithUS(runSingleWithoutIncrement(item).map(immutable.Iterable(_)))
                .discard
            }
          } else {
            val itemsNE = itemsAndCompletionPromisesNE.flatMap(_.items)
            val responsesSizesAndPromisesNE = itemsAndCompletionPromisesNE.map { iap =>
              iap.items.size -> iap.completionPromise
            }
            val batchTraceContext =
              TraceContext.ofBatch("run_batch_queued_queries")(itemsNE)(processor.logger)

            FutureUnlessShutdown
              .fromTry(
                Try(processor.executeBatch(itemsNE)(batchTraceContext, callerCloseContext))
              )
              .flatten
              .onComplete { result =>
                inFlight.decrementAndGet()
                maybeRunQueuedQueries()
                result match {
                  case Success(UnlessShutdown.Outcome(responses)) =>
                    var responseIterator = responses.iterator
                    responsesSizesAndPromisesNE.foreach { case (size, promise) =>
                      val (responses, newResponsesIterator) = responseIterator.splitAt(size)
                      responseIterator = newResponsesIterator
                      promise.success(UnlessShutdown.Outcome(responses.toSeq))
                    }
                    // Complain about too many items
                    val excessItemsCount = itemsNE.size - responses.size
                    if (excessItemsCount > 0) {
                      processor.logger.error(
                        s"Detected $excessItemsCount excess items for ${processor.kind} batch"
                      )(batchTraceContext)
                    }
                    // Complain about too many responses
                    val excessResponseCount = responseIterator.length
                    if (excessResponseCount > 0) {
                      processor.logger.error(
                        s"Received $excessResponseCount excess responses for ${processor.kind} batch"
                      )(batchTraceContext)
                    }
                  case Success(UnlessShutdown.AbortedDueToShutdown) =>
                    responsesSizesAndPromisesNE.foreach { case (_, promise) =>
                      promise.shutdown_()
                    }
                  case Failure(ex) =>
                    implicit val prettyItem: Pretty[A] = processor.prettyItem
                    processor.logger
                      .error(
                        show"Batch request failed for ${processor.kind.unquoted}s ${itemsNE.map(_.value).toList}",
                        ex,
                      )(
                        batchTraceContext
                      )
                    responsesSizesAndPromisesNE.foreach { case (_, promise) =>
                      promise.failure(ex)
                    }
                }
              }
          }
        case None => inFlight.decrementAndGet().discard[Int]
      }
    } else ()
  }
}

object BatchAggregatorImpl {

  private final case class ItemsAndCompletionPromise[A, B](
      items: NonEmpty[Vector[Traced[A]]],
      completionPromise: PromiseUnlessShutdown[immutable.Iterable[B]],
  ) extends BestFittingBatcher.Sized {
    override val size: PositiveInt = PositiveInt.tryCreate(items.size)
    override def sizeIs: IterableOps.SizeCompareOps = items.sizeIs
  }
}
