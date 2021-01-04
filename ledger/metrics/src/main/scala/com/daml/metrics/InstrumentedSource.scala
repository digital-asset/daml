// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import akka.Done
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.codahale.metrics.{Counter, Timer}
import com.daml.dec.DirectExecutionContext

import scala.concurrent.Future

object InstrumentedSource {

  final class QueueWithComplete[T](
      delegate: SourceQueueWithComplete[(Timer.Context, T)],
      lengthCounter: Counter,
      delayTimer: Timer,
  ) extends SourceQueueWithComplete[T] {

    override def complete(): Unit = delegate.complete()

    override def fail(ex: Throwable): Unit = delegate.fail(ex)

    override def watchCompletion(): Future[Done] = delegate.watchCompletion()

    override def offer(elem: T): Future[QueueOfferResult] = {
      val result = delegate.offer(
        delayTimer.time() -> elem
      )
      // Use the `DirectExecutionContext` to ensure that the
      // counter is updated as closely as possible to the
      // update of the queue, so to offer the most consistent
      // reading possible via the counter
      result.foreach {
        case QueueOfferResult.Enqueued =>
          lengthCounter.inc()

        case _ => // do nothing
      }(DirectExecutionContext)
      result
    }
  }

  /**
    * Returns a `Source` that can be fed via the materialized queue.
    *
    * The queue length counter can at most be eventually consistent due to
    * the counter increment and decrement operation being scheduled separately
    * and possibly not in the same order as the actual enqueuing and dequeueing
    * of items.
    *
    * For this reason, you may also read values on the saturation counter which
    * are negative or exceed `bufferSize`.
    *
    * Note that the fact that the count is decremented in a second operator means
    * that its buffering will likely skew the measurements to be greater than the
    * actual value, rather than the other way around.
    *
    * We track the queue capacity as a counter as we may want to aggregate
    * the metrics for multiple individual queues of the same kind and we want to be able
    * to decrease the capacity when the queue gets completed.
    */
  def queue[T](
      bufferSize: Int,
      overflowStrategy: OverflowStrategy,
      capacityCounter: Counter,
      lengthCounter: Counter,
      delayTimer: Timer)(
      implicit materializer: Materializer,
  ): Source[T, QueueWithComplete[T]] = {
    val (queue, source) =
      Source.queue[(Timer.Context, T)](bufferSize, overflowStrategy).preMaterialize()

    val instrumentedQueue =
      new QueueWithComplete[T](queue, lengthCounter, delayTimer)
    // Using `map` and not `wireTap` because the latter is skipped on backpressure.

    capacityCounter.inc(bufferSize.toLong)
    instrumentedQueue
      .watchCompletion()
      .andThen {
        case _ => capacityCounter.dec(bufferSize.toLong)
      }(DirectExecutionContext)

    source.mapMaterializedValue(_ => instrumentedQueue).map {
      case (timingContext, item) =>
        timingContext.stop()
        lengthCounter.dec()
        item
    }
  }

}
