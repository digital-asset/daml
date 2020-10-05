// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.akka.stream

import akka.Done
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.codahale.metrics.Counter
import com.daml.dec.DirectExecutionContext

import scala.concurrent.Future

object InstrumentedSource {

  final class QueueWithComplete[T](
      delegate: SourceQueueWithComplete[T],
      saturation: Counter,
  ) extends SourceQueueWithComplete[T] {

    override def complete(): Unit = delegate.complete()

    override def fail(ex: Throwable): Unit = delegate.fail(ex)

    override def watchCompletion(): Future[Done] = delegate.watchCompletion()

    override def offer(elem: T): Future[QueueOfferResult] = {
      val result = delegate.offer(elem)
      // Use the `DirectExecutionContext` to ensure that the
      // counter is updated as closely as possible to the
      // update of the queue, so to offer the most consistent
      // reading possible via the counter
      result.foreach {
        case QueueOfferResult.Enqueued => saturation.inc()
        case _ => // do nothing
      }(DirectExecutionContext)
      result
    }
  }

  /**
    * Returns a `Source` that can be fed via the materialized queue.
    *
    * The saturation counter can at most be eventually consistent due to
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
    * FIXME The clean (but involved) thing to do, would be to re-implement
    * FIXME `Source.queue` from scratch, adding the possibility of
    * FIXME instrumenting it.
    */
  def queue[T](bufferSize: Int, overflowStrategy: OverflowStrategy, saturation: Counter)(
      implicit materializer: Materializer,
  ): Source[T, QueueWithComplete[T]] = {
    val (queue, source) = Source.queue[T](bufferSize, overflowStrategy).preMaterialize()
    val instrumentedQueue = new QueueWithComplete[T](queue, saturation)
    // Using `map` and not `wireTap` because the latter is skipped on backpressure
    source.mapMaterializedValue(_ => instrumentedQueue).map { item =>
      saturation.dec()
      item
    }
  }

}
