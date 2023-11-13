// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.metrics.api.MetricHandle.Timer.TimerHandle
import com.daml.metrics.api.MetricHandle.{Counter, Timer}
import com.daml.metrics.api.MetricsContext

import scala.util.chaining._

object InstrumentedGraph {

  final class InstrumentedBoundedSourceQueue[T](
      delegate: BoundedSourceQueue[(TimerHandle, T)],
      bufferSize: Int,
      capacityCounter: Counter,
      lengthCounter: Counter,
      delayTimer: Timer,
  ) extends BoundedSourceQueue[T] {

    override def complete(): Unit = {
      delegate.complete()
      capacityCounter.dec(bufferSize.toLong)(MetricsContext.Empty)
    }

    override def size(): Int = bufferSize

    override def fail(ex: Throwable): Unit = delegate.fail(ex)

    override def offer(elem: T): QueueOfferResult = {
      val result = delegate.offer(
        delayTimer.startAsync() -> elem
      )
      result match {
        case QueueOfferResult.Enqueued =>
          lengthCounter.inc()

        case _ => // do nothing
      }
      result
    }
  }

  /** Returns a `Source` that can be fed via the materialized queue.
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
      capacityCounter: Counter,
      lengthCounter: Counter,
      delayTimer: Timer,
  )(implicit
      materializer: Materializer
  ): Source[T, BoundedSourceQueue[T]] = {
    val (boundedQueue, source) =
      Source.queue[(TimerHandle, T)](bufferSize).preMaterialize()

    val instrumentedQueue =
      new InstrumentedBoundedSourceQueue[T](
        boundedQueue,
        bufferSize,
        capacityCounter,
        lengthCounter,
        delayTimer,
      )
    capacityCounter.inc(bufferSize.toLong)(MetricsContext.Empty)

    source.mapMaterializedValue(_ => instrumentedQueue).map { case (timer, item) =>
      timer.stop()
      lengthCounter.dec()
      item
    }
  }

  implicit class BufferedFlow[In, Out, Mat](val original: Flow[In, Out, Mat]) extends AnyVal {

    /** Adds a buffer to the output of the [[original]] flow, and adds a Counter metric for buffer size.
      *
      * Good for detecting bottlenecks and speed difference between consumer and producer.
      * In case producer is faster, this buffer should be mostly empty.
      * In case producer is slower, this buffer should be mostly full.
      *
      * @param counter the counter to track the actual size of the buffer
      * @param size the maximum size of the buffer.
      *             In case of a bottleneck in producer this will be mostly full,
      *             so careful estimation is needed to prevent excessive memory pressure.
      * @return the instrumented flow
      */
    def buffered(counter: Counter, size: Int): Flow[In, Out, Mat] =
      original
        // since wireTap is not guaranteed to be executed always, we need map to prevent counter skew over time.
        .map(_.tap(_ => counter.inc()))
        .buffer(size, OverflowStrategy.backpressure)
        .map(_.tap(_ => counter.dec()))
  }

  implicit class BufferedSource[Out, Mat](val original: Source[Out, Mat]) extends AnyVal {
    def buffered(counter: Counter, size: Int): Source[Out, Mat] =
      original.via(Flow[Out].buffered(counter, size))
  }
}
