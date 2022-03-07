// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer, OverflowStrategy, QueueOfferResult}
import io.prometheus.client.{Gauge, Summary}

object InstrumentedSource {

  final class InstrumentedBoundedSourceQueue[T](
      delegate: BoundedSourceQueue[(Summary.Timer, T)],
      bufferSize: Int,
      capacityGauge: Gauge,
      lengthGauge: Gauge,
      delayTimer: Summary,
  ) extends BoundedSourceQueue[T] {

    override def complete(): Unit = {
      delegate.complete()
      capacityGauge.dec(bufferSize.toDouble)
    }

    override def size(): Int = bufferSize

    override def fail(ex: Throwable): Unit = delegate.fail(ex)

    override def offer(elem: T): QueueOfferResult = {
      val result = delegate.offer(
        delayTimer.startTimer() -> elem
      )
      result match {
        case QueueOfferResult.Enqueued =>
          lengthGauge.inc()

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
      capacityGauge: Gauge,
      lengthGauge: Gauge,
      delayTimer: Summary,
  )(implicit
      materializer: Materializer
  ): Source[T, BoundedSourceQueue[T]] = {
    val (boundedQueue, source) =
      Source.queue[(Summary.Timer, T)](bufferSize).preMaterialize()

    val instrumentedQueue =
      new InstrumentedBoundedSourceQueue[T](
        boundedQueue,
        bufferSize,
        capacityGauge,
        lengthGauge,
        delayTimer,
      )
    capacityGauge.inc(bufferSize.toDouble)

    source.mapMaterializedValue(_ => instrumentedQueue).map { case (timer, item) =>
      timer.observeDuration()
      lengthGauge.dec()
      item
    }
  }

  /** Adds a buffer to the output of the original source, and adds a Counter metric for buffer size.
    *
    * Good for detecting bottlenecks and speed difference between consumer and producer.
    * In case producer is faster, this buffer should be mostly empty.
    * In case producer is slower, this buffer should be mostly full.
    *
    * @param original the original source which will be instrumented
    * @param counter the counter to track the actual size of the buffer
    * @param size the maximum size of the buffer. In case of a bottleneck in producer this will be mostly full, so careful estimation needed to prevent excessive memory pressure
    * @tparam T
    * @tparam U
    * @return the instrumentes source
    */
  def bufferedSource[T, U](
      original: Source[T, U],
      counter: com.codahale.metrics.Counter,
      size: Int,
  ): Source[T, U] = {
    def tap(block: => Unit): T => T = t => {
      block
      t
    }
    original
      .map(
        tap(counter.inc())
      ) // since wireTap is not guaranteed to be executed always, we need map to prevent counter skew over time.
      .buffer(size, OverflowStrategy.backpressure)
      .map(tap(counter.dec()))
  }
}
