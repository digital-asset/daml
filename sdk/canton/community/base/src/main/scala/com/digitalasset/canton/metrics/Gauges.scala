// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.data.{EitherT, OptionT}
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.CheckedT
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, blocking}

class TimedLoadGauge(timer: Timer, loadGauge: LoadGauge, delegateMetricHandle: CloseableGauge)
    extends CloseableGauge {
  def event[T](fut: => Future[T])(implicit ec: ExecutionContext): Future[T] =
    Timed.future(timer, loadGauge.event(fut))

  def eventUS[T](fut: => FutureUnlessShutdown[T])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(event(fut.unwrap))

  def syncEvent[T](body: => T): T =
    Timed.value(timer, loadGauge.syncEvent(body))

  def checkedTEvent[A, N, R](checked: => CheckedT[Future, A, N, R])(implicit
      ec: ExecutionContext
  ): CheckedT[Future, A, N, R] =
    CheckedT(event(checked.value))

  def eitherTEvent[A, B](eitherT: => EitherT[Future, A, B])(implicit
      ec: ExecutionContext
  ): EitherT[Future, A, B] =
    EitherT(event(eitherT.value))

  def eitherTEventUnlessShutdown[A, B](eitherT: => EitherT[FutureUnlessShutdown, A, B])(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, A, B] =
    EitherT(FutureUnlessShutdown(event(eitherT.value.unwrap)))

  def optionTEvent[A](optionT: => OptionT[Future, A])(implicit
      ec: ExecutionContext
  ): OptionT[Future, A] =
    OptionT(event(optionT.value))

  override def close(): Unit = delegateMetricHandle.close()

  override def name: String = delegateMetricHandle.name

  override def metricType: String = delegateMetricHandle.metricType
}

class LoadGauge(interval: FiniteDuration, now: => Long = System.nanoTime) {

  private val intervalNanos = interval.toNanos
  private val measure = mutable.ListBuffer[(Boolean, Long)]()
  private val eventCount = new AtomicInteger(1)

  record(false)

  private def record(loaded: Boolean): Unit = blocking(synchronized {
    val count = if (loaded) {
      eventCount.getAndIncrement()
    } else {
      eventCount.decrementAndGet()
    }
    if (count == 0) {
      measure.append((loaded, now))
    }
    cleanup(now)
  })

  def event[T](fut: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    record(true)
    fut.thereafter { _ =>
      record(false)
    }
  }

  def syncEvent[T](body: => T): T = {
    record(true)
    try {
      body
    } finally {
      record(false)
    }
  }

  @tailrec
  private def cleanup(tm: Long): Unit = {
    // keep on cleaning up
    if (measure.lengthCompare(1) > 0 && measure(1)._2 <= tm - intervalNanos) {
      measure.remove(0).discard
      cleanup(tm)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.IterableOps"))
  def getLoad: Double = {
    val endTs = now
    val startTs = endTs - intervalNanos

    def computeLoad(lastLoaded: Boolean, lastTs: Long, ts: Long): Long =
      if (lastLoaded) ts - lastTs else 0

    blocking(synchronized {
      cleanup(endTs)
      // We know that t0 <= t1 <= ... <= tn <= endTs and
      // startTs < t1, if t1 exists.

      val (loaded0, t0) = measure.head

      var lastLoaded = loaded0
      var lastTs = Math.max(t0, startTs)
      var load = 0L

      for ((loaded, ts) <- measure.tail) {
        load += computeLoad(lastLoaded, lastTs, ts)

        lastLoaded = loaded
        lastTs = ts
      }

      load += computeLoad(lastLoaded, lastTs, endTs)

      load.toDouble / intervalNanos
    })
  }

}
