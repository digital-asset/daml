// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.util
import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}

import com.daml.metrics.api.{MetricDoc, MetricsContext}
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, Factory, Meter, Timer}

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

class InstrumentedExecutorServiceMetrics(factory: Factory) {

  @MetricDoc.Tag(
    summary = "The number of tasks submitted to an instrumented executor.",
    description = "Number of tasks that were submitted to the executor.",
    qualification = Debug,
  )
  val submitted: Meter =
    factory.meter(
      InstrumentedExecutorServiceMetrics.Prefix :+ "submitted",
      "Number of tasks submitted to this executor",
    )

  @MetricDoc.Tag(
    summary = "The number of tasks running in an instrumented executor.",
    description = "Currently running number of tasks.",
    qualification = Debug,
  )
  val running: Counter = factory.counter(
    InstrumentedExecutorServiceMetrics.Prefix :+ "running",
    "The number of currently running tasks.",
  )

  @MetricDoc.Tag(
    summary = "The number of tasks completed in an instrumented executor.",
    description = "Number of tasks that were completed by the executor.",
    qualification = Debug,
  )
  val completed: Meter = factory.meter(
    InstrumentedExecutorServiceMetrics.Prefix :+ "completed",
    "Number of tasks completed by this executor",
  )

  @MetricDoc.Tag(
    summary = "The time that a task is idle in an instrumented executor.",
    description =
      "A task is considered idle if it was submitted to the executor but it has not started execution yet.",
    qualification = Debug,
  )
  val idle: Timer = factory.timer(
    InstrumentedExecutorServiceMetrics.Prefix :+ "idle",
    "Time passed since the tasks was submitted to the executor until it started execution.",
  )

  @MetricDoc.Tag(
    summary = "The duration of a task is running in an instrumented executor.",
    description = "A task is considered running only after it has started execution.",
    qualification = Debug,
  )
  val duration: Timer = factory.timer(
    InstrumentedExecutorServiceMetrics.Prefix :+ "duration",
    "Time passed from the moment the task started execution until it has finished execution. Idle time is not considered as part of this timer.",
  )
}
object InstrumentedExecutorServiceMetrics {

  private val Prefix = ExecutorServiceMetrics.Prefix :+ "runtime"

  /** Provides instrumentation for all the submissions to the executor service.
    * Note:
    * - We might want to change in the future how instrumentation for the `invokeAll`/`invokeAny` is done.
    *    Currently we treat all the tasks as individual tasks but from an executor point of view
    *    we might want to monitor the different semantics that those methods have (eg: 1 submission but multiple tasks,
    *    timing when the slowest finishes or when the fastest finishes..)
    */
  class InstrumentedExecutorService(
      delegate: ExecutorService,
      metrics: InstrumentedExecutorServiceMetrics,
      name: String,
  ) extends ExecutorService {

    private implicit val metricsContext: MetricsContext = MetricsContext(
      ExecutorServiceMetrics.NameLabelKey -> name
    )

    override def shutdown(): Unit = delegate.shutdown()

    override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()

    override def isShutdown: Boolean = delegate.isShutdown

    override def isTerminated: Boolean = delegate.isTerminated

    override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean =
      delegate.awaitTermination(l, timeUnit)

    override def submit[T](callable: Callable[T]): Future[T] = {
      metrics.submitted.mark()
      delegate.submit(new InstrumentedCallable[T](callable))
    }

    override def submit[T](runnable: Runnable, t: T): Future[T] = {
      metrics.submitted.mark()
      delegate.submit(new InstrumentedRunnable(runnable), t)
    }

    override def submit(runnable: Runnable): Future[_] = {
      metrics.submitted.mark()
      delegate.submit(new InstrumentedRunnable(runnable))
    }

    override def invokeAll[T](
        collection: util.Collection[_ <: Callable[T]]
    ): util.List[Future[T]] = {
      metrics.submitted.mark(collection.size().toLong)
      delegate.invokeAll(collection.asScala.map(new InstrumentedCallable(_)).toSeq.asJava)
    }

    override def invokeAll[T](
        collection: util.Collection[_ <: Callable[T]],
        l: Long,
        timeUnit: TimeUnit,
    ): util.List[Future[T]] = {
      metrics.submitted.mark(collection.size().toLong)
      delegate.invokeAll(
        collection.asScala.map(new InstrumentedCallable(_)).toSeq.asJava,
        l,
        timeUnit,
      )
    }

    override def invokeAny[T](collection: util.Collection[_ <: Callable[T]]): T = {
      metrics.submitted.mark(collection.size().toLong)
      delegate.invokeAny(
        collection.asScala.map(new InstrumentedCallable(_)).toSeq.asJava
      )
    }

    override def invokeAny[T](
        collection: util.Collection[_ <: Callable[T]],
        l: Long,
        timeUnit: TimeUnit,
    ): T = {
      metrics.submitted.mark(collection.size().toLong)
      delegate.invokeAny(
        collection.asScala.map(new InstrumentedCallable(_)).toSeq.asJava,
        l,
        timeUnit,
      )
    }

    override def execute(runnable: Runnable): Unit = {
      metrics.submitted.mark()
      delegate.execute(new InstrumentedRunnable(runnable))
    }

    class InstrumentedCallable[T](delegate: Callable[T]) extends Callable[T] {

      private val idleTimer = metrics.idle.startAsync()
      metrics.running.inc()

      override def call(): T = {
        idleTimer.stop()
        val runningTimer = metrics.duration.startAsync()
        try {
          delegate.call()
        } finally {
          runningTimer.stop()
          metrics.completed.mark()
          metrics.running.dec()
        }
      }
    }

    class InstrumentedRunnable(delegate: Runnable) extends Runnable {

      private val idleTimer = metrics.idle.startAsync()
      metrics.running.inc()

      override def run(): Unit = {
        idleTimer.stop()
        val runningTimer = metrics.duration.startAsync()
        try {
          delegate.run()
        } finally {
          runningTimer.stop()
          metrics.completed.mark()
          metrics.running.dec()
        }
      }
    }

  }

}
