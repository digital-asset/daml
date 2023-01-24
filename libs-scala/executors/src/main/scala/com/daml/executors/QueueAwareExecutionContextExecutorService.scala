// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Callable, Future, TimeUnit}

import com.daml.scalautil.Statement.discard

import scala.concurrent.ExecutionContextExecutorService
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

/** Keeps track of the number of tasks submitted to the executor but that have not started execution just yet.
  * We use this wrapper to access the queue size in performance critical code paths because
  * reading the queue size from the executor itself can take an amount of time linear with the number of tasks waiting
  * in the queues.
  */
class QueueAwareExecutionContextExecutorService(
    delegate: ExecutionContextExecutorService,
    val name: String,
) extends ExecutionContextExecutorService {

  private val queueTracking = new AtomicLong(0)

  def getQueueSize: Long = queueTracking.get()

  override def shutdown(): Unit = delegate.shutdown()
  override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()
  override def isShutdown: Boolean = delegate.isShutdown
  override def isTerminated: Boolean = delegate.isTerminated
  override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean =
    delegate.awaitTermination(l, timeUnit)

  override def reportFailure(cause: Throwable): Unit = delegate.reportFailure(cause)

  override def submit[T](
      callable: Callable[T]
  ): Future[T] = {
    discard { queueTracking.incrementAndGet() }
    delegate.submit(new TrackingCallable[T](callable))
  }

  override def submit[T](
      runnable: Runnable,
      t: T,
  ): Future[T] = {
    discard { queueTracking.incrementAndGet() }
    delegate.submit(new TrackingRunnable(runnable), t)
  }

  override def submit(runnable: Runnable): Future[_] = {
    discard { queueTracking.incrementAndGet() }
    delegate.submit(new TrackingRunnable(runnable))
  }

  override def invokeAll[T](
      collection: util.Collection[_ <: Callable[T]]
  ): util.List[Future[T]] = {
    discard { queueTracking.updateAndGet(_ + collection.size()) }
    delegate.invokeAll(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava)
  }

  override def invokeAll[T](
      collection: util.Collection[_ <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): util.List[Future[T]] = {
    discard { queueTracking.updateAndGet(_ + collection.size()) }
    delegate.invokeAll(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava, l, timeUnit)
  }

  override def invokeAny[T](
      collection: util.Collection[_ <: Callable[T]]
  ): T = {
    discard { queueTracking.updateAndGet(_ + collection.size()) }
    delegate.invokeAny(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava)
  }

  override def invokeAny[T](
      collection: util.Collection[_ <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): T = {
    discard { queueTracking.updateAndGet(_ + collection.size()) }
    delegate.invokeAny(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava, l, timeUnit)
  }

  override def execute(runnable: Runnable): Unit = {
    discard { queueTracking.incrementAndGet() }
    delegate.execute(new TrackingRunnable(runnable))
  }

  class TrackingRunnable(delegate: Runnable) extends Runnable {
    override def run(): Unit = {
      discard { queueTracking.decrementAndGet() }
      delegate.run()
    }
  }

  class TrackingCallable[T](delegate: Callable[T]) extends Callable[T] {
    override def call(): T = {
      discard { queueTracking.decrementAndGet() }
      delegate.call()
    }
  }

}
