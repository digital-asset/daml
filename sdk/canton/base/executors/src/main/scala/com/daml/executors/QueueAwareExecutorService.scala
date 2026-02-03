// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}

import java.util
import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

/** REMOVE ME (it's really a bad idea to sync all threads)
  */
class QueueAwareExecutorService(
    delegate: ExecutorService,
    val name: String,
) extends ExecutorService
    with QueueAwareExecutor
    with NamedExecutor {

  override def shutdown(): Unit = delegate.shutdown()
  override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()
  override def isShutdown: Boolean = delegate.isShutdown
  override def isTerminated: Boolean = delegate.isTerminated
  override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean =
    delegate.awaitTermination(l, timeUnit)

  override def submit[T](
      callable: Callable[T]
  ): Future[T] =
    delegate.submit(new TrackingCallable[T](callable))

  override def submit[T](
      runnable: Runnable,
      t: T,
  ): Future[T] =
    delegate.submit(new TrackingRunnable(runnable), t)

  override def submit(runnable: Runnable): Future[?] =
    delegate.submit(new TrackingRunnable(runnable))

  override def invokeAll[T](
      collection: util.Collection[? <: Callable[T]]
  ): util.List[Future[T]] =
    delegate.invokeAll(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava)

  override def invokeAll[T](
      collection: util.Collection[? <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): util.List[Future[T]] =
    delegate.invokeAll(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava, l, timeUnit)

  override def invokeAny[T](
      collection: util.Collection[? <: Callable[T]]
  ): T =
    delegate.invokeAny(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava)

  override def invokeAny[T](
      collection: util.Collection[? <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): T =
    delegate.invokeAny(collection.asScala.map(new TrackingCallable[T](_)).toSeq.asJava, l, timeUnit)

  override def execute(runnable: Runnable): Unit =
    delegate.execute(new TrackingRunnable(runnable))

  class TrackingRunnable(delegate: Runnable) extends Runnable {
    override def run(): Unit =
      delegate.run()
  }

  class TrackingCallable[T](delegate: Callable[T]) extends Callable[T] {
    override def call(): T =
      delegate.call()
  }
  override def queueSize: Long = 0L
}
