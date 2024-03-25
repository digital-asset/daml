// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

import java.util.concurrent.{ThreadFactory, Executors => JavaExecutors}

import com.daml.executors.executors.QueueAwareExecutionContextExecutorService
import com.daml.metrics.ExecutorServiceMetrics

import scala.concurrent.ExecutionContext

object InstrumentedExecutors {

  def newWorkStealingExecutor(
      name: String,
      parallelism: Int,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newWorkStealingPool(parallelism)
    val executorServiceWithMetrics = executorServiceMetrics
      .monitorExecutorService(name, executorService)
    new QueueAwareExecutionContextExecutorService(
      executorServiceWithMetrics,
      name,
      errorReporter,
    )
  }

  def newFixedThreadPool(
      name: String,
      nThreads: Int,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads)
    val executorServiceWithMetrics = executorServiceMetrics
      .monitorExecutorService(name, executorService)
    new QueueAwareExecutionContextExecutorService(
      executorServiceWithMetrics,
      name,
      errorReporter,
    )
  }

  def newFixedThreadPoolWithFactory(
      name: String,
      nThreads: Int,
      threadFactory: ThreadFactory,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads, threadFactory)
    val executorServiceWithMetrics = executorServiceMetrics
      .monitorExecutorService(name, executorService)
    new QueueAwareExecutionContextExecutorService(
      executorServiceWithMetrics,
      name,
      errorReporter,
    )
  }

  def newCachedThreadPoolWithFactory(
      name: String,
      threadFactory: ThreadFactory,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newCachedThreadPool(threadFactory)
    val executorServiceWithMetrics = executorServiceMetrics
      .monitorExecutorService(name, executorService)
    new QueueAwareExecutionContextExecutorService(
      executorServiceWithMetrics,
      name,
      errorReporter,
    )
  }

}
