// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

import java.util.concurrent.{ExecutorService, ThreadFactory, Executors => JavaExecutors}

import com.codahale.metrics.{InstrumentedExecutorService, MetricRegistry}
import com.daml.metrics.ExecutorServiceMetrics

import scala.concurrent.ExecutionContext

object InstrumentedExecutors {

  def newWorkStealingExecutor(
      name: String,
      parallelism: Int,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newWorkStealingPool(parallelism)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    val executionContext =
      ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
    new QueueAwareExecutionContextExecutorService(
      executionContext,
      name
    )
  }

  def newFixedThreadPool(
      name: String,
      nThreads: Int,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    val executionContext =
      ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
    new QueueAwareExecutionContextExecutorService(
      executionContext,
      name
    )
  }

  def newFixedThreadPoolWithFactory(
      name: String,
      nThreads: Int,
      threadFactory: ThreadFactory,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads, threadFactory)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    val executionContext =
      ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
    new QueueAwareExecutionContextExecutorService(
      executionContext,
      name
    )
  }

  def newCachedThreadPoolWithFactory(
      name: String,
      threadFactory: ThreadFactory,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newCachedThreadPool(threadFactory)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    val executionContext =
      ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
    new QueueAwareExecutionContextExecutorService(
      executionContext,
      name
    )
  }

  private def addMetricsToExecutorService(
      name: String,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      executorService: ExecutorService,
  ) = new InstrumentedExecutorService(
    executorServiceMetrics
      .monitorExecutorService(name, executorService),
    registry,
    name,
  )
}
