// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

import java.util.concurrent.{ExecutorService, ThreadFactory, Executors => JavaExecutors}

import com.codahale.metrics.{InstrumentedExecutorService, MetricRegistry}
import com.daml.metrics.ExecutorServiceMetrics

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object InstrumentedExecutors {

  def newWorkStealingExecutor(
      name: String,
      parallelism: Int,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): ExecutionContextExecutorService = {
    val executorService = JavaExecutors.newWorkStealingPool(parallelism)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
  }

  def newFixedThreadPool(
      name: String,
      nThreads: Int,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): ExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
  }

  def newFixedThreadPoolWithFactory(
      name: String,
      nThreads: Int,
      threadFactory: ThreadFactory,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): ExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads, threadFactory)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
  }

  def newCachedThreadPoolWithFactory(
      name: String,
      threadFactory: ThreadFactory,
      registry: MetricRegistry,
      executorServiceMetrics: ExecutorServiceMetrics,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): ExecutionContextExecutorService = {
    val executorService = JavaExecutors.newCachedThreadPool(threadFactory)
    val executorServiceWithMetrics =
      addMetricsToExecutorService(name, registry, executorServiceMetrics, executorService)
    ExecutionContext.fromExecutorService(executorServiceWithMetrics, errorReporter)
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
