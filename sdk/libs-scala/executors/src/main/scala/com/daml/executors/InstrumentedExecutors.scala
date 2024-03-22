// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors

import java.util.concurrent.{ThreadFactory, Executors => JavaExecutors}

import com.daml.executors.executors.QueueAwareExecutionContextExecutorService

import scala.concurrent.ExecutionContext

object InstrumentedExecutors {

  def newWorkStealingExecutor(
      name: String,
      parallelism: Int,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newWorkStealingPool(parallelism)
    new QueueAwareExecutionContextExecutorService(
      executorService,
      name,
      errorReporter,
    )
  }

  def newFixedThreadPool(
      name: String,
      nThreads: Int,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads)
    new QueueAwareExecutionContextExecutorService(
      executorService,
      name,
      errorReporter,
    )
  }

  def newFixedThreadPoolWithFactory(
      name: String,
      nThreads: Int,
      threadFactory: ThreadFactory,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newFixedThreadPool(nThreads, threadFactory)
    new QueueAwareExecutionContextExecutorService(
      executorService,
      name,
      errorReporter,
    )
  }

  def newCachedThreadPoolWithFactory(
      name: String,
      threadFactory: ThreadFactory,
      errorReporter: Throwable => Unit = ExecutionContext.defaultReporter,
  ): QueueAwareExecutionContextExecutorService = {
    val executorService = JavaExecutors.newCachedThreadPool(threadFactory)
    new QueueAwareExecutionContextExecutorService(
      executorService,
      name,
      errorReporter,
    )
  }

}
