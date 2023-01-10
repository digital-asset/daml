// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import com.codahale.metrics.MetricRegistry
import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.MetricName
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, Future}

object AsyncSupport {

  trait Executor {
    def execute[FIN, FOUT](f: FIN => FOUT): FIN => Future[FOUT]
  }

  object Executor {
    def forExecutionContext(executionContext: ExecutionContext): Executor =
      new Executor {
        override def execute[FIN, FOUT](f: FIN => FOUT): FIN => Future[FOUT] =
          in => Future(f(in))(executionContext)
      }
  }

  def asyncPool(
      size: Int,
      namePrefix: String,
      withMetric: (MetricName, MetricRegistry, ExecutorServiceMetrics),
  )(implicit loggingContext: LoggingContext): ResourceOwner[Executor] =
    ResourceOwner
      .forExecutorService { () =>
        val (executorName, metricRegistry, executorServiceMetrics) = withMetric
        InstrumentedExecutors.newFixedThreadPoolWithFactory(
          executorName,
          size,
          new ThreadFactoryBuilder()
            .setNameFormat(s"$namePrefix-%d")
            .build,
          metricRegistry,
          executorServiceMetrics,
          throwable =>
            ContextualizedLogger
              .get(this.getClass)
              .error(s"ExecutionContext $namePrefix has failed with an exception", throwable),
        )
      }
      .map(Executor.forExecutionContext)
}
