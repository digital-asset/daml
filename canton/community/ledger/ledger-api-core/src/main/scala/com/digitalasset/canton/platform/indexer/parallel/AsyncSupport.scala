// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
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
      executorName: String,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): ResourceOwner[Executor] = {
    val logger = loggerFactory.getTracedLogger(getClass)
    ResourceOwner
      .forExecutorService { () =>
        InstrumentedExecutors.newFixedThreadPoolWithFactory(
          executorName,
          size,
          new ThreadFactoryBuilder()
            .setNameFormat(s"$namePrefix-%d")
            .build,
          new ExecutorServiceMetrics(NoOpMetricsFactory),
          throwable =>
            logger
              .error(s"ExecutionContext $namePrefix has failed with an exception", throwable),
        )
      }
      .map(Executor.forExecutionContext)
  }
}
