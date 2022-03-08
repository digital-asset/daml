// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.concurrent.Executors

import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
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
//      withMetric: Option[(MetricName, MetricRegistry)] = None,
  )(implicit loggingContext: LoggingContext): ResourceOwner[Executor] =
    ResourceOwner
      .forExecutorService(() =>
        ExecutionContext.fromExecutorService(
          {
            val executor = Executors.newFixedThreadPool(
              size,
              new ThreadFactoryBuilder()
                .setNameFormat(s"$namePrefix-%d")
                .build,
            )
            executor
//            withMetric match {
//              case Some((metricName, metricRegistry)) =>
            // TODO Prometheus metrics: implement
//                new InstrumentedExecutorService(
//                  executor,
//                  metricRegistry,
//                  metricName,
//                )
//                executor
//
//              case None => executor
//            }
          },
          throwable =>
            ContextualizedLogger
              .get(this.getClass)
              .error(s"ExecutionContext $namePrefix has failed with an exception", throwable),
        )
      )
      .map(Executor.forExecutionContext)
}
