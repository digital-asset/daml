// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.concurrent.Executors

import com.codahale.metrics.{InstrumentedExecutorService, MetricRegistry}
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.MetricName
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, Future}

// TODO append-only: add unit tests and move to some scala/concurrent library project for convenience
object AsyncSupport {

  trait Executor {
    def execute[FIN, FOUT](f: FIN => FOUT): FIN => Future[FOUT]
  }

  def asyncPool(
      size: Int,
      namePrefix: String,
      withMetric: Option[(MetricName, MetricRegistry)] = None,
  ): ResourceOwner[Executor] =
    ResourceOwner.forCloseable { () =>
      val executor = Executors.newFixedThreadPool(
        size,
        new ThreadFactoryBuilder().setNameFormat(s"$namePrefix-%d").build,
      )
      val workerE = withMetric match {
        case Some((metricName, metricRegistry)) =>
          new InstrumentedExecutorService(
            executor,
            metricRegistry,
            metricName,
          )
        case None => executor
      }
      val workerEC = ExecutionContext.fromExecutorService(workerE)
      new Executor with AutoCloseable {
        override def execute[FIN, FOUT](f: FIN => FOUT): FIN => Future[FOUT] =
          in => Future(f(in))(workerEC)

        override def close(): Unit = {
          workerEC.shutdownNow()
          ()
        }
      }
    }
}
