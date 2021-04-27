// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.concurrent.Executors

import com.codahale.metrics.{InstrumentedExecutorService, MetricRegistry}
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.MetricName

import scala.concurrent.{ExecutionContext, Future}

object AsyncSupport {

  trait Executor {
    def execute[FIN, FOUT](f: FIN => FOUT): FIN => Future[FOUT]
  }

  def asyncPool(
      size: Int,
      withMetric: Option[(MetricName, MetricRegistry)] = None,
  ): ResourceOwner[Executor] =
    ResourceOwner.forCloseable { () =>
      val executor = Executors.newFixedThreadPool(size)
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
