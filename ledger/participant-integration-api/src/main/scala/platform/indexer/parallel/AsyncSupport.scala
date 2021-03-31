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

    def execute[OUT](block: => OUT): Future[OUT]
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

        override def execute[OUT](block: => OUT): Future[OUT] = Future(block)(workerEC)

        override def close(): Unit = {
          workerEC.shutdownNow()
          ()
        }
      }
    }

  trait PooledResourceExecutor[RESOURCE] {
    def execute[FIN, FOUT](f: (FIN, RESOURCE) => FOUT): FIN => Future[FOUT]

    def execute[FIN, FOUT](f: RESOURCE => FOUT): Future[FOUT]
  }

  def asyncResourcePool[RESOURCE <: AutoCloseable](
      createResource: () => RESOURCE,
      size: Int,
      withMetric: Option[(MetricName, MetricRegistry)] = None,
  ): ResourceOwner[PooledResourceExecutor[RESOURCE]] =
    for {
      asyncPool <- asyncPool(size, withMetric)
      resourcePool <- ResourceOwner.forCloseable(() =>
        PostgresDAO.ResourcePool(createResource, size)
      )
    } yield new PooledResourceExecutor[RESOURCE] {
      override def execute[FIN, FOUT](f: (FIN, RESOURCE) => FOUT): FIN => Future[FOUT] =
        asyncPool.execute(in => resourcePool.borrow(resource => f(in, resource)))

      override def execute[FIN, FOUT](f: RESOURCE => FOUT): Future[FOUT] =
        asyncPool.execute(resourcePool.borrow(f))
    }
}
