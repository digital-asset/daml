// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.poc

import java.util.concurrent.Executors

import com.daml.ledger.resources.ResourceOwner

import scala.concurrent.{ExecutionContext, Future}

object AsyncSupport {

  implicit class WithAspect[F](val f: F) extends AnyVal {
    def withAspect(aspect: F => F): F = aspect(f)
  }

  trait Executor {
    def execute[FIN, FOUT](f: FIN => FOUT): FIN => Future[FOUT]

    def execute[OUT](block: => OUT): Future[OUT]
  }

  def asyncPool(size: Int): ResourceOwner[Executor] =
    ResourceOwner.forCloseable { () =>
      val workerE = Executors.newFixedThreadPool(size)
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
  ): ResourceOwner[PooledResourceExecutor[RESOURCE]] =
    for {
      asyncPool <- asyncPool(size)
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
