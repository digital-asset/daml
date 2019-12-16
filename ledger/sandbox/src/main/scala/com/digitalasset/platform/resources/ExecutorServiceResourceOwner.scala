// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.util.concurrent.{ExecutorService, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}

class ExecutorServiceResourceOwner[T <: ExecutorService](acquireExecutorService: () => T)
    extends ResourceOwner[T] {
  override def acquire()(implicit _executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      override protected val executionContext: ExecutionContext = _executionContext

      private val executorService: T = acquireExecutorService()

      override protected val future: Future[T] = Future.successful(executorService)

      override def releaseResource(): Future[Unit] = Future {
        executorService.shutdown()
        val _ = executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
      }
    }
}
