// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.{ExecutorService, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}

class ExecutorServiceResourceOwner[T <: ExecutorService](acquireExecutorService: () => T)
    extends ResourceOwner[T] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[T] =
    Resource(
      Future(acquireExecutorService()),
      executorService =>
        Future {
          executorService.shutdown()
          val _ = executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
      }
    )
}
