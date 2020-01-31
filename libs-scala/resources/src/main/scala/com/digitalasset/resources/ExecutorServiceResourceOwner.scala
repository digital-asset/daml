// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.{ExecutorService, TimeUnit}

import com.digitalasset.resources.ExecutorServiceResourceOwner._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

class ExecutorServiceResourceOwner[T <: ExecutorService](acquireExecutorService: () => T)
    extends ResourceOwner[T] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[T] =
    Resource(
      Future {
        val executorService = acquireExecutorService()
        executionContext match {
          case context: ExecutionContextExecutorService =>
            if (executorService == context) {
              throw new CannotAcquireExecutionContext()
            }
            // Ugly, but important so that we make sure we're not going to end up in deadlock.
            try {
              val contextExecutor =
                context.getClass.getMethod("executor").invoke(context).asInstanceOf[ExecutorService]
              if (executorService == contextExecutor) {
                throw new CannotAcquireExecutionContext()
              }
            } catch {
              case _: NoSuchMethodException =>
            }
          case context: ExecutorService =>
            if (executorService == context) {
              throw new CannotAcquireExecutionContext()
            }
          case _ =>
        }
        executorService
      },
      executorService =>
        Future {
          executorService.shutdown()
          val _ = executorService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
      }
    )
}

object ExecutorServiceResourceOwner {

  class CannotAcquireExecutionContext
      extends RuntimeException(
        "The execution context used by resource acquisition cannot itself be acquired. This is to prevent deadlock upon release.")

}
