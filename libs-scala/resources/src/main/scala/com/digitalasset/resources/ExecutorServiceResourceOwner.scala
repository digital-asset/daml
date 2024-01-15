// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.{ExecutorService, TimeUnit}

import com.daml.resources.ExecutorServiceResourceOwner._

import scala.concurrent.{ExecutionContextExecutorService, Future}

class ExecutorServiceResourceOwner[Context: HasExecutionContext, T <: ExecutorService](
    acquireExecutorService: () => T,
    gracefulAwaitTerminationMillis: Long,
    forcefulAwaitTerminationMillis: Long,
) extends AbstractResourceOwner[Context, T] {
  override def acquire()(implicit context: Context): Resource[Context, T] =
    ReleasableResource(Future {
      val executorService = acquireExecutorService()
      // If we try and release an executor service which is itself being used to power the
      // releasing, we end up in a deadlock—the executor can't shut down, and therefore
      // `awaitTermination `never completes. We mitigate this by attempting to catch the problem
      // early and fail with a meaningful exception.
      executionContext match {
        case context: ExecutionContextExecutorService =>
          if (executorService == context) {
            throw new CannotAcquireExecutionContext()
          }
          // Ugly, but important so that we make sure we're not going to end up in deadlock.
          // This calls a method on the private `ExecutionContextExecutorServiceImpl` class to get
          // the underlying executor, then compares it. If you're using a custom
          // `ExecutionContextExecutorService`, it probably won't follow the same API, so this
          // will silently ignore it. This means it'll acquire happily but deadlock on release.
          // We can't cover everything in these checks, unfortunately.
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
    })(executorService =>
      Future {
        executorService.shutdown()
        val gracefulShutdownSuccessful =
          executorService.awaitTermination(gracefulAwaitTerminationMillis, TimeUnit.MILLISECONDS)
        if (!gracefulShutdownSuccessful) {
          executorService.shutdownNow()
          val _ =
            executorService.awaitTermination(forcefulAwaitTerminationMillis, TimeUnit.MILLISECONDS)
        }
      }
    )
}

object ExecutorServiceResourceOwner {

  class CannotAcquireExecutionContext
      extends RuntimeException(
        "The execution context used by resource acquisition cannot itself be acquired. This is to prevent deadlock upon release."
      )

}
