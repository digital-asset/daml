// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.resources.ProgramResource._

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.control.{NoStackTrace, NonFatal}

final class ProgramResource[Context: HasExecutionContext, T](
    owner: => AbstractResourceOwner[Context, T],
    tearDownTimeout: FiniteDuration = 10.seconds,
) {
  private val logger = ContextualizedLogger.get(getClass)

  private val executorService = {
    val counter = new AtomicLong(0L)
    Executors.newCachedThreadPool((runnable: Runnable) =>
      new Thread(runnable, s"program-resource-pool-${counter.incrementAndGet()}")
    )
  }

  def run(newContext: ExecutionContext => Context): Unit = {
    newLoggingContext { implicit loggingContext =>
      val resource = {
        implicit val context: Context = newContext(
          ExecutionContext.fromExecutor(
            executorService,
            throwable =>
              LoggingContext.newLoggingContext { implicit loggingContext =>
                logger.error("ExecutionContext has failed with an exception", throwable)
              },
          )
        )
        Try(owner.acquire()).fold(exception => PureResource(Future.failed(exception)), identity)
      }

      def stop(): Unit = {
        Await.result(resource.release(), tearDownTimeout)
        executorService.shutdown()
        executorService.awaitTermination(tearDownTimeout.toMillis, TimeUnit.MILLISECONDS)
        ()
      }

      sys.runtime.addShutdownHook(new Thread(() => {
        try {
          stop()
        } catch {
          case NonFatal(exception) =>
            logger.error("Failed to stop successfully.", exception)
        }
      }))

      // On failure, shut down immediately.
      resource.asFuture.failed.foreach { exception =>
        exception match {
          // The error is suppressed; we don't need to print anything more.
          case _: SuppressedStartupException =>
          case _: StartupException =>
            logger.error(
              s"Shutting down because of an initialization error.\n${exception.getMessage}"
            )
          case NonFatal(_) =>
            logger.error("Shutting down because of an initialization error.", exception)
        }
        sys.exit(1) // `stop` will be triggered by the shutdown hook.
      }(ExecutionContext.global) // Run on the global execution context to avoid deadlock.
    }
  }
}

object ProgramResource {

  trait StartupException extends NoStackTrace {
    self: Exception =>
  }

  trait SuppressedStartupException {
    self: Exception =>
  }

}
