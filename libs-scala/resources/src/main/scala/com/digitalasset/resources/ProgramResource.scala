// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.{Executors, TimeUnit}

import com.digitalasset.logging.ContextualizedLogger
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.ProgramResource._

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.{NoStackTrace, NonFatal}
import scala.util.{Failure, Success}

class ProgramResource[T](
    owner: ResourceOwner[T],
    startupTimeout: FiniteDuration = 1.minute,
    tearDownDuration: FiniteDuration = 10.seconds,
) {
  private val logger = ContextualizedLogger.get(getClass)

  private val executorService = Executors.newCachedThreadPool()
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(executorService)

  def run(): Unit = {
    newLoggingContext { implicit logCtx =>
      val resource = owner.acquire()

      def stop(): Unit = {
        Await.result(resource.release(), tearDownDuration)
        executorService.shutdown()
        executorService.awaitTermination(tearDownDuration.toMillis, TimeUnit.MILLISECONDS)
        ()
      }

      resource.asFuture.onComplete {
        case Success(_) =>
          try {
            sys.runtime.addShutdownHook(new Thread(() => stop()))
          } catch {
            case NonFatal(exception) =>
              logger.error("Shutting down because of an initialization error.", exception)
              stop()
              sys.exit(1)
          }
        case Failure(exception: StartupException) =>
          logger.error(
            s"Shutting down because of an initialization error.\n${exception.getMessage}")
          stop()
          sys.exit(1)
        case Failure(_: SuppressedStartupException) =>
          stop()
          sys.exit(1)
        case Failure(NonFatal(exception)) =>
          logger.error("Shutting down because of an initialization error.", exception)
          stop()
          sys.exit(1)
      }
    }
  }
}

object ProgramResource {

  abstract class StartupException(message: String, cause: Throwable)
      extends RuntimeException
      with NoStackTrace {
    def this() = this(null, null)

    def this(message: String) = this(message, null)

    def this(cause: Throwable) = this(null, cause)
  }

  abstract class SuppressedStartupException(message: String, cause: Throwable)
      extends RuntimeException {
    def this() = this(null, null)

    def this(message: String) = this(message, null)

    def this(cause: Throwable) = this(null, cause)
  }
}
