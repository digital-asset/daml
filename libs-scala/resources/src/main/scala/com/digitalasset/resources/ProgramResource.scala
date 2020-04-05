// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.{Executors, TimeUnit}

import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.resources.ProgramResource._

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.{NoStackTrace, NonFatal}
import scala.util.{Failure, Success, Try}

class ProgramResource[T](
    owner: => ResourceOwner[T],
    startupTimeout: FiniteDuration = 1.minute,
    tearDownDuration: FiniteDuration = 10.seconds,
) {
  private val logger = ContextualizedLogger.get(getClass)

  private val executorService = Executors.newCachedThreadPool()
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(executorService)

  def run(): Unit = {
    newLoggingContext { implicit logCtx =>
      val resource = Try(owner.acquire()).fold(Resource.failed, identity)

      def stop(): Unit = {
        Await.result(resource.release(), tearDownDuration)
        executorService.shutdown()
        executorService.awaitTermination(tearDownDuration.toMillis, TimeUnit.MILLISECONDS)
        ()
      }

      val acquisition =
        Await.result(resource.asFuture.transformWith(Future.successful), startupTimeout)

      acquisition match {
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

  trait StartupException extends NoStackTrace {
    self: Exception =>
  }

  trait SuppressedStartupException {
    self: Exception =>
  }
}
