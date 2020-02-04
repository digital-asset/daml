// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import com.digitalasset.logging.ContextualizedLogger
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.ProgramResource._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal

class ProgramResource[T](owner: ResourceOwner[T]) {
  private val logger = ContextualizedLogger.get(getClass)

  def run()(implicit executionContext: ExecutionContext): Resource[T] = {
    newLoggingContext { implicit logCtx =>
      val resource = owner.acquire()

      resource.asFuture.failed.foreach {
        case _: SuppressedException =>
          System.exit(1)
        case exception =>
          logger.error("Shutting down because of an initialization error.", exception)
          System.exit(1)
      }

      try {
        sys.runtime.addShutdownHook(new Thread(() =>
          Await.result(resource.release(), AsyncTimeout)))
      } catch {
        case NonFatal(exception) =>
          logger.error("Shutting down because of an initialization error.", exception)
          Await.result(resource.release(), AsyncTimeout)
          System.exit(1)
      }

      resource
    }
  }
}

object ProgramResource {
  private val AsyncTimeout = 10.seconds

  abstract class SuppressedException extends RuntimeException
}
