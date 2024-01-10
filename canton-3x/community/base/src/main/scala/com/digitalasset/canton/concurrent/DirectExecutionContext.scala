// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.typesafe.scalalogging.Logger

import scala.concurrent.{BatchingExecutorCanton, ExecutionContextExecutor}

/** A light-weight execution context that runs tasks on the thread calling `execute`.
  * Only use this for running tasks that will terminate very quickly.
  */
final case class DirectExecutionContext(logger: Logger)
    extends ExecutionContextExecutor
    with BatchingExecutorCanton {

  private val reporter: Throwable => Unit =
    Threading.createReporter(getClass.getSimpleName, logger, exitOnFatal = true)

  override def submitForExecution(runnable: Runnable): Unit = runnable.run()

  override def execute(runnable: Runnable): Unit = submitSyncBatched(runnable)

  override def reportFailure(cause: Throwable): Unit = {
    reporter(cause)
    // Do not rethrow cause.
    // If this method throws an exception, the exception would ultimately be reported by a different EC,
    // but this leads to a messy repetition of error messages in the log file.
  }
}
object DirectExecutionContext {
  def apply(tracedLogger: TracedLogger): DirectExecutionContext =
    DirectExecutionContext(NamedLogging.loggerWithoutTracing(tracedLogger))
}
