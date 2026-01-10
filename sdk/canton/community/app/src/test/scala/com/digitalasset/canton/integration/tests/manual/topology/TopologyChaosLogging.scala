// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.manual.topology.TopologyOperations.isCi
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Pause, Success}
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[topology] trait TopologyChaosLogging {

  protected def logger: TracedLogger

  /** Logging wrapper for "top-level" operations with more-verbose-than-usual logging.
    * @param operation
    *   the name of the operation (without parameters, e.g. "add mediator")
    * @param message
    *   message providing specifics such as parameters, e.g. "mediator1 to group 0"
    * @param expr
    *   operation to wrap
    */
  protected def withOperation_(
      operation: String
  )(message: String)(
      expr: => Unit
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit =
    clue(operation, verbose = true, callerLogsThrowable = true)(message)(
      try expr
      catch {
        case NonFatal(e) =>
          LoggerUtil.logThrowableAtLevel(Level.WARN, fullMessage(operation, message), e)
      }
    )

  // Optional: time of initialization. Allows to report time progress
  protected lazy val initializationTime: AtomicReference[Option[CantonTimestamp]] =
    new AtomicReference[Option[CantonTimestamp]](None)

  def logOperationStep(
      operation: String
  )(
      message: String,
      level: Level = Level.INFO,
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit = {
    val logMessage = fullMessage(operation, message)
    if (!isCi && level == Level.INFO) println(logMessage)
    LoggerUtil.logAtLevel(level, logMessage)
  }

  /** Logging wrapper for granular operations, typically used "nested".
    * @param operation
    *   the name of the operation (without parameters, e.g. "add mediator")
    * @param verbose
    *   should generally be set to false (except for call by withOperation_ above)
    * @param callerLogsThrowable
    *   whether to disable logging of exception, useful to avoid double-logging or logging of
    *   expected exceptions as info by caller
    * @param message
    *   message providing specifics such as parameters, e.g. "mediator1 to group 0"
    * @param expr
    *   operation to wrap
    */
  protected def clue[T](
      operation: String,
      verbose: Boolean = false,
      callerLogsThrowable: Boolean = false,
  )(message: String)(
      expr: => T
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): T = {
    val clueMessage = fullMessage(operation, message)
    if (!isCi && verbose) println(clueMessage)
    LoggerUtil.clue(clueMessage, logNonFatalThrowable = !callerLogsThrowable)(expr)
  }

  /** Logging wrapper for granular operations, typically used "nested".
    * @param operation
    *   the name of the operation (without parameters, e.g. "add mediator")
    * @param verbose
    *   should generally be set to false (except for call by withOperation_ above)
    * @param callerLogsThrowable
    *   whether to disable logging of exception, useful to avoid double-logging or logging of
    *   expected exceptions as info by caller
    * @param message
    *   message providing specifics such as parameters, e.g. "mediator1 to group 0"
    * @param expr
    *   operation to wrap
    */
  protected def clueF[T](
      operation: String,
      verbose: Boolean = false,
      callerLogsThrowable: Boolean = false,
  )(message: String)(
      expr: => Future[T]
  )(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Future[T] = {
    import env.executionContext
    val clueMessage = fullMessage(operation, message)
    if (!isCi && verbose) println(clueMessage)
    LoggerUtil.clueF(clueMessage, logNonFatalThrowable = !callerLogsThrowable)(expr)
  }

  /** A convenience method that combines the logging of clueF and the retry mechanism of Pause. This
    * helps to avoid that the nesting levels become too unwieldy.
    * @param operation
    *   the name of the operation/topology change. Should be the same for a specific type of change
    *   (eg. "add sequencer ${sequencer.name}")
    * @param message
    *   the message describing a specific action
    * @param maxRetries
    *   maximum number of retries for the Pause retry mechanism
    * @param delay
    *   the delay for the Pause retry mechanism
    * @param action
    *   the actual action being performed and possibly retried
    * @return
    */
  def retryWithClue[T](
      operation: String,
      message: String,
      maxRetries: Int,
      delay: FiniteDuration,
  )(
      action: => Future[T]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      success: Success[T],
      environment: TestConsoleEnvironment,
      closeContext: CloseContext,
      executionContext: ExecutionContext,
  ): Future[T] = {
    val retryCounter = new AtomicInteger(0)
    Pause(
      errorLoggingContext.logger,
      closeContext.context,
      maxRetries,
      delay,
      fullMessage(operation, message),
    )(
      clueF(operation)(s"$message, attempt #${retryCounter.incrementAndGet()}")(action),
      AllExceptionRetryPolicy,
    )
  }

  /** Helper to log non-fatal exceptions as info log message so that topology proposals expected to
    * time out or error (e.g. ALREADY_EXISTS/TOPOLOGY_MAPPING_ALREADY_EXISTS) due to conflicts with
    * concurrent topology transactions don't fail the test and can be retried.
    */
  protected def catchTopologyExceptionsAndLogAsInfo(
      operation: String,
      message: String,
      onFailure: String => Unit,
  )(
      expr: => Unit
  )(implicit loggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit =
    try {
      clue(operation, callerLogsThrowable = true)(message)(
        expr
      )
    } catch {
      case NonFatal(e) =>
        val msg =
          s"${fullMessage(operation, message)} produced exception ${e.getMessage}"
        logger.info(msg)
        onFailure(msg)
    }

  protected def fullMessage(operation: String, message: String)(implicit
      env: TestConsoleEnvironment
  ): String = {
    val timeProgress = getRelativeTime()

    s"$timeProgress[chaos testing][$operation] $message"
  }

  /** Get relative time as a string.
    * @return
    *   [t=43.123] if initializationTime is defined, empty string otherwise.
    */
  protected def getRelativeTime()(implicit
      env: TestConsoleEnvironment
  ): String = initializationTime
    .get()
    .map(env.environment.clock.now - _)
    .map(_.toMillis.toDouble)
    .map(d => s"[t=${d / 1000}s]")
    .getOrElse("")

  implicit protected def toTraceContext(implicit
      errorLoggingContext: ErrorLoggingContext
  ): TraceContext = errorLoggingContext.traceContext

}
