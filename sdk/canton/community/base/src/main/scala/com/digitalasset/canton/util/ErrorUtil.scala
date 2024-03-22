// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext

import java.io.{PrintWriter, StringWriter}
import scala.concurrent.Future
import scala.util.Failure
import scala.util.control.NonFatal

object ErrorUtil {

  /** Yields a string representation of a throwable (including stack trace and causes).
    */
  def messageWithStacktrace(t: Throwable): String = {
    val result = new StringWriter()
    t.printStackTrace(new PrintWriter(result))
    result.toString
  }

  /** Logs and rethrows any throwable.
    */
  def withThrowableLogging[T](action: => T, valueOnThrowable: Option[T] = None)(implicit
      loggingContext: ErrorLoggingContext
  ): T =
    try {
      action
    } catch {
      case t: Throwable =>
        loggingContext.logger.error("Unexpected exception", t)(loggingContext.traceContext)
        valueOnThrowable match {
          case Some(value) if NonFatal(t) => value
          case Some(_) | None => throw t
        }
    }

  val internalErrorMessage: String = "An internal error has occurred."

  private def logInternalError(t: Throwable)(implicit loggingContext: ErrorLoggingContext): Unit =
    loggingContext.logger.error(internalErrorMessage, t)(loggingContext.traceContext)

  /** Throws a throwable and logs it at ERROR level with proper formatting. */
  def internalError(t: Throwable)(implicit loggingContext: ErrorLoggingContext): Nothing = {
    logInternalError(t)
    throw t
  }

  /** Wraps a throwable in [[scala.util.Failure]] and logs it at ERROR level with proper formatting */
  def internalErrorTry(
      t: Throwable
  )(implicit loggingContext: ErrorLoggingContext): Failure[Nothing] = {
    logInternalError(t)
    Failure(t)
  }

  /** If `condition` is not satisfied, log an ERROR and throw an IllegalArgumentException
    * @throws java.lang.IllegalArgumentException
    */
  def requireArgument(condition: Boolean, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit =
    if (!condition) internalError(new IllegalArgumentException(message))

  /** If `condition` is not satisfied, log an ERROR and throw an IllegalStateException
    * @throws java.lang.IllegalStateException
    */
  def requireState(condition: Boolean, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit =
    if (!condition) invalidState(message)

  /** Indicate an illegal state by logging an ERROR and throw an IllegalStateException
    * @throws java.lang.IllegalStateException
    */
  def invalidState(message: => String)(implicit loggingContext: ErrorLoggingContext): Nothing =
    internalError(new IllegalStateException(message))

  /** Indicate an illegal state by logging an ERROR and return a IllegalStateException in a failed future.
    * @return The throwable in a failed future.
    */
  def invalidStateAsync(
      message: => String
  )(implicit loggingContext: ErrorLoggingContext): Future[Nothing] =
    internalErrorAsync(new IllegalStateException(message))

  /** Log a throwable at ERROR level with proper formatting.
    * @return The throwable in a failed future.
    */
  def internalErrorAsync(
      t: Throwable
  )(implicit loggingContext: ErrorLoggingContext): Future[Nothing] = {
    logInternalError(t)
    Future.failed(t)
  }

  /** Log a throwable at ERROR level with proper formatting.
    * @return The throwable in a failed [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]].
    */
  def internalErrorAsyncShutdown(
      t: Throwable
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[Nothing] = {
    logInternalError(t)
    FutureUnlessShutdown.failed(t)
  }

  /** If `condition` is not satisfied, log an ERROR and return a failed future with an [[java.lang.IllegalArgumentException]]
    */
  def requireArgumentAsync(condition: Boolean, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): Future[Unit] =
    if (condition) Future.unit else internalErrorAsync(new IllegalArgumentException(message))

  /** If `condition` is not satisfied, log an ERROR and return a failed future with an [[java.lang.IllegalStateException]]
    */
  def requireStateAsync(condition: Boolean, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): Future[Unit] =
    if (condition) Future.unit else internalErrorAsync(new IllegalStateException(message))

  /** If `condition` is not satisfied, log an ERROR and return a failed FutureUnlessShutdown with an [[java.lang.IllegalStateException]]
    */
  def requireStateAsyncShutdown(condition: Boolean, message: => String)(implicit
      loggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[Unit] =
    if (condition) FutureUnlessShutdown.unit
    else internalErrorAsyncShutdown(new IllegalStateException(message))

}
