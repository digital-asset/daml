// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.retry.ErrorKind.*
import org.slf4j.event.Level

import java.sql.SQLException
import scala.util.{Failure, Try}

/** When using retry code in different contexts, different exceptions should be retried on. This trait provides a
  * way to define what exceptions should be retried and which are fatal.
  */
trait ExceptionRetryPolicy {

  /** Classify the error kind for a given exception */
  protected def determineExceptionErrorKind(exception: Throwable, logger: TracedLogger)(implicit
      tc: TraceContext
  ): ErrorKind

  /** Determines what kind of error resulted in the outcome,
    * and gives a recommendation on how many times to retry.
    *
    * Also logs the embedded exception.
    */
  def logAndDetermineErrorKind(
      outcome: Try[_],
      logger: TracedLogger,
      lastErrorKind: Option[ErrorKind],
  )(implicit
      tc: TraceContext
  ): ErrorKind = {
    outcome match {
      case util.Success(_) => NoSuccessErrorKind
      case Failure(exception) =>
        val errorKind = determineExceptionErrorKind(exception, logger)
        // only log the full exception if the error kind changed such that we avoid spamming the logs
        if (!lastErrorKind.contains(errorKind)) {
          logThrowable(exception, logger)
        } else {
          logger.debug(
            s"Retrying on same error kind ${errorKind} for ${exception.getClass.getSimpleName}/${exception.getMessage}"
          )
        }
        errorKind
    }
  }

  protected def logThrowable(e: Throwable, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Unit = {
    val level = retryLogLevel(e).getOrElse(Level.INFO)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    e match {
      case sqlE: SQLException =>
        // Unfortunately, the sql state and error code won't get logged automatically.
        LoggerUtil.logThrowableAtLevel(
          level,
          s"Detected an SQLException. SQL state: ${sqlE.getSQLState}, error code: ${sqlE.getErrorCode}",
          e,
        )
      case _: Throwable =>
        LoggerUtil.logThrowableAtLevel(level, s"Detected an error.", e)
    }
  }

  /** Return an optional log level to log an exception with.
    *
    * This allows to override the log level for particular exceptions on retry globally.
    */
  def retryLogLevel(e: Throwable): Option[Level] = None

  def retryLogLevel(outcome: Try[Any]): Option[Level] = outcome match {
    case Failure(exception) => retryLogLevel(exception)
    case util.Success(_value) => None
  }
}

sealed trait ErrorKind {
  def maxRetries: Int
}

object ErrorKind {

  /** The outcome of the future was success, but the success predicate was false, we retry indefinitely */
  case object NoSuccessErrorKind extends ErrorKind {
    override val maxRetries: Int = Int.MaxValue

    override def toString: String = "no success error (request infinite retries)"
  }

  /** We don't classify the kind of error, so we default to infinite retries */
  case object UnknownErrorKind extends ErrorKind {
    override val maxRetries: Int = Int.MaxValue

    override def toString: String = "unknown error (request infinite retries)"
  }

  /** A fatal error that we should not retry on */
  case object FatalErrorKind extends ErrorKind {
    override val maxRetries = 0

    override def toString: String = "fatal error (give up immediately)"
  }

  /** Main use case is a network outage. Infinite retries are needed, as we don't know how long the outage takes.
    */
  final case class TransientErrorKind(maxRetries: Int = Int.MaxValue) extends ErrorKind {
    private lazy val numRetriesString =
      if (maxRetries == Int.MaxValue) "infinite" else maxRetries.toString

    override def toString: String = s"transient error (request $numRetriesString retries)"
  }

}

/** Retry on any exception.
  *
  * This is a sensible default choice for non-db tasks with a finite maximum number of retries.
  */
case object AllExceptionRetryPolicy extends ExceptionRetryPolicy {

  override protected def determineExceptionErrorKind(exception: Throwable, logger: TracedLogger)(
      implicit tc: TraceContext
  ): ErrorKind = {
    // We don't classify the kind of error, always retry indefinitely
    UnknownErrorKind
  }

}

/** Don't retry on any exception.
  */
case object NoExceptionRetryPolicy extends ExceptionRetryPolicy {

  override protected def determineExceptionErrorKind(exception: Throwable, logger: TracedLogger)(
      implicit tc: TraceContext
  ): ErrorKind = {
    // We treat the exception as fatal, never retry
    FatalErrorKind
  }

}
