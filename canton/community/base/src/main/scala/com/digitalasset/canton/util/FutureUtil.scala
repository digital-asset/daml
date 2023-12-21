// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import org.slf4j.event.Level

import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.*
import scala.util.Try
import scala.util.control.NonFatal

object FutureUtil {

  /** If the future fails, log the associated error and re-throw. The returned future completes after logging.
    */
  def logOnFailure[T](
      future: Future[T],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Future[T] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.noTracingLogger)
    future.recover {
      // Catching NonFatal only, because a future cannot fail with fatal throwables.
      // Also, it may be a bad idea to run a callback after an OutOfMemoryError.
      case NonFatal(err) =>
        // if the optional close context is closing down, log at most with INFO
        if (closeContext.exists(_.context.isClosing) && level > Level.INFO) {
          LoggerUtil.logThrowableAtLevel(
            Level.INFO,
            s"Logging the following failure on INFO instead of $level due to an ongoing shutdown: $failureMessage",
            err,
          )
        } else {
          LoggerUtil.logThrowableAtLevel(level, failureMessage, err)
        }
        try {
          onFailure(err)
        } catch {
          case t: Throwable => // Catching all throwables, because we are merely logging.
            // Always log at ERROR independent of `level` because we don't expect `onFailure` to throw.
            loggingContext.logger.error(
              "An unexpected exception occurred while handling a failed future.",
              t,
            )(loggingContext.traceContext)
            t.addSuppressed(err)
            throw t
        }
        throw err
    }
  }

  /** If the future fails, log the associated error and re-throw. The returned future completes after logging.
    */
  def logOnFailureUnlessShutdown[T](
      future: FutureUnlessShutdown[T],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] = {
    FutureUnlessShutdown(
      logOnFailure(future.unwrap, failureMessage, onFailure, level, closeContext)
    )
  }

  /** Discard `future` and log an error if it does not complete successfully.
    * This is useful to document that a `Future` is intentionally not being awaited upon.
    */
  def doNotAwait(
      future: Future[?],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    val _ = logOnFailure(future, failureMessage, onFailure, level, closeContext)
  }

  /** [[doNotAwait]] but for FUS
    */
  def doNotAwaitUnlessShutdown(
      future: FutureUnlessShutdown[?],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    doNotAwait(future.unwrap, failureMessage, onFailure, level, closeContext)
  }

  /** Variant of [[doNotAwait]] that also catches non-fatal errors thrown while constructing the future. */
  def catchAndDoNotAwait(
      future: => Future[?],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    val wrappedFuture = Future.fromTry(Try(future)).flatten
    doNotAwait(wrappedFuture, failureMessage, onFailure, level)
  }

  lazy val defaultStackTraceFilter: Thread => Boolean = {
    // Include threads directly used by Canton (incl. tests).
    // Excludes threads used by the ledger api server, grpc, ...
    val patterns = Seq(
      ".*-env-execution-context.*",
      ".*-test-execution-context.*",
      ".*-env-scheduler.*",
      ".*-test-execution-context-monitor.*",
      ".*-wallclock.*",
      ".*-remoteclock.*",
      ".*delay-util.*",
      ".*-ccf-execution-context.*",
      ".*-fabric-sequencer-execution-context.*",
      ".*-db-execution-context.*",
      "ScalaTest-run.*",
    )

    // Take the disjunction of patterns.
    val isRelevant = Pattern
      .compile(patterns.map(p => s"($p)").mkString("|"))
      .asMatchPredicate()
    thread => isRelevant.test(thread.getName)
  }
}
