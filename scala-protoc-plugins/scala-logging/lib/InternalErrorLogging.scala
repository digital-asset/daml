// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.logging

import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusException, StatusRuntimeException}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/** Provides facilities for logging unexpected results of gRPC calls */
trait InternalErrorLogging {

  protected def logger: Logger

  protected def loggingStreamObserver[T](
      streamObserver: StreamObserver[T]): ErrorLoggingStreamObserver[T] =
    ErrorLoggingStreamObserver(streamObserver, logger)

  protected def logTryIfInternal[T](tryValue: Try[T]): Try[T] = tryValue match {
    case Failure(exception) =>
      logIfInternal(exception)
      tryValue
    case success => success
  }

  protected def logFutureIfInternal[T](futureValue: Future[T]): Future[T] =
    futureValue.andThen({
      case t => logTryIfInternal(t)
    })(InternalErrorLogging.LoggingExecutionContext)

  protected def logIfInternal(t: Throwable): Unit = {
    try {
      t match {
        case s: StatusException =>
          if (isInternalError(s.getStatus.getCode)) logError(s)
        case s: StatusRuntimeException =>
          if (isInternalError(s.getStatus.getCode)) logError(s)
        case otherException =>
          logError(otherException)
      }
    } catch {
      case NonFatal(loggingException) => loggingException.addSuppressed(t)
    }
  }

  @inline
  private def logError(t: Throwable): Unit = {
    logger.error("Unhandled internal error.", t)
  }

  @inline
  private def isInternalError(code: Status.Code): Boolean =
    code == Status.Code.UNKNOWN || code == Status.Code.INTERNAL
}

object InternalErrorLogging {

  /** Used solely for logging throwables from failed Futures */
  private object LoggingExecutionContext extends ExecutionContext {

    private val logger = LoggerFactory.getLogger(this.getClass)

    override def execute(runnable: Runnable): Unit =
      runnable.run()

    override def reportFailure(cause: Throwable): Unit =
      logger.error("Unhandled exception while writing log statement", cause)
  }
}
