// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.grpc.GrpcException
import com.daml.logging.LoggingContext
import com.daml.logging.entries.LoggingEntries
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object TracedLoggerOps {
  implicit class TracedLoggerOps(val logger: TracedLogger) extends AnyVal {
    def logErrorsOnCall[Out](implicit traceContext: TraceContext): PartialFunction[Try[Out], Unit] =
      logErrorsOnCallImpl(logger)

    def logErrorsOnStream[Out](implicit traceContext: TraceContext): Flow[Out, Out, NotUsed] =
      logErrorsOnStreamImpl(logger)

    def enrichedDebugStream[Out](
        msg: String,
        withContext: Out => LoggingEntries,
    )(implicit traceContext: TraceContext): Flow[Out, Out, NotUsed] =
      enrichedDebugStreamImpl[Out](logger)(msg, withContext)

  }

  private def internalOrUnknown(code: Status.Code): Boolean =
    code == Status.Code.INTERNAL || code == Status.Code.UNKNOWN

  private def logError(logger: TracedLogger, t: Throwable)(implicit
      traceContext: TraceContext
  ): Unit =
    logger.error("Unhandled internal error", t)

  private def logErrorsOnStreamImpl[Out](
      logger: TracedLogger
  )(implicit traceContext: TraceContext): Flow[Out, Out, NotUsed] =
    Flow[Out].mapError {
      case e @ GrpcException(s, _) =>
        if (internalOrUnknown(s.getCode)) {
          logError(logger, e)
        }
        e
      case NonFatal(e) =>
        logError(logger, e)
        e
    }

  private def logErrorsOnCallImpl[Out](logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): PartialFunction[Try[Out], Unit] = {
    case Failure(e @ GrpcException(s, _)) =>
      if (internalOrUnknown(s.getCode)) {
        logError(logger, e)
      }
    case Failure(NonFatal(e)) =>
      logError(logger, e)
  }

  private def enrichedDebugStreamImpl[Out](logger: TracedLogger)(
      msg: String,
      withContext: Out => LoggingEntries,
  )(implicit traceContext: TraceContext): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      LoggingContext.withEnrichedLoggingContextFrom(withContext(item)) {
        implicit enrichedLoggingContext =>
          logger.debug(s"$msg, ${enrichedLoggingContext.makeString}")(traceContext)
          item
      }(LoggingContext.empty)
    }

}
