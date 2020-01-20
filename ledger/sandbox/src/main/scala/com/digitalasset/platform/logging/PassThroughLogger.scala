// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.logging

import akka.stream.scaladsl.Source
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.GrpcException
import io.grpc.Status

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.control.NonFatal

private[platform] object PassThroughLogger {

  def wrap(logger: ContextualizedLogger) = new PassThroughLogger(logger)
  def get(service: Class[_]) = new PassThroughLogger(ContextualizedLogger.get(service.getClass))

}

private[platform] final class PassThroughLogger private (logger: ContextualizedLogger) {

  @inline private def internalOrUnknown(code: Status.Code): Boolean =
    code == Status.Code.INTERNAL || code == Status.Code.UNKNOWN

  def apply[A](f: => Future[A])(implicit logCtx: LoggingContext): Future[A] =
    f.andThen {
      case Failure(e @ GrpcException(status, _)) =>
        // This is purposefully not a guard: the intent is to skip logging
        // of any gRPC error which is neither internal or unknown
        if (internalOrUnknown(status.getCode))
          logger.error("Unhandled internal error", e)
      case Failure(NonFatal(e)) =>
        logger.error("Unhandled internal error", e)
    }(DirectExecutionContext)

  def apply[Out, Mat](f: => Source[Out, Mat])(implicit logCtx: LoggingContext): Source[Out, Mat] =
    f.mapError {
      case e @ GrpcException(status, _) =>
        // This is purposefully not a guard: the intent is to skip logging
        // of any gRPC error which is neither internal or unknown
        if (internalOrUnknown(status.getCode))
          logger.error("Unhandled internal error", e)
        e
      case NonFatal(e) =>
        logger.error("Unhandled internal error", e)
        e
    }

}
