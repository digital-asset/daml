// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{BaseError, ContextualizedErrorLogger, ErrorCode}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException

/** Similar to `CantonError` but without dependencies on Canton internals. */
object SequencerBaseError {

  abstract class Impl(
      override val cause: String,
      override val throwableO: Option[Throwable] = None,
  )(implicit override val code: ErrorCode)
      extends BaseError
      with LogOnCreation

  def asGrpcError(error: BaseError)(implicit
      logger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    ErrorCode.asGrpcError(error)

  def stringFromContext(
      error: BaseError
  )(implicit loggingContext: ErrorLoggingContext, traceContext: TraceContext): String = {
    val contextMap = error.context ++ loggingContext.properties
    val errorCodeMsg = error.code.toMsg(error.cause, traceContext.traceId)
    if (contextMap.nonEmpty) {
      errorCodeMsg + "; " + ContextualizedErrorLogger.formatContextAsString(contextMap)
    } else {
      errorCodeMsg
    }
  }
}
