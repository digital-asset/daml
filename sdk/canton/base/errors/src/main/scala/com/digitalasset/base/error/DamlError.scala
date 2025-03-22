// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

import com.google.rpc.Status
import io.grpc.StatusRuntimeException

abstract class ContextualizedDamlError(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    extraContext: Map[String, Any] = Map(),
)(implicit
    override val code: ErrorCode,
    val errorContext: ContextualizedErrorLogger,
) extends BaseError
    with DamlRpcError {

  def asGrpcStatus: Status =
    ErrorCode.asGrpcStatus(this)(errorContext)

  def asGrpcError: StatusRuntimeException =
    ErrorCode.asGrpcError(this)(errorContext)

  // Automatically log the error on generation
  errorContext.logError(this, Map())

  override def context: Map[String, String] =
    super.context ++ extraContext.view.mapValues(_.toString)

  def correlationId: Option[String] = errorContext.correlationId

  def traceId: Option[String] = errorContext.traceId
}

/** @param definiteAnswer
  *   Determines the value of the `definite_answer` key in the error details
  */
class DamlErrorWithDefiniteAnswer(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    val definiteAnswer: Boolean = false,
    extraContext: Map[String, Any] = Map(),
)(implicit
    override val code: ErrorCode,
    loggingContext: ContextualizedErrorLogger,
) extends ContextualizedDamlError(
      cause = cause,
      throwableO = throwableO,
      extraContext = extraContext,
    ) {

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)

}
