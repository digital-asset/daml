// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error.{BaseError, ContextualizedErrorLogger, ErrorCode}
import com.google.rpc.Status
import io.grpc.StatusRuntimeException

import scala.jdk.CollectionConverters._

class DamlError(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
)(implicit
    override val code: ErrorCode,
    loggingContext: ContextualizedErrorLogger,
) extends BaseError {

  // Automatically log the error on generation
  loggingContext.logError(this, Map())

  def asGrpcStatus: Status =
    code.asGrpcStatus(this)(loggingContext)

  def asGrpcError: StatusRuntimeException =
    code.asGrpcError(this)(loggingContext)

  def rpcStatus(
  )(implicit loggingContext: ContextualizedErrorLogger): com.google.rpc.status.Status = {
    val status0: com.google.rpc.Status = code.asGrpcStatus(this)
    val details: Seq[com.google.protobuf.Any] = status0.getDetailsList.asScala.toSeq
    val detailsScalapb = details.map(com.google.protobuf.any.Any.fromJavaProto)
    com.google.rpc.status.Status(
      status0.getCode,
      status0.getMessage,
      detailsScalapb,
    )
  }

}

/** @param definiteAnswer Determines the value of the `definite_answer` key in the error details
  */
class DamlErrorWithDefiniteAnswer(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    val definiteAnswer: Boolean = false,
)(implicit
    override val code: ErrorCode,
    loggingContext: ContextualizedErrorLogger,
) extends DamlError(cause = cause, throwableO = throwableO) {

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)

}
