// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error.{ContextualizedError, ContextualizedErrorLogger, ErrorCode}

import scala.jdk.CollectionConverters._

class DamlError(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
)(implicit
    override val code: ErrorCode,
    override val errorContext: ContextualizedErrorLogger,
) extends ContextualizedError {

  // Automatically log the error on generation
  errorContext.logError(this, Map())

  def rpcStatus(): com.google.rpc.status.Status = {
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
