// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.ErrorCode.truncateResourceForTransport
import com.daml.error.{BaseError, ErrorCode, ErrorCodeUtils}
import com.daml.ledger.participant.state.v2.Update.CommandRejected.{
  FinalReason,
  RejectionReasonTemplate,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.google.rpc.status.{Status => RpcStatus}

trait TransactionError extends BaseError {
  def createRejection(
      correlationId: Option[String]
  )(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): RejectionReasonTemplate = {
    FinalReason(rpcStatus(correlationId))
  }

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)

  def rpcStatus(
      correlationId: Option[String]
  )(implicit logger: ContextualizedLogger, loggingContext: LoggingContext): RpcStatus = {

    // yes, this is a horrible duplication of ErrorCode.asGrpcError. why? because
    // scalapb does not really support grpc rich errors. there is literally no method
    // that supports turning scala com.google.rpc.status.Status into java com.google.rpc.Status
    // objects. however, the sync-api uses the scala variant whereas we have to return StatusRuntimeExceptions.
    // therefore, we have to compose the status code a second time here ...
    // the ideal fix would be to extend scalapb accordingly ...
    val ErrorCode.StatusInfo(codeGrpc, messageWithoutContext, contextMap, _) =
      ErrorCodeUtils.getStatusInfo(this, correlationId, logger)(loggingContext)

    // TODO error codes: avoid appending the context to the description. right now, we need to do that as the ledger api server is throwing away any error details
    val message =
      if (code.category.securitySensitive) messageWithoutContext
      else messageWithoutContext + "; " + ErrorCodeUtils.formatContextAsString(contextMap)

    val definiteAnswerKey =
      "definite_answer" // TODO error codes: Can we use a constant from some upstream class?

    val metadata = if (code.category.securitySensitive) Map.empty[String, String] else contextMap
    val errorInfo = com.google.rpc.error_details.ErrorInfo(
      reason = code.id,
      metadata = metadata.updated(definiteAnswerKey, definiteAnswer.toString),
    )

    val retryInfoO = retryable.map { ri =>
      val dr = com.google.protobuf.duration.Duration(
        java.time.Duration.ofMillis(ri.duration.toMillis)
      )
      com.google.protobuf.any.Any.pack(com.google.rpc.error_details.RetryInfo(Some(dr)))
    }

    val requestInfoO = correlationId.map { ci =>
      com.google.protobuf.any.Any.pack(com.google.rpc.error_details.RequestInfo(requestId = ci))
    }

    val resourceInfos =
      if (code.category.securitySensitive) Seq()
      else
        truncateResourceForTransport(resources).map { case (rs, item) =>
          com.google.protobuf.any.Any
            .pack(
              com.google.rpc.error_details
                .ResourceInfo(resourceType = rs.asString, resourceName = item)
            )
        }

    val details = Seq[com.google.protobuf.any.Any](
      com.google.protobuf.any.Any.pack(errorInfo)
    ) ++ retryInfoO.toList ++ requestInfoO.toList ++ resourceInfos

    com.google.rpc.status.Status(
      codeGrpc.value(),
      message,
      details,
    )
  }
}

abstract class TransactionErrorImpl(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    override val definiteAnswer: Boolean = false,
)(implicit override val code: ErrorCode)
    extends TransactionError
