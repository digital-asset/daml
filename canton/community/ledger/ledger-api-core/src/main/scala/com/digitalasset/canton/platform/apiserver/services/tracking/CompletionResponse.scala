// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.Checkpoint as PbCheckpoint
import com.daml.ledger.api.v2.completion.Completion as PbCompletion
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors
import com.google.rpc.status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import scala.util.{Failure, Success, Try}

final case class CompletionResponse(checkpoint: Option[PbCheckpoint], completion: PbCompletion)

object CompletionResponse {
  def fromCompletion(
      errorLogger: ContextualizedErrorLogger,
      completion: PbCompletion,
      checkpoint: Option[PbCheckpoint],
  ): Try[CompletionResponse] =
    completion.status
      .toRight(missingStatusError(errorLogger))
      .toTry
      .flatMap {
        case status if status.code == 0 =>
          Success(CompletionResponse(checkpoint, completion))
        case nonZeroStatus =>
          Failure(
            StatusProto.toStatusRuntimeException(
              status.Status.toJavaProto(nonZeroStatus)
            )
          )
      }

  def timeout(commandId: String, submissionId: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Try[CompletionResponse] =
    Failure(
      CommonErrors.RequestTimeOut
        .Reject(
          s"Timed out while awaiting for a completion corresponding to a command submission with command-id=$commandId and submission-id=$submissionId.",
          definiteAnswer = false,
        )
        .asGrpcError
    )

  def duplicate(
      submissionId: String
  )(implicit errorLogger: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(
      ConsistencyErrors.DuplicateCommand
        .Reject(existingCommandSubmissionId = Some(submissionId))
        .asGrpcError
    )

  def closing(implicit errorLogger: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(CommonErrors.ServerIsShuttingDown.Reject().asGrpcError)

  private def missingStatusError(errorLogger: ContextualizedErrorLogger): StatusRuntimeException = {
    CommonErrors.ServiceInternalError
      .Generic(
        "Missing status in completion response",
        throwableO = None,
      )(errorLogger)
      .asGrpcError
  }
}
