// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.platform.error.definitions.LedgerApiErrors
import com.google.rpc.status.{Status => StatusProto}
import com.google.rpc.{Status => StatusJavaProto}
import io.grpc.Status.Code
import io.grpc.{StatusRuntimeException, protobuf}

object CompletionResponse {

  /** Represents failures from executing submissions through gRPC.
    */
  sealed trait CompletionFailure
  final case class NotOkResponse(completion: Completion, checkpoint: Option[Checkpoint])
      extends CompletionFailure {
    val commandId: String = completion.commandId
    val grpcStatus: StatusProto = completion.getStatus
    def metadata: Map[String, String] = Map(
      GrpcStatuses.DefiniteAnswerKey -> GrpcStatuses.isDefiniteAnswer(grpcStatus).toString
    )
  }
  final case class TimeoutResponse(commandId: String) extends CompletionFailure

  final case class NoStatusInResponse(completion: Completion, checkpoint: Option[Checkpoint])
      extends CompletionFailure {
    val commandId: String = completion.commandId
  }

  /** Represents failures of submissions throughout the execution queue.
    */
  private[daml] sealed trait TrackedCompletionFailure

  /** The submission was executed after it was enqueued but was not successful.
    */
  private[daml] final case class QueueCompletionFailure(failure: CompletionFailure)
      extends TrackedCompletionFailure

  /** The submission could not be added to the execution queue.
    * @param status - gRPC status chosen based on the reason why adding to the queue failed
    */
  private[daml] final case class QueueSubmitFailure(status: StatusJavaProto)
      extends TrackedCompletionFailure

  final case class CompletionSuccess(
      completion: Completion,
      checkpoint: Option[Checkpoint],
  ) {
    val commandId: String = completion.commandId
    val transactionId: String = completion.transactionId
    val originalStatus: StatusProto = completion.getStatus
  }

  def apply(
      completion: Completion,
      checkpoint: Option[Checkpoint],
  ): Either[CompletionFailure, CompletionSuccess] =
    completion.status match {
      case Some(grpcStatus) if Code.OK.value() == grpcStatus.code =>
        Right(CompletionSuccess(completion, checkpoint))
      case Some(_) =>
        Left(NotOkResponse(completion, checkpoint))
      case None =>
        Left(NoStatusInResponse(completion, checkpoint))
    }

  /** For backwards compatibility, clients that are too coupled to [[Completion]] as a type can convert back from [[Either[CompletionFailure, CompletionSuccess]]]
    */
  def toCompletion(response: Either[CompletionFailure, CompletionSuccess]): Completion =
    response match {
      case Left(failure) =>
        failure match {
          case NotOkResponse(completion, _) =>
            completion
          case TimeoutResponse(commandId) =>
            Completion(
              commandId = commandId,
              status = Some(StatusProto(Code.ABORTED.value(), "Timeout")),
            )
          case NoStatusInResponse(completion, _) =>
            completion
        }
      case Right(success) =>
        success.completion
    }

  private[daml] def toException(response: TrackedCompletionFailure)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    val status = response match {
      case QueueCompletionFailure(failure) =>
        val metadata = extractMetadata(failure)
        extractStatus(failure, metadata)
      case QueueSubmitFailure(status) => status
    }
    protobuf.StatusProto.toStatusRuntimeException(status)
  }

  private def extractMetadata(response: CompletionFailure): Map[String, String] = response match {
    case notOkResponse: CompletionResponse.NotOkResponse => notOkResponse.metadata
    case _ => Map.empty
  }

  private def extractStatus(
      response: CompletionFailure,
      metadata: Map[String, String],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusJavaProto =
    response match {
      case notOkResponse: CompletionResponse.NotOkResponse =>
        val statusBuilder = GrpcStatus.toJavaBuilder(notOkResponse.grpcStatus)
        GrpcStatus.buildStatus(metadata, statusBuilder)
      case CompletionResponse.TimeoutResponse(_) =>
        LedgerApiErrors.RequestTimeOut
          .Reject(
            "Timed out while awaiting for a completion corresponding to a command submission.",
            definiteAnswer = false,
          )
          .asGrpcStatus
      case CompletionResponse.NoStatusInResponse(_, _) =>
        LedgerApiErrors.InternalError
          .Generic(
            "Missing status in completion response.",
            throwableO = None,
          )
          .asGrpcStatus

    }

}
