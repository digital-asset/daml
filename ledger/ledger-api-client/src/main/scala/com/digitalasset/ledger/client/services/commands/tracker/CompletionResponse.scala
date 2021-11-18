// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.rpc.status.{Status => StatusProto}
import com.google.rpc.{Status => StatusJavaProto}
import io.grpc.Status.Code
import io.grpc.{StatusRuntimeException, protobuf}

object CompletionResponse {

  /** Represents failures from executing submissions through gRPC.
    */
  sealed trait CompletionFailure
  final case class NotOkResponse(completion: Completion) extends CompletionFailure {
    val commandId: String = completion.commandId
    val grpcStatus: StatusProto = completion.getStatus
    def metadata: Map[String, String] = Map(
      GrpcStatuses.DefiniteAnswerKey -> GrpcStatuses.isDefiniteAnswer(grpcStatus).toString
    )
  }
  final case class TimeoutResponse(commandId: String) extends CompletionFailure

  final case class NoStatusInResponse(completion: Completion) extends CompletionFailure {
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
      completion: Completion
  ) {
    val commandId: String = completion.commandId
    val transactionId: String = completion.transactionId
    val originalStatus: StatusProto = completion.getStatus
  }

  def apply(completion: Completion): Either[CompletionFailure, CompletionSuccess] =
    completion.status match {
      case Some(grpcStatus) if Code.OK.value() == grpcStatus.code =>
        Right(CompletionSuccess(completion))
      case Some(_) =>
        Left(NotOkResponse(completion))
      case None =>
        Left(NoStatusInResponse(completion))
    }

  /** For backwards compatibility, clients that are too coupled to [[Completion]] as a type can convert back from [[Either[CompletionFailure, CompletionSuccess]]]
    */
  def toCompletion(response: Either[CompletionFailure, CompletionSuccess]): Completion =
    response match {
      case Left(failure) =>
        failure match {
          case NotOkResponse(completion) =>
            completion
          case TimeoutResponse(commandId) =>
            Completion(
              commandId = commandId,
              status = Some(StatusProto(Code.ABORTED.value(), "Timeout")),
            )
          case NoStatusInResponse(completion) =>
            completion
        }
      case Right(success) =>
        success.completion
    }

  private[daml] def toException(response: TrackedCompletionFailure, errorFactories: ErrorFactories)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    val status = response match {
      case QueueCompletionFailure(failure) =>
        val metadata = extractMetadata(failure)
        extractStatus(failure, errorFactories, metadata)
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
      errorFactories: ErrorFactories,
      metadata: Map[String, String],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusJavaProto =
    response match {
      case notOkResponse: CompletionResponse.NotOkResponse =>
        val statusBuilder = GrpcStatus.toJavaBuilder(notOkResponse.grpcStatus)
        GrpcStatus.buildStatus(metadata, statusBuilder)
      case CompletionResponse.TimeoutResponse(_) =>
        errorFactories.SubmissionQueueErrors.timedOutOnAwaitingForCommandCompletion()
      case CompletionResponse.NoStatusInResponse(_) =>
        errorFactories.SubmissionQueueErrors.noStatusInCompletionResponse()
    }

}
