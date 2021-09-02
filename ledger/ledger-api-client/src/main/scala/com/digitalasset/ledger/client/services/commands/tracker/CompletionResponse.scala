// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.grpc.GrpcStatuses
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status.Code
import io.grpc.{Status, StatusException, protobuf}

import scala.jdk.CollectionConverters._

object CompletionResponse {

  /** Represents failures from executing submissions through gRPC.
    */
  sealed trait CompletionFailure
  final case class NotOkResponse(commandId: String, grpcStatus: StatusProto)
      extends CompletionFailure {
    def metadata: Map[String, String] = Map(
      GrpcStatuses.DefiniteAnswerKey -> GrpcStatuses.isDefiniteAnswer(grpcStatus).toString
    )
  }
  final case class TimeoutResponse(commandId: String) extends CompletionFailure
  final case class NoStatusInResponse(commandId: String) extends CompletionFailure

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
  private[daml] final case class QueueSubmitFailure(status: Status) extends TrackedCompletionFailure

  final case class CompletionSuccess(
      commandId: String,
      transactionId: String,
      originalStatus: StatusProto,
  )

  def apply(completion: Completion): Either[CompletionFailure, CompletionSuccess] =
    completion.status match {
      case Some(grpcStatus) if Code.OK.value() == grpcStatus.code =>
        Right(
          CompletionSuccess(
            commandId = completion.commandId,
            transactionId = completion.transactionId,
            grpcStatus,
          )
        )
      case Some(grpcStatus) =>
        Left(NotOkResponse(completion.commandId, grpcStatus))
      case None =>
        Left(NoStatusInResponse(completion.commandId))
    }

  /** For backwards compatibility, clients that are too coupled to [[Completion]] as a type can convert back from [[Either[CompletionFailure, CompletionSuccess]]]
    */
  def toCompletion(response: Either[CompletionFailure, CompletionSuccess]): Completion =
    response match {
      case Left(failure) =>
        failure match {
          case NotOkResponse(commandId, grpcStatus) =>
            Completion(commandId = commandId, status = Some(grpcStatus))
          case TimeoutResponse(commandId) =>
            Completion(
              commandId = commandId,
              status = Some(StatusProto(Code.ABORTED.value(), "Timeout")),
            )
          case NoStatusInResponse(commandId) =>
            Completion(commandId = commandId)
        }
      case Right(success) =>
        Completion(
          commandId = success.commandId,
          transactionId = success.transactionId,
          status = Some(success.originalStatus),
        )
    }

  private[daml] def toException(response: TrackedCompletionFailure): StatusException =
    response match {
      case QueueCompletionFailure(failure) =>
        val metadata = extractMetadata(failure)
        val statusBuilder = extractStatus(failure)
        buildException(metadata, statusBuilder)
      case QueueSubmitFailure(status) =>
        val statusBuilder = GrpcStatus.toJavaBuilder(status)
        buildException(Map.empty, statusBuilder)
    }

  private def extractMetadata(response: CompletionFailure): Map[String, String] = response match {
    case notOkResponse: CompletionResponse.NotOkResponse => notOkResponse.metadata
    case _ => Map.empty
  }

  private def extractStatus(response: CompletionFailure) = response match {
    case CompletionResponse.NotOkResponse(_, grpcStatus) => GrpcStatus.toJavaBuilder(grpcStatus)
    case CompletionResponse.TimeoutResponse(_) =>
      GrpcStatus.toJavaBuilder(Code.ABORTED.value(), Some("Timeout"), Iterable.empty)
    case CompletionResponse.NoStatusInResponse(_) =>
      GrpcStatus.toJavaBuilder(
        Code.INTERNAL.value(),
        Some("Missing status in completion response."),
        Iterable.empty,
      )
  }

  private def buildException(metadata: Map[String, String], status: rpc.Status.Builder) = {
    val newDetails = status.getDetailsList.asScala.map { any =>
      if (any.is[rpc.ErrorInfo]) {
        val previousErrorInfo: rpc.ErrorInfo = any.unpack[rpc.ErrorInfo]
        val newErrorInfo = previousErrorInfo.toBuilder.putAllMetadata(metadata.asJava).build()
        AnyProto.pack(newErrorInfo)
      } else any
    }
    protobuf.StatusProto.toStatusException(
      status
        .clearDetails()
        .addAllDetails(newDetails.asJava)
        .build()
    )
  }
}
