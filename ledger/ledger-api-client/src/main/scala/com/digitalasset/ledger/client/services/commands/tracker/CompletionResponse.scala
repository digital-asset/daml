// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.grpc.GrpcStatuses
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc.status.{Status => StatusProto}
import com.google.rpc.{ErrorInfo, Status => StatusJavaProto}
import io.grpc.Status.Code
import io.grpc.{Status, StatusException, protobuf}

import scala.jdk.CollectionConverters._

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
  private[daml] final case class QueueSubmitFailure(status: Status) extends TrackedCompletionFailure

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

  private[daml] def toException(response: TrackedCompletionFailure): StatusException =
  // TODO error codes: Adapt V2 ?
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

  private def extractStatus(response: CompletionFailure): StatusJavaProto.Builder = response match {
    case notOkResponse: CompletionResponse.NotOkResponse =>
      GrpcStatus.toJavaBuilder(notOkResponse.grpcStatus)
    case CompletionResponse.TimeoutResponse(_) =>
      GrpcStatus.toJavaBuilder(Code.ABORTED.value(), Some("Timeout"), Iterable.empty)
    case CompletionResponse.NoStatusInResponse(_) =>
      GrpcStatus.toJavaBuilder(
        Code.INTERNAL.value(),
        Some("Missing status in completion response."),
        Iterable.empty,
      )
  }

  private def buildException(metadata: Map[String, String], status: StatusJavaProto.Builder) = {
    val details = mergeDetails(metadata, status)
    // TODO error codes: Adapt V2 ?
    protobuf.StatusProto.toStatusException(
      status
        .clearDetails()
        .addAllDetails(details.asJava)
        .build()
    )
  }

  private def mergeDetails(metadata: Map[String, String], status: StatusJavaProto.Builder) = {
    val previousDetails = status.getDetailsList.asScala
    val newDetails = if (previousDetails.exists(_.is(classOf[ErrorInfo]))) {
      previousDetails.map {
        case detail if detail.is(classOf[ErrorInfo]) =>
          val previousErrorInfo: ErrorInfo = detail.unpack(classOf[ErrorInfo])
          val newErrorInfo = previousErrorInfo.toBuilder.putAllMetadata(metadata.asJava).build()
          AnyProto.pack(newErrorInfo)
        case otherDetail => otherDetail
      }
    } else {
      previousDetails :+ AnyProto.pack(
        ErrorInfo.newBuilder().putAllMetadata(metadata.asJava).build()
      )
    }
    newDetails
  }
}
