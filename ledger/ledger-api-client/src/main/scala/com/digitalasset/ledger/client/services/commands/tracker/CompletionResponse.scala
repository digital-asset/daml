// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.ledger.api.v1.completion.Completion
import com.google.protobuf.Any
import com.google.rpc
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status.Code
import io.grpc.{Status, StatusException, protobuf}

import scala.jdk.CollectionConverters._

object CompletionResponse {

  /** Represents failures from executing submissions through gRPC.
    */
  sealed trait CompletionFailure {
    def metadata: Map[String, String] = Map.empty
  }
  final case class NotOkResponse(commandId: String, grpcStatus: StatusProto)
      extends CompletionFailure
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

  object CompletionSuccess {

    /** In most cases we're not interested in the original grpc status, as it's used only to keep backwards compatibility
      */
    def unapply(success: CompletionSuccess): Option[(String, String)] = Some(
      success.commandId -> success.transactionId
    )
  }

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
        toException(failure)
      case QueueSubmitFailure(status) =>
        val protoStatus =
          rpc.Status
            .newBuilder()
            .setCode(status.getCode.value())
            .setMessage(Option(status.getDescription).getOrElse("Failed to submit request"))
        buildException(Map.empty[String, String], protoStatus)
    }

  def toException(response: CompletionResponse.CompletionFailure): StatusException = {
    val metadata = response.metadata
    val status = extractStatus(response)
    buildException(metadata, status)
  }

  private def buildException(metadata: Map[String, String], status: rpc.Status.Builder) = {
    val errorInfo = rpc.ErrorInfo.newBuilder().putAllMetadata(metadata.asJava).build()
    val details = Any.pack(errorInfo)
    protobuf.StatusProto.toStatusException(
      status
        .addDetails(details)
        .build()
    )
  }

  private def extractStatus(response: CompletionFailure) = response match {
    case CompletionResponse.NotOkResponse(_, grpcStatus) =>
      rpc.Status.newBuilder().setCode(grpcStatus.code).setMessage(grpcStatus.message)
    case CompletionResponse.TimeoutResponse(_) =>
      rpc.Status.newBuilder().setCode(Code.ABORTED.value()).setMessage("Timeout")
    case CompletionResponse.NoStatusInResponse(_) =>
      rpc.Status
        .newBuilder()
        .setCode(Code.INTERNAL.value())
        .setMessage("Missing status in completion response.")
  }
}
