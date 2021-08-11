// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import com.daml.ledger.api.v1.completion.Completion
import com.google.protobuf.Any
import com.google.rpc
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status.Code
import io.grpc.{StatusException, protobuf}

import scala.jdk.CollectionConverters._

object CompletionResponse {
  sealed trait CompletionFailure {
    def metadata: Map[String, String] = Map.empty
  }
  final case class NotOkResponse(commandId: String, grpcStatus: StatusProto)
      extends CompletionFailure
  final case class TimeoutResponse(commandId: String) extends CompletionFailure
  final case class NoStatusInResponse(commandId: String) extends CompletionFailure

  final case class CompletionSuccess(
      commandId: String,
      transactionId: String,
      originalStatus: StatusProto,
  )

  object CompletionSuccess {

    /** In most cases we're not interested in the original grpc status, as it's used only to keep backwards compatibility
      */
    def unapply(success: CompletionSuccess): Option[(String, String)] = {
      Some(success.commandId, success.transactionId)
    }
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
  def toCompletion(response: Either[CompletionFailure, CompletionSuccess]): Completion = {
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
  }

  def toException(response: CompletionResponse.CompletionFailure): StatusException = {
    val errorInfo = rpc.ErrorInfo.newBuilder().putAllMetadata(response.metadata.asJava).build()
    val details = Any.pack(errorInfo)
    val status = response match {
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
    protobuf.StatusProto.toStatusException(
      status
        .addDetails(details)
        .build()
    )
  }
}
