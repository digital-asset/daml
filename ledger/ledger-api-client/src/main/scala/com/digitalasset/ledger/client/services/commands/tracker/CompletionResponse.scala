package com.daml.ledger.client.services.commands.tracker

import com.daml.ledger.api.v1.completion.Completion
import com.google.protobuf.Any
import com.google.rpc
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status.Code
import io.grpc.{StatusRuntimeException, protobuf}

import scala.jdk.CollectionConverters.MapHasAsJava

object CompletionResponse {
  type CompletionResponse = Either[CompletionFailure, CompletionSuccess]
  sealed trait CompletionFailure {
    def metadata: Map[String, String] = Map.empty
  }
  final case class NotOkResponse(commandId: String, grpcStatus: StatusProto)
      extends CompletionFailure
  final case class TimeoutResponse(commandId: String) extends CompletionFailure
  final case class NoStatusInResponse(commandId: String) extends CompletionFailure

  final case class CompletionSuccess(commandId: String, transactionId: String)

  def apply(completion: Completion): Either[CompletionFailure, CompletionSuccess] = {
    completion.status match {
      case Some(grpcStatus) =>
        if (Code.OK.value() == grpcStatus.code) {
          Right(
            CompletionSuccess(
              commandId = completion.commandId,
              transactionId = completion.transactionId,
            )
          )
        } else {
          Left(NotOkResponse(completion.commandId, grpcStatus))
        }
      case None =>
        Left(NoStatusInResponse(completion.commandId))
    }
  }

  def toException(response: CompletionResponse.CompletionFailure): StatusRuntimeException = {
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
    protobuf.StatusProto.toStatusRuntimeException(
      status
        .addDetails(details)
        .build()
    )
  }
}
