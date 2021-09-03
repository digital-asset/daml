// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.platform.server.api.ApiException
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc.Status
import com.google.rpc.ErrorInfo
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import scalaz.syntax.tag._

trait ErrorFactories {

  import ErrorFactories._

  def ledgerIdMismatch(expected: LedgerId, received: LedgerId): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.NOT_FOUND.value())
        .setMessage(
          s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."
        )
        .addDetails(DefiniteAnswerInfo)
        .build()
    )

  def missingField(fieldName: String): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.INVALID_ARGUMENT.value())
        .setMessage(s"Missing field: $fieldName")
        .addDetails(DefiniteAnswerInfo)
        .build()
    )

  def invalidArgument(errorMsg: String): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.INVALID_ARGUMENT.value())
        .setMessage(s"Invalid argument: $errorMsg")
        .addDetails(DefiniteAnswerInfo)
        .build()
    )

  def invalidField(fieldName: String, message: String): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.INVALID_ARGUMENT.value())
        .setMessage(s"Invalid field $fieldName: $message")
        .addDetails(DefiniteAnswerInfo)
        .build()
    )

  def outOfRange(description: String): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.OUT_OF_RANGE.value())
        .setMessage(description)
        .addDetails(DefiniteAnswerInfo)
        .build()
    )

  def aborted(description: String): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.ABORTED.value())
        .setMessage(description)
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  // permission denied is intentionally without description to ensure we don't leak security relevant information by accident
  def permissionDenied(): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.PERMISSION_DENIED.value())
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  def unauthenticated(): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.UNAUTHENTICATED.value())
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  def missingLedgerConfig(): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.UNAVAILABLE.value())
        .setMessage("The ledger configuration is not available.")
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  def missingLedgerConfigUponRequest(): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.NOT_FOUND.value())
        .setMessage("The ledger configuration is not available.")
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  def participantPrunedDataAccessed(message: String): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.NOT_FOUND.value())
        .setMessage(message)
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  def serviceNotRunning(): StatusRuntimeException =
    grpcError(
      Status
        .newBuilder()
        .setCode(Code.UNAVAILABLE.value())
        .setMessage("Service has been shut down.")
        .addDetails(IndefiniteAnswerInfo)
        .build()
    )

  def grpcError(status: Status): StatusRuntimeException = {
    val temporaryException = StatusProto.toStatusException(status) // TODO: sort this out
    new ApiException(temporaryException.getStatus, temporaryException.getTrailers)
  }
}

object ErrorFactories extends ErrorFactories {
  val DefiniteAnswerInfo: AnyProto =
    AnyProto.pack[ErrorInfo](
      ErrorInfo.newBuilder().putMetadata(GrpcStatuses.DefiniteAnswerKey, "true").build()
    )
  val IndefiniteAnswerInfo: AnyProto =
    AnyProto.pack[ErrorInfo](
      ErrorInfo.newBuilder().putMetadata(GrpcStatuses.DefiniteAnswerKey, "false").build()
    )
}
