// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.ContextualizedErrorLogger
import com.daml.platform.server.api.{ApiException => NoStackTraceApiException}
import com.google.rpc.Status
import io.grpc.protobuf.StatusProto
import io.grpc.StatusRuntimeException

object ErrorFactories {

  /** @param fieldName A missing field's name.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def missingField(fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.MissingField
      .Reject(fieldName)
      .asGrpcError

  /** @param message A status' message.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidArgument(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidArgument
      .Reject(message)
      .asGrpcError

  /** @param fieldName An invalid field's name.
    * @param message A status' message.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidField(
      fieldName: String,
      message: String,
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidField
      .Reject(s"Invalid field $fieldName: $message")
      .asGrpcError

  /** Transforms Protobuf [[Status]] objects, possibly including metadata packed as [[ErrorInfo]] objects,
    * into exceptions with metadata in the trailers.
    *
    * Asynchronous errors, i.e. failed completions, contain Protobuf [[Status]] objects themselves.
    *
    * NOTE: The length of the Status message is truncated to a reasonable size for satisfying
    *        the Netty header size limit - as the message is also incorporated in the header, bundled in the gRPC metadata.
    *
    * @param status A Protobuf [[Status]] object.
    * @return An exception without a stack trace.
    */
  def grpcError(status: Status): StatusRuntimeException = {
    val newStatus =
      Status
        .newBuilder(status)
        .setMessage(truncated(status.getMessage))
    new NoStackTraceApiException(
      StatusProto.toStatusRuntimeException(newStatus.build)
    )
  }

  private def truncated(message: String): String = {
    val maxMessageLength =
      1536 // An arbitrary limit that doesn't break netty serialization while being useful to human operator.
    if (message.length > maxMessageLength) message.take(maxMessageLength) + "..." else message
  }

}
