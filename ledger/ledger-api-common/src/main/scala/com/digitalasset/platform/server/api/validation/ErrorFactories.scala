// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.server.api.ApiException
import io.grpc.{Status, StatusRuntimeException}

import scalaz.syntax.tag._

trait ErrorFactories {

  def ledgerIdMismatch(expected: LedgerId, received: LedgerId): StatusRuntimeException =
    grpcError(
      Status.NOT_FOUND.withDescription(
        s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."))

  def missingField(fieldName: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Missing field: $fieldName"))

  def invalidArgument(errorMsg: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Invalid argument: $errorMsg"))

  def invalidField(fieldName: String, message: String) =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Invalid field $fieldName: $message"))

  def notFound(target: String): StatusRuntimeException =
    grpcError(Status.NOT_FOUND.withDescription(s"$target not found."))

  def internal(description: String): StatusRuntimeException =
    grpcError(Status.INTERNAL.withDescription(description))

  def aborted(description: String): StatusRuntimeException =
    grpcError(Status.INTERNAL.withDescription(description))

  def unimplemented(description: String): StatusRuntimeException =
    grpcError(Status.UNIMPLEMENTED.withDescription(description))

  def permissionDenied(description: String): StatusRuntimeException =
    grpcError(Status.PERMISSION_DENIED.withDescription(description))

  def grpcError(status: Status) = new ApiException(status)

}

object ErrorFactories extends ErrorFactories
