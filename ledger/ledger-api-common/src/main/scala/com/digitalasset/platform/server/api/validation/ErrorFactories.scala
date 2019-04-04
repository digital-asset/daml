// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.platform.server.api.ApiException
import io.grpc.{Status, StatusRuntimeException}

trait ErrorFactories {

  def ledgerIdMismatch(expected: String, received: String): StatusRuntimeException =
    grpcError(
      Status.NOT_FOUND.withDescription(
        s"Ledger ID '$received' not found. Actual Ledger ID is '$expected'."))

  def missingField(fieldName: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Missing field: $fieldName"))

  def invalidArgument(errorMsg: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Invalid argument: $errorMsg"))

  def notFound(target: String): StatusRuntimeException =
    grpcError(Status.NOT_FOUND.withDescription(s"$target not found."))

  def internal(description: String): StatusRuntimeException =
    grpcError(Status.INTERNAL.withDescription(description))

  def aborted(description: String): StatusRuntimeException =
    grpcError(Status.INTERNAL.withDescription(description))

  def grpcError(status: Status) = new ApiException(status)

}

object ErrorFactories extends ErrorFactories
