// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.api.domain.LedgerId
import com.daml.platform.server.api.ApiException
import io.grpc.{Status, StatusRuntimeException}

import scalaz.syntax.tag._

trait ErrorFactories {

  def ledgerIdMismatch(expected: LedgerId, received: LedgerId): StatusRuntimeException =
    grpcError(
      Status.NOT_FOUND.withDescription(
        s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."
      )
    )

  def missingField(fieldName: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Missing field: $fieldName"))

  def invalidArgument(errorMsg: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Invalid argument: $errorMsg"))

  def invalidField(fieldName: String, message: String): StatusRuntimeException =
    grpcError(Status.INVALID_ARGUMENT.withDescription(s"Invalid field $fieldName: $message"))

  def outOfRange(description: String): StatusRuntimeException =
    grpcError(Status.OUT_OF_RANGE.withDescription(description))

  def aborted(description: String): StatusRuntimeException =
    grpcError(Status.ABORTED.withDescription(description))

  // permission denied is intentionally without description to ensure we don't leak security relevant information by accident
  def permissionDenied(): StatusRuntimeException =
    grpcError(Status.PERMISSION_DENIED)

  def unauthenticated(): StatusRuntimeException =
    grpcError(Status.UNAUTHENTICATED)

  def missingLedgerConfig(): StatusRuntimeException =
    grpcError(Status.UNAVAILABLE.withDescription("The ledger configuration is not available."))

  def missingLedgerConfigOnConfigRequest(): StatusRuntimeException =
    grpcError(Status.NOT_FOUND.withDescription("The ledger configuration is not available."))

  def participantPrunedDataAccessed(message: String): StatusRuntimeException =
    grpcError(Status.NOT_FOUND.withDescription(message))

  def grpcError(status: Status): StatusRuntimeException =
    new ApiException(status)

}

object ErrorFactories extends ErrorFactories
