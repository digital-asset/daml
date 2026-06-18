// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

object ValidationErrors {

  def missingField(fieldName: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): StatusRuntimeException =
    RequestValidationErrors.MissingField
      .Reject(fieldName)
      .asGrpcError

  def invalidArgument(message: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): StatusRuntimeException =
    RequestValidationErrors.InvalidArgument
      .Reject(message)
      .asGrpcError

  def invalidField(
      fieldName: String,
      message: String,
  )(implicit errorLoggingContext: ErrorLoggingContext): StatusRuntimeException =
    RequestValidationErrors.InvalidField
      .Reject(fieldName = fieldName, message = message)
      .asGrpcError

}
