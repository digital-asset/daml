// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import io.grpc.StatusRuntimeException

object ValidationErrors {

  def missingField(fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    RequestValidationErrors.MissingField
      .Reject(fieldName)
      .asGrpcError

  def invalidArgument(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    RequestValidationErrors.InvalidArgument
      .Reject(message)
      .asGrpcError

  def invalidField(
      fieldName: String,
      message: String,
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    RequestValidationErrors.InvalidField
      .Reject(fieldName = fieldName, message = message)
      .asGrpcError

}
