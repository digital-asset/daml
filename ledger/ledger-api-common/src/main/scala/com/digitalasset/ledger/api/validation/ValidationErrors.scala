// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import io.grpc.StatusRuntimeException

object ValidationErrors {

  def missingField(fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.MissingField
      .Reject(fieldName)
      .asGrpcError

  def invalidArgument(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidArgument
      .Reject(message)
      .asGrpcError

  def invalidField(
      fieldName: String,
      message: String,
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidField
      .Reject(fieldName = fieldName, message = message)
      .asGrpcError

}
