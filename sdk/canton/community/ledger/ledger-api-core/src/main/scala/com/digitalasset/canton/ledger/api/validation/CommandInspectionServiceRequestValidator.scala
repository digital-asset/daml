// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.admin.command_inspection_service.GetCommandStatusRequest
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidField
import com.digitalasset.daml.lf.data.Ref
import io.grpc.StatusRuntimeException

object CommandInspectionServiceRequestValidator {
  def validateCommandStatusRequest(
      request: GetCommandStatusRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, GetCommandStatusRequest] =
    if (request.commandIdPrefix.isEmpty) Right(request)
    else
      Ref.CommandId
        .fromString(request.commandIdPrefix)
        .map(_ => request)
        .left
        .map(invalidField("command_id_prefix", _))

}
