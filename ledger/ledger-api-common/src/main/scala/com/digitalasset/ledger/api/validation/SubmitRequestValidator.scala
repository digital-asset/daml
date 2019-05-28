// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.ledger.api.messages.command.submission
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.platform.server.api.validation.FieldValidations.requirePresence
import com.digitalasset.platform.server.util.context.TraceContextConversions.toBrave
import io.grpc.StatusRuntimeException

class SubmitRequestValidator(commandsValidator: CommandsValidator) {

  def validate(req: SubmitRequest): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- commandsValidator.validateCommands(commands)
    } yield submission.SubmitRequest(validatedCommands, req.traceContext.map(toBrave))

}
