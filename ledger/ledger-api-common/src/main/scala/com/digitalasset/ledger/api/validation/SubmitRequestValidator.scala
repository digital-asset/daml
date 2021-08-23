// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import java.time.{Duration, Instant}

import com.daml.ledger.api.messages.command.submission
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.platform.server.api.validation.FieldValidations.requirePresence
import io.grpc.StatusRuntimeException

class SubmitRequestValidator(commandsValidator: CommandsValidator) {

  def validate(
      req: SubmitRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationTime: Option[Duration],
  ): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationTime,
      )
    } yield submission.SubmitRequest(validatedCommands)

}
