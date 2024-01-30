// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.canton.ledger.api.messages.command.submission
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import io.grpc.StatusRuntimeException

import java.time.{Duration, Instant}

class SubmitAndWaitRequestValidator(
    commandsValidator: CommandsValidator
) {

  def validate(
      req: SubmitAndWaitRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Option[Duration],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
    } yield submission.SubmitRequest(validatedCommands)

}
