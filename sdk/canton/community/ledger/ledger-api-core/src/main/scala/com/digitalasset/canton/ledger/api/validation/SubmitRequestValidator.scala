// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.canton.ledger.api.messages.command.submission
import io.grpc.StatusRuntimeException

import java.time.{Duration, Instant}

class SubmitRequestValidator(
    commandsValidator: CommandsValidator
) {
  import FieldValidator.*
  def validate(
      req: SubmitRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Option[Duration],
      domainIdString: Option[String],
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
      domainId <- validateOptional(domainIdString)(
        requireDomainId(_, "domain_id")
      )
    } yield submission.SubmitRequest(validatedCommands.copy(domainId = domainId))
}
