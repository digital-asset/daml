// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitRequest,
}
import com.digitalasset.canton.ledger.api.messages.command.submission
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import io.grpc.StatusRuntimeException

import java.time.{Duration, Instant}

class SubmitAndWaitRequestValidator(
    commandsValidator: CommandsValidator
) {

  def validate(
      req: SubmitAndWaitRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
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

  def validate(
      req: SubmitAndWaitForTransactionRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      _ <- requirePresence(req.transactionFormat, "transaction_format").flatMap(
        FormatValidator.validate
      )
      validatedCommands <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
    } yield submission.SubmitRequest(validatedCommands)

}
