// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import java.time.{Duration, Instant}

import com.digitalasset.ledger.api.messages.command.submission
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.platform.server.api.validation.FieldValidations.requirePresence
import com.digitalasset.platform.server.util.context.TraceContextConversions.toBrave
import io.grpc.StatusRuntimeException

class SubmitRequestValidator(commandsValidator: CommandsValidator) {

  def validate(
      req: SubmitRequest,
      currentLedgerTime: Instant,
      currentUTCTime: Instant,
      maxDeduplicationTime: Duration): Either[StatusRuntimeException, submission.SubmitRequest] =
    for {
      commands <- requirePresence(req.commands, "commands")
      validatedCommands <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUTCTime,
        maxDeduplicationTime)
    } yield submission.SubmitRequest(validatedCommands, req.traceContext.map(toBrave))

}
