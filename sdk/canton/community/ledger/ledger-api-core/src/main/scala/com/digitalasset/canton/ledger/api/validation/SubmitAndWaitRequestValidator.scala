// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitRequest,
}
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.logging.ErrorLoggingContext
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
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Unit] =
    for {
      commands <- requirePresence(req.commands, "commands")
      _ <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
    } yield ()

  def validate(
      req: SubmitAndWaitForTransactionRequest,
      currentLedgerTime: Instant,
      currentUtcTime: Instant,
      maxDeduplicationDuration: Duration,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Unit] =
    for {
      commands <- requirePresence(req.commands, "commands")
      _ <- requirePresence(req.transactionFormat, "transaction_format").flatMap(
        FormatValidator.validate
      )
      _ <- commandsValidator.validateCommands(
        commands,
        currentLedgerTime,
        currentUtcTime,
        maxDeduplicationDuration,
      )
    } yield ()

  def validate(
      req: SubmitAndWaitForReassignmentRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Unit] =
    for {
      commands <- requirePresence(req.reassignmentCommands, "reassignment_commands")
      _ <- req.eventFormat match {
        case Some(eventFormat) => FormatValidator.validate(eventFormat).map(_ => ())
        case None => Right(())
      }
      _ <- commandsValidator.validateReassignmentCommands(
        commands
      )
    } yield ()

}
