// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.command.completion
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest => GrpcCompletionStreamRequest,
}
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.StatusRuntimeException

class CompletionServiceRequestValidator(
    ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
) {

  private val errorFactories = new ErrorFactories(errorCodesVersionSwitcher)
  private val fieldValidations = new FieldValidations(errorFactories)
  private val partyValidator =
    new PartyValidator(partyNameChecker, errorFactories, fieldValidations)
  private val ledgerOffsetValidator = new LedgerOffsetValidator(errorFactories)

  def validateCompletionStreamRequest(
      request: GrpcCompletionStreamRequest,
      ledgerEnd: LedgerOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      _ <- fieldValidations.matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      nonEmptyAppId <- fieldValidations.requireNonEmptyString(
        request.applicationId,
        "application_id",
      )
      appId <- Ref.LedgerString
        .fromString(nonEmptyAppId)
        .left
        .map(errorFactories.invalidField("application_id", _, None))
      nonEmptyParties <- fieldValidations.requireNonEmpty(request.parties, "parties")
      knownParties <- partyValidator.requireKnownParties(nonEmptyParties)
      convertedOffset <- ledgerOffsetValidator.validateOptional(request.offset, "offset")
      _ <- ledgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "Begin",
        convertedOffset,
        ledgerEnd,
      )
    } yield CompletionStreamRequest(
      ledgerId,
      ApplicationId(appId),
      knownParties,
      convertedOffset,
    )

  def validateCompletionEndRequest(
      req: CompletionEndRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, completion.CompletionEndRequest] =
    for {
      ledgerId <- fieldValidations.matchLedgerId(ledgerId)(LedgerId(req.ledgerId))
    } yield completion.CompletionEndRequest(ledgerId)

}
