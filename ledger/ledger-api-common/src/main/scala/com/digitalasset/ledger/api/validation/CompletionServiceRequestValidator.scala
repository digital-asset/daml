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

  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
  private val fieldValidations = FieldValidations(errorFactories)
  private val partyValidator =
    new PartyValidator(partyNameChecker, errorFactories, fieldValidations)
  private val ledgerOffsetValidator = new LedgerOffsetValidator(errorFactories)

  import errorFactories._
  import fieldValidations._

  def validateGrpcCompletionStreamRequest(
      request: GrpcCompletionStreamRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      validLedgerId <- matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      nonEmptyAppId <- requireNonEmptyString(
        request.applicationId,
        "application_id",
      )
      appId <- Ref.LedgerString
        .fromString(nonEmptyAppId)
        .left
        .map(invalidField("application_id", _, None))
      parties <- requireParties(request.parties.toSet)
      convertedOffset <- ledgerOffsetValidator.validateOptional(request.offset, "offset")
    } yield CompletionStreamRequest(
      validLedgerId,
      ApplicationId(appId),
      parties,
      convertedOffset,
    )

  def validateCompletionStreamRequest(
      request: CompletionStreamRequest,
      ledgerEnd: LedgerOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      _ <- matchLedgerId(ledgerId)(request.ledgerId)
      _ <- ledgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "Begin",
        request.offset,
        ledgerEnd,
      )
      _ <- requireNonEmpty(request.parties, "parties")
      _ <- partyValidator.requireKnownParties(request.parties)
    } yield request

  def validateCompletionEndRequest(
      req: CompletionEndRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, completion.CompletionEndRequest] =
    for {
      ledgerId <- matchLedgerId(ledgerId)(LedgerId(req.ledgerId))
    } yield completion.CompletionEndRequest(ledgerId)

}
