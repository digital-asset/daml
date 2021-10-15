// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ErrorCodeLoggingContext
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.command.completion
import com.daml.ledger.api.messages.command.completion.CompletionStreamRequest
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest => GrpcCompletionStreamRequest,
}
import com.daml.platform.server.api.validation.FieldValidations
import io.grpc.StatusRuntimeException
import com.daml.platform.server.api.validation.ErrorFactories._

class CompletionServiceRequestValidator(ledgerId: LedgerId, partyNameChecker: PartyNameChecker)
    extends FieldValidations {

  private val partyValidator = new PartyValidator(partyNameChecker)

  def validateCompletionStreamRequest(
      request: GrpcCompletionStreamRequest,
      ledgerEnd: LedgerOffset.Absolute,
  )(implicit
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      _ <- matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      nonEmptyAppId <- requireNonEmptyString(request.applicationId, "application_id")
      appId <- Ref.LedgerString
        .fromString(nonEmptyAppId)
        .left
        .map(invalidField("application_id", _, None))
      nonEmptyParties <- requireNonEmpty(request.parties, "parties")
      knownParties <- partyValidator.requireKnownParties(nonEmptyParties)
      convertedOffset <- LedgerOffsetValidator.validateOptional(request.offset, "offset")
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
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
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Either[StatusRuntimeException, completion.CompletionEndRequest] =
    for {
      ledgerId <- matchLedgerId(ledgerId)(LedgerId(req.ledgerId))
    } yield completion.CompletionEndRequest(ledgerId)

}
