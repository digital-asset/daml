// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest as GrpcCompletionStreamRequest,
}
import com.digitalasset.canton.ledger.api.domain.{LedgerId, LedgerOffset, optionalLedgerId}
import com.digitalasset.canton.ledger.api.messages.command.completion
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import io.grpc.StatusRuntimeException

class CompletionServiceRequestValidator(
    ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
) {

  private val partyValidator =
    new PartyValidator(partyNameChecker)

  import FieldValidator.*

  def validateGrpcCompletionStreamRequest(
      request: GrpcCompletionStreamRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      validLedgerId <- matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      appId <- requireApplicationId(request.applicationId, "application_id")
      parties <- requireParties(request.parties.toSet)
      convertedOffset <- LedgerOffsetValidator.validateOptional(request.offset, "offset")
    } yield CompletionStreamRequest(
      validLedgerId,
      appId,
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
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
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
      _ <- matchLedgerId(ledgerId)(optionalLedgerId(req.ledgerId))
    } yield completion.CompletionEndRequest()

}
