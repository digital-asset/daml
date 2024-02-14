// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionStreamRequest as GrpcCompletionStreamRequest
}
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import io.grpc.StatusRuntimeException

class CompletionServiceRequestValidator(
    partyNameChecker: PartyNameChecker
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
      appId <- requireApplicationId(request.applicationId, "application_id")
      parties <- requireParties(request.parties.toSet)
      convertedOffset <- LedgerOffsetValidator.validateOptional(request.offset, "offset")
    } yield CompletionStreamRequest(
      None,
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
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "Begin",
        request.offset,
        ledgerEnd,
      )
      _ <- requireNonEmpty(request.parties, "parties")
      _ <- partyValidator.requireKnownParties(request.parties)
    } yield request

}
