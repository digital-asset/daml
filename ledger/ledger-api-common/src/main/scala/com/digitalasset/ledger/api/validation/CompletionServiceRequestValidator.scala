// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerId}
import com.digitalasset.ledger.api.messages.command.completion
import com.digitalasset.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionEndRequest,
  CompletionStreamRequest => GrpcCompletionStreamRequest
}
import com.digitalasset.platform.server.api.validation.FieldValidations
import com.digitalasset.platform.server.util.context.TraceContextConversions.toBrave
import io.grpc.StatusRuntimeException
import com.digitalasset.platform.server.api.validation.ErrorFactories._

class CompletionServiceRequestValidator(ledgerId: LedgerId, partyNameChecker: PartyNameChecker)
    extends FieldValidations {

  private val partyValidator = new PartyValidator(partyNameChecker)

  def validateCompletionStreamRequest(request: GrpcCompletionStreamRequest)
    : Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      _ <- matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      nonEmptyAppId <- requireNonEmptyString(request.applicationId, "application_id")
      appId <- Ref.LedgerString
        .fromString(nonEmptyAppId)
        .left
        .map(invalidField("application_id", _))
      nonEmptyParties <- requireNonEmpty(request.parties, "parties")
      knownParties <- partyValidator.requireKnownParties(nonEmptyParties)
      convertedOffset <- LedgerOffsetValidator.validateOptional(request.offset, "offset")
    } yield
      CompletionStreamRequest(
        ledgerId,
        ApplicationId(appId),
        knownParties,
        convertedOffset
      )

  def validateCompletionEndRequest(
      req: CompletionEndRequest): Either[StatusRuntimeException, completion.CompletionEndRequest] =
    for {
      ledgerId <- matchLedgerId(ledgerId)(LedgerId(req.ledgerId))
    } yield completion.CompletionEndRequest(ledgerId, req.traceContext.map(toBrave))

}
