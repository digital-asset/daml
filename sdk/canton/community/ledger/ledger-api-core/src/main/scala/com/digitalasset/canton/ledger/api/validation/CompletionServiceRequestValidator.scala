// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamRequest as GrpcCompletionStreamRequest
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

object CompletionServiceRequestValidator {

  import FieldValidator.*

  def validateGrpcCompletionStreamRequest(
      request: GrpcCompletionStreamRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      userId <- requireUserId(request.userId, "user_id")
      parties <- requireParties(request.parties.toSet)
      offsetO <- ParticipantOffsetValidator.validateNonNegative(
        request.beginExclusive,
        "begin_exclusive",
      )
    } yield CompletionStreamRequest(
      userId,
      parties,
      offsetO,
    )

  def validateCompletionStreamRequest(
      request: CompletionStreamRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, CompletionStreamRequest] =
    for {
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "Begin",
        request.offset,
        ledgerEnd,
      )
      _ <- requireNonEmpty(request.parties, "parties")
    } yield request

}
