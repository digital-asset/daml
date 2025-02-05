// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByIdRequest,
  GetTransactionByOffsetRequest,
  GetUpdatesRequest,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.UpdateId
import com.digitalasset.canton.ledger.api.messages.transaction
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.daml.lf.data.Ref
import io.grpc.StatusRuntimeException

object UpdateServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

  import FieldValidator.*

  final case class PartialValidation(
      transactionFilter: TransactionFilter,
      begin: Option[Offset],
      end: Option[Offset],
      knownParties: Set[Ref.Party],
  )

  private def commonValidations(
      req: GetUpdatesRequest
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[PartialValidation] =
    for {
      filter <- requirePresence(req.filter, "filter")
      begin <- ParticipantOffsetValidator
        .validateNonNegative(req.beginExclusive, "begin_exclusive")
      end <- ParticipantOffsetValidator
        .validateOptionalPositive(req.endInclusive, "end_inclusive")
      knownParties <- requireParties(req.getFilter.filtersByParty.keySet)
    } yield PartialValidation(
      filter,
      begin,
      end,
      knownParties,
    )

  def validate(
      req: GetUpdatesRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetUpdatesRequest] =
    for {
      partial <- commonValidations(req)
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "Begin",
        partial.begin,
        ledgerEnd,
      )
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "End",
        partial.end,
        ledgerEnd,
      )
      convertedFilter <- FormatValidator.validate(partial.transactionFilter, req.verbose)
    } yield {
      transaction.GetUpdatesRequest(
        partial.begin,
        partial.end,
        convertedFilter,
      )
    }

  def validateTransactionById(
      req: GetTransactionByIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionByIdRequest] =
    for {
      _ <- requireNonEmptyString(req.updateId, "update_id")
      trId <- requireLedgerString(req.updateId)
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- requireParties(req.requestingParties.toSet)
    } yield {
      transaction.GetTransactionByIdRequest(
        UpdateId(trId),
        parties,
      )
    }

  def validateTransactionByOffset(
      req: GetTransactionByOffsetRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionByOffsetRequest] =
    for {
      offset <- ParticipantOffsetValidator.validatePositive(req.offset, "offset")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- requireParties(req.requestingParties.toSet)
    } yield {
      transaction.GetTransactionByOffsetRequest(
        offset,
        parties,
      )
    }
}
