// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetUpdatesRequest,
}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.ParticipantOffset
import com.digitalasset.canton.ledger.api.domain.types.ParticipantOffset
import com.digitalasset.canton.ledger.api.messages.transaction
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.daml.lf.data.Ref
import io.grpc.StatusRuntimeException

object UpdateServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

}
class UpdateServiceRequestValidator(partyValidator: PartyValidator) {

  import FieldValidator.*
  import UpdateServiceRequestValidator.Result

  case class PartialValidation(
      transactionFilter: TransactionFilter,
      begin: ParticipantOffset,
      end: Option[ParticipantOffset],
      knownParties: Set[Ref.Party],
  )

  private def commonValidations(
      req: GetUpdatesRequest
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[PartialValidation] =
    for {
      filter <- requirePresence(req.filter, "filter")
      begin <- ParticipantOffsetValidator
        .validate(req.beginExclusive)
        .map(ParticipantOffset.fromString)
      convertedEnd <- ParticipantOffsetValidator
        .validate(req.endInclusive)
        .map(str =>
          if (str.isEmpty) None
          else Some(ParticipantOffset.fromString(str))
        )
      knownParties <- partyValidator.requireKnownParties(req.getFilter.filtersByParty.keySet)
    } yield PartialValidation(
      filter,
      begin,
      convertedEnd,
      knownParties,
    )

  def validate(
      req: GetUpdatesRequest,
      ledgerEnd: ParticipantOffset,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionsRequest] =
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
      convertedFilter <- TransactionFilterValidator.validate(partial.transactionFilter)
    } yield {
      transaction.GetTransactionsRequest(
        partial.begin,
        partial.end,
        convertedFilter,
        req.verbose,
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
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      transaction.GetTransactionByIdRequest(
        domain.TransactionId(trId),
        parties,
      )
    }

  def validateTransactionByEventId(
      req: GetTransactionByEventIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionByEventIdRequest] =
    for {
      eventId <- requireLedgerString(req.eventId, "event_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      transaction.GetTransactionByEventIdRequest(
        domain.EventId(eventId),
        parties,
      )
    }
}
