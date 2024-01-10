// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{LedgerId, LedgerOffset, optionalLedgerId}
import com.digitalasset.canton.ledger.api.messages.transaction
import com.digitalasset.canton.ledger.api.messages.transaction.GetTransactionTreesRequest
import io.grpc.StatusRuntimeException

object TransactionServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

}
class TransactionServiceRequestValidator(
    ledgerId: LedgerId,
    partyValidator: PartyValidator,
    transactionFilterValidator: TransactionFilterValidator,
) {

  import TransactionServiceRequestValidator.Result

  import FieldValidator.*
  import ValidationErrors.invalidArgument

  private def matchId(input: Option[LedgerId])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[Option[LedgerId]] = matchLedgerId(ledgerId)(input)

  case class PartialValidation(
      ledgerId: Option[domain.LedgerId],
      transactionFilter: TransactionFilter,
      begin: domain.LedgerOffset,
      end: Option[domain.LedgerOffset],
      knownParties: Set[Ref.Party],
  )

  private def commonValidations(
      req: GetTransactionsRequest
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[PartialValidation] = {
    for {
      ledgerId <- matchId(optionalLedgerId(req.ledgerId))
      filter <- requirePresence(req.filter, "filter")
      requiredBegin <- requirePresence(req.begin, "begin")
      convertedBegin <- LedgerOffsetValidator.validate(requiredBegin, "begin")
      convertedEnd <- LedgerOffsetValidator.validateOptional(req.end, "end")
      knownParties <- partyValidator.requireKnownParties(req.getFilter.filtersByParty.keySet)
    } yield PartialValidation(
      ledgerId,
      filter,
      convertedBegin,
      convertedEnd,
      knownParties,
    )

  }

  def validate(
      req: GetTransactionsRequest,
      ledgerEnd: LedgerOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionsRequest] = {

    for {
      partial <- commonValidations(req)
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "Begin",
        partial.begin,
        ledgerEnd,
      )
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "End",
        partial.end,
        ledgerEnd,
      )
      convertedFilter <- transactionFilterValidator.validate(partial.transactionFilter)
    } yield {
      transaction.GetTransactionsRequest(
        partial.ledgerId,
        partial.begin,
        partial.end,
        convertedFilter,
        req.verbose,
      )
    }
  }

  def validateTree(
      req: GetTransactionsRequest,
      ledgerEnd: LedgerOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[GetTransactionTreesRequest] = {

    for {
      partial <- commonValidations(req)
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "Begin",
        partial.begin,
        ledgerEnd,
      )
      _ <- LedgerOffsetValidator.offsetIsBeforeEndIfAbsolute(
        "End",
        partial.end,
        ledgerEnd,
      )
      convertedFilter <- transactionFilterToPartySet(partial.transactionFilter)
    } yield {
      transaction.GetTransactionTreesRequest(
        partial.ledgerId,
        partial.begin,
        partial.end,
        convertedFilter,
        req.verbose,
      )
    }
  }

  def validateLedgerEnd(req: GetLedgerEndRequest)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetLedgerEndRequest] = {
    for {
      _ <- matchId(optionalLedgerId(req.ledgerId))
    } yield {
      transaction.GetLedgerEndRequest()
    }
  }

  def validateTransactionById(
      req: GetTransactionByIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionByIdRequest] = {
    for {
      ledgerId <- matchId(optionalLedgerId(req.ledgerId))
      _ <- requireNonEmptyString(req.transactionId, "transaction_id")
      trId <- requireLedgerString(req.transactionId)
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      transaction.GetTransactionByIdRequest(
        ledgerId,
        domain.TransactionId(trId),
        parties,
      )
    }
  }

  def validateTransactionByEventId(
      req: GetTransactionByEventIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetTransactionByEventIdRequest] = {
    for {
      ledgerId <- matchId(optionalLedgerId(req.ledgerId))
      eventId <- requireLedgerString(req.eventId, "event_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      transaction.GetTransactionByEventIdRequest(
        ledgerId,
        domain.EventId(eventId),
        parties,
      )
    }
  }

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.transaction_filter.*")
  private def transactionFilterToPartySet(
      transactionFilter: TransactionFilter
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger) =
    transactionFilter.filtersByParty
      .collectFirst { case (party, Filters(Some(inclusive))) =>
        invalidArgument(
          s"$party attempted subscription for templates ${inclusive.templateIds.mkString("[", ", ", "]")}. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC."
        )
      }
      .fold(partyValidator.requireKnownParties(transactionFilter.filtersByParty.keys))(Left(_))

}
