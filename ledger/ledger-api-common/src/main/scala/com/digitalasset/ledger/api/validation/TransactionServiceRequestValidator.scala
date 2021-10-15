// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ErrorCodeLoggingContext
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.ledger.api.messages.transaction
import com.daml.ledger.api.messages.transaction.GetTransactionTreesRequest
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.daml.platform.server.api.validation.ErrorFactories._
import com.daml.platform.server.api.validation.FieldValidations._
import io.grpc.StatusRuntimeException

object TransactionServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

}
class TransactionServiceRequestValidator(
    ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
) {

  import TransactionServiceRequestValidator.Result

  private val partyValidator = new PartyValidator(partyNameChecker)

  private def matchId(input: LedgerId)(implicit
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Result[LedgerId] = matchLedgerId(ledgerId)(input)

  case class PartialValidation(
      ledgerId: domain.LedgerId,
      transactionFilter: TransactionFilter,
      begin: domain.LedgerOffset,
      end: Option[domain.LedgerOffset],
      knownParties: Set[Ref.Party],
  )

  private def commonValidations(
      req: GetTransactionsRequest
  )(implicit errorCodeLoggingContext: ErrorCodeLoggingContext): Result[PartialValidation] = {
    for {
      ledgerId <- matchId(LedgerId(req.ledgerId))
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
      errorCodeLoggingContext: ErrorCodeLoggingContext
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
      convertedFilter <- TransactionFilterValidator.validate(partial.transactionFilter)
    } yield {
      transaction.GetTransactionsRequest(
        ledgerId,
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
      errorCodeLoggingContext: ErrorCodeLoggingContext
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
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Result[transaction.GetLedgerEndRequest] = {
    for {
      ledgerId <- matchId(LedgerId(req.ledgerId))
    } yield {
      transaction.GetLedgerEndRequest(ledgerId)
    }
  }

  def validateTransactionById(
      req: GetTransactionByIdRequest
  )(implicit
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Result[transaction.GetTransactionByIdRequest] = {
    for {
      ledgerId <- matchId(LedgerId(req.ledgerId))
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
      errorCodeLoggingContext: ErrorCodeLoggingContext
  ): Result[transaction.GetTransactionByEventIdRequest] = {
    for {
      ledgerId <- matchId(LedgerId(req.ledgerId))
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

  private def transactionFilterToPartySet(
      transactionFilter: TransactionFilter
  )(implicit errorCodeLoggingContext: ErrorCodeLoggingContext) =
    transactionFilter.filtersByParty
      .collectFirst { case (party, Filters(Some(inclusive))) =>
        invalidArgument(None)(
          s"$party attempted subscription for templates ${inclusive.templateIds.mkString("[", ", ", "]")}. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC."
        )
      }
      .fold(partyValidator.requireKnownParties(transactionFilter.filtersByParty.keys))(Left(_))

}
