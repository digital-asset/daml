// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import brave.propagation.TraceContext
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
  GetTransactionsRequest
}
import com.daml.platform.server.api.validation.ErrorFactories._
import com.daml.platform.server.api.validation.FieldValidations._
import com.daml.platform.server.util.context.TraceContextConversions._
import io.grpc.StatusRuntimeException

object TransactionServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

}
class TransactionServiceRequestValidator(
    ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker
) {

  import TransactionServiceRequestValidator.Result

  private val partyValidator = new PartyValidator(partyNameChecker)

  private def matchId(input: LedgerId): Result[LedgerId] = matchLedgerId(ledgerId)(input)

  case class PartialValidation(
      ledgerId: domain.LedgerId,
      transactionFilter: TransactionFilter,
      begin: domain.LedgerOffset,
      end: Option[domain.LedgerOffset],
      knownParties: Set[Ref.Party],
      traceContext: Option[TraceContext])

  private def commonValidations(req: GetTransactionsRequest): Result[PartialValidation] = {
    for {
      ledgerId <- matchId(LedgerId(req.ledgerId))
      filter <- requirePresence(req.filter, "filter")
      requiredBegin <- requirePresence(req.begin, "begin")
      convertedBegin <- LedgerOffsetValidator.validate(requiredBegin, "begin")
      convertedEnd <- LedgerOffsetValidator.validateOptional(req.end, "end")
      knownParties <- partyValidator.requireKnownParties(req.getFilter.filtersByParty.keySet)
    } yield
      PartialValidation(
        ledgerId,
        filter,
        convertedBegin,
        convertedEnd,
        knownParties,
        req.traceContext.map(toBrave))

  }

  private def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: LedgerOffset,
      ledgerEnd: LedgerOffset.Absolute,
      offsetOrdering: Ordering[LedgerOffset.Absolute]): Result[Unit] =
    ledgerOffset match {
      case abs: LedgerOffset.Absolute if offsetOrdering.gt(abs, ledgerEnd) =>
        Left(
          invalidArgument(
            s"$offsetType offset ${abs.value} is after ledger end ${ledgerEnd.value}"))
      case _ => Right(())
    }

  def validate(
      req: GetTransactionsRequest,
      ledgerEnd: LedgerOffset.Absolute,
      offsetOrdering: Ordering[LedgerOffset.Absolute])
    : Result[transaction.GetTransactionsRequest] = {

    for {
      partial <- commonValidations(req)
      _ <- offsetIsBeforeEndIfAbsolute("Begin", partial.begin, ledgerEnd, offsetOrdering)
      _ <- partial.end.fold[Result[Unit]](Right(()))(
        offsetIsBeforeEndIfAbsolute("End", _, ledgerEnd, offsetOrdering))
      convertedFilter <- TransactionFilterValidator.validate(
        partial.transactionFilter,
        "filter.filters_by_party")
    } yield {
      transaction.GetTransactionsRequest(
        ledgerId,
        partial.begin,
        partial.end,
        convertedFilter,
        req.verbose,
        req.traceContext.map(toBrave))
    }
  }

  def validateTree(
      req: GetTransactionsRequest,
      ledgerEnd: LedgerOffset.Absolute,
      offsetOrdering: Ordering[LedgerOffset.Absolute]): Result[GetTransactionTreesRequest] = {

    for {
      partial <- commonValidations(req)
      _ <- offsetIsBeforeEndIfAbsolute("Begin", partial.begin, ledgerEnd, offsetOrdering)
      _ <- partial.end.fold[Result[Unit]](Right(()))(
        offsetIsBeforeEndIfAbsolute("End", _, ledgerEnd, offsetOrdering))
      convertedFilter <- transactionFilterToPartySet(
        partial.transactionFilter,
        "filter.filters_by_party")
    } yield {
      transaction.GetTransactionTreesRequest(
        partial.ledgerId,
        partial.begin,
        partial.end,
        convertedFilter,
        req.verbose,
        req.traceContext.map(toBrave))
    }
  }

  def validateLedgerEnd(req: GetLedgerEndRequest): Result[transaction.GetLedgerEndRequest] = {
    for {
      ledgerId <- matchId(LedgerId(req.ledgerId))
    } yield {
      transaction.GetLedgerEndRequest(ledgerId, req.traceContext.map(toBrave))
    }
  }

  def validateTransactionById(
      req: GetTransactionByIdRequest): Result[transaction.GetTransactionByIdRequest] = {
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
        req.traceContext.map(toBrave))
    }
  }

  def validateTransactionByEventId(
      req: GetTransactionByEventIdRequest): Result[transaction.GetTransactionByEventIdRequest] = {
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
        req.traceContext.map(toBrave))
    }
  }

  private def transactionFilterToPartySet(
      transactionFilter: TransactionFilter,
      fieldName: String
  ) =
    transactionFilter.filtersByParty
      .collectFirst {
        case (party, Filters(Some(inclusive))) =>
          invalidArgument(
            s"$party attempted subscription for templates ${inclusive.templateIds.mkString("[", ", ", "]")}. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC.")
      }
      .fold(partyValidator.requireKnownParties(transactionFilter.filtersByParty.keys))(Left(_))

}
