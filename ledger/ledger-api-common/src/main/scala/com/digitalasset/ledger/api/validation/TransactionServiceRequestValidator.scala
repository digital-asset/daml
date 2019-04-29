// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{LedgerId, LedgerIdTag, LedgerOffset}
import com.digitalasset.ledger.api.messages.transaction
import com.digitalasset.ledger.api.messages.transaction.GetTransactionTreesRequest
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest
}
import com.digitalasset.platform.server.api.validation.ErrorFactories._
import com.digitalasset.platform.server.api.validation.FieldValidations._
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.util.context.TraceContextConversions._
import io.grpc.StatusRuntimeException
import scalaz.Tag

class TransactionServiceRequestValidator(
    ledgerId: String,
    partyNameChecker: PartyNameChecker,
    identifierResolver: IdentifierResolver) {

  private type Result[X] = Either[StatusRuntimeException, X]

  private val filterValidator = new TransactionFilterValidator(identifierResolver)

  private def matchId(input: String): Result[LedgerId] =
    Tag.subst[String, Result[?], LedgerIdTag](matchLedgerId(ledgerId)(input))
  private val rightNone = Right(None)

  case class PartialValidation(
      ledgerId: domain.LedgerId,
      transactionFilter: TransactionFilter,
      begin: domain.LedgerOffset,
      end: Option[domain.LedgerOffset],
      traceContext: Option[TraceContext])

  private def commonValidations(req: GetTransactionsRequest): Result[PartialValidation] = {
    for {
      ledgerId <- matchId(req.ledgerId)
      filter <- requirePresence(req.filter, "filter")
      requiredBegin <- requirePresence(req.begin, "begin")
      convertedBegin <- LedgerOffsetValidator.validate(requiredBegin, "begin")
      convertedEnd <- req.end
        .fold[Result[Option[domain.LedgerOffset]]](rightNone)(end =>
          LedgerOffsetValidator.validate(end, "end").map(Some(_)))
      _ <- requireKnownParties(req.getFilter)
    } yield {

      PartialValidation(
        ledgerId,
        filter,
        convertedBegin,
        convertedEnd,
        req.traceContext.map(toBrave))
    }
  }

  private def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: LedgerOffset,
      ledgerEnd: LedgerOffset.Absolute,
      offsetOrdering: Ordering[LedgerOffset.Absolute]): Result[Unit] = {
    ledgerOffset match {
      case abs: LedgerOffset.Absolute if offsetOrdering.gt(abs, ledgerEnd) =>
        Left(
          invalidArgument(
            s"$offsetType offset ${abs.value} is after ledger end ${ledgerEnd.value}"))
      case _ => Right(())
    }
  }

  private def requireParties(parties: Traversable[String]): Result[Set[Party]] =
    parties.foldLeft[Result[Set[Party]]](Right(Set.empty)) { (acc, partyTxt) =>
      for {
        parties <- acc
        party <- requireSimpleString(partyTxt)
      } yield parties + party
    }

  private def requireKnownParties(transactionFilter: TransactionFilter): Result[Unit] =
    requireParties(transactionFilter.filtersByParty.keys).flatMap(requireKnownParties)

  private def requireKnownParties(partiesInRequest: Iterable[Party]): Result[Unit] = {
    val unknownParties = partiesInRequest.filterNot(partyNameChecker.isKnownParty)
    if (unknownParties.nonEmpty)
      Left(
        invalidArgument(
          s"Unknown parties: ${unknownParties.map(_.underlyingString).mkString("[", ", ", "]")}"))
    else Right(())
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
      convertedFilter <- filterValidator.validate(
        partial.transactionFilter,
        "filter.filters_by_party")
      _ <- requireKnownParties(req.getFilter)
    } yield {
      transaction.GetTransactionsRequest(
        partial.ledgerId,
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
      ledgerId <- matchId(req.ledgerId)
    } yield {
      transaction.GetLedgerEndRequest(ledgerId, req.traceContext.map(toBrave))
    }
  }

  def validateTransactionById(
      req: GetTransactionByIdRequest): Result[transaction.GetTransactionByIdRequest] = {
    for {
      ledgerId <- matchId(req.ledgerId)
      _ <- requireNonEmptyString(req.transactionId, "transaction_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- requireParties(req.requestingParties)
      _ <- requireKnownParties(parties)
    } yield {
      transaction.GetTransactionByIdRequest(
        ledgerId,
        domain.TransactionId(req.transactionId),
        parties,
        req.traceContext.map(toBrave))
    }
  }

  def validateTransactionByEventId(
      req: GetTransactionByEventIdRequest): Result[transaction.GetTransactionByEventIdRequest] = {
    for {
      ledgerId <- matchId(req.ledgerId)
      _ <- requireNonEmptyString(req.eventId, "event_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- requireParties(req.requestingParties)
      _ <- requireKnownParties(parties)
    } yield {
      transaction.GetTransactionByEventIdRequest(
        ledgerId,
        domain.EventId(req.eventId),
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
      .fold(requireParties(transactionFilter.filtersByParty.keys))(Left(_))

}
