// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, LedgerOffset, optionalLedgerId}
import com.daml.ledger.api.messages.transaction
import com.daml.ledger.api.messages.transaction.GetTransactionTreesRequest
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.daml.platform.server.api.validation.FieldValidations
import io.grpc.StatusRuntimeException

object TransactionServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

}
class TransactionServiceRequestValidator(
    ledgerId: LedgerId,
    partyNameChecker: PartyNameChecker,
) {

  import TransactionServiceRequestValidator.Result

  private val partyValidator =
    new PartyValidator(partyNameChecker)

  import FieldValidations._
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
      convertedFilter <- TransactionFilterValidator.validate(partial.transactionFilter)
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

  def validateEventsByContractId(
      req: GetEventsByContractIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetEventsByContractIdRequest] = {
    for {
      contractId <- requireContractId(req.contractId, "contract_id")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- partyValidator.requireKnownParties(req.requestingParties)
    } yield {
      transaction.GetEventsByContractIdRequest(contractId, parties)
    }
  }

  def validateEventsByContractKey(
      req: GetEventsByContractKeyRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[transaction.GetEventsByContractKeyRequest] = {

    for {
      apiContractKey <- requirePresence(req.contractKey, "contract_key")
      contractId <- ValueValidator.validateValue(apiContractKey)
      apiTemplateId <- requirePresence(req.templateId, "template_id")
      templateId <- FieldValidations.validateIdentifier(apiTemplateId)
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      requestingParties <- partyValidator.requireKnownParties(req.requestingParties)
      startExclusive <- LedgerOffsetValidator.validateOptional(
        req.beginExclusive,
        "start_exclusive",
      )
      endInclusive <- LedgerOffsetValidator.validateOptional(
        req.endInclusive,
        "end_inclusive",
      )
    } yield {

      transaction.GetEventsByContractKeyRequest(
        contractKey = contractId,
        templateId = templateId,
        requestingParties = requestingParties,
        maxEvents = if (req.maxEvents != 0) req.maxEvents else 1000,
        startExclusive = startExclusive.getOrElse(LedgerOffset.LedgerBegin),
        endInclusive = endInclusive.getOrElse(LedgerOffset.LedgerEnd),
      )
    }

  }

}
