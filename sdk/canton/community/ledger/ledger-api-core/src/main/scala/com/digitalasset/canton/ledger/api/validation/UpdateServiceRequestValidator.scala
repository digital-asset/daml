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
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.api.messages.update
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.api.{
  ParticipantAuthorizationFormat,
  TopologyFormat,
  TransactionFormat,
  UpdateFormat,
  UpdateId,
}
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
  ): Result[update.GetUpdatesRequest] =
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
      eventFormat <- FormatValidator.validate(partial.transactionFilter, req.verbose)
      filterPartiesO = eventFormat.filtersForAnyParty match {
        case Some(_) => None // wildcard
        case None => Some(eventFormat.filtersByParty.keySet)
      }
      // TODO(#23517) fill the transaction and reassignment formats separately when GetUpdateRequest includes the
      //  UpdateFormat field
      updateFormat = UpdateFormat(
        includeTransactions =
          Some(TransactionFormat(eventFormat = eventFormat, transactionShape = AcsDelta)),
        includeReassignments = Some(eventFormat),
        includeTopologyEvents = Some(
          TopologyFormat(
            Some(
              ParticipantAuthorizationFormat(
                filterPartiesO
              )
            )
          )
        ),
      )
    } yield {
      update.GetUpdatesRequest(
        partial.begin,
        partial.end,
        updateFormat,
      )
    }

  // TODO(#23504) cleanup
  def validateForTrees(
      req: GetUpdatesRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[update.GetUpdatesRequestForTrees] =
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
      eventFormat <- FormatValidator.validate(partial.transactionFilter, req.verbose)
    } yield {
      update.GetUpdatesRequestForTrees(
        partial.begin,
        partial.end,
        eventFormat,
      )
    }

  def validateTransactionById(
      req: GetTransactionByIdRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[update.GetTransactionByIdRequest] =
    for {
      _ <- requireNonEmptyString(req.updateId, "update_id")
      trId <- requireLedgerString(req.updateId)
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- requireParties(req.requestingParties.toSet)
    } yield {
      update.GetTransactionByIdRequest(
        UpdateId(trId),
        parties,
      )
    }

  def validateTransactionByOffset(
      req: GetTransactionByOffsetRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Result[update.GetTransactionByOffsetRequest] =
    for {
      offset <- ParticipantOffsetValidator.validatePositive(req.offset, "offset")
      _ <- requireNonEmpty(req.requestingParties, "requesting_parties")
      parties <- requireParties(req.requestingParties.toSet)
    } yield {
      update.GetTransactionByOffsetRequest(
        offset,
        parties,
      )
    }
}
