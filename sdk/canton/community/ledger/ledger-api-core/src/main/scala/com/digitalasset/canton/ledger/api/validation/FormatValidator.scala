// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat as ProtoEventFormat,
  Filters,
  InterfaceFilter as ProtoInterfaceFilter,
  ParticipantAuthorizationTopologyFormat as ProtoParticipantAuthorizationTopologyFormat,
  TemplateFilter as ProtoTemplateFilter,
  TopologyFormat as ProtoTopologyFormat,
  TransactionFilter as ProtoTransactionFilter,
  TransactionFormat as ProtoTransactionFormat,
  TransactionShape as ProtoTransactionShape,
  UpdateFormat as ProtoUpdateFormat,
  WildcardFilter,
}
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  InterfaceFilter,
  ParticipantAuthorizationFormat,
  TemplateFilter,
  TemplateWildcardFilter,
  TopologyFormat,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import io.grpc.StatusRuntimeException
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

object FormatValidator {

  import FieldValidator.*
  import ValidationErrors.*

  // TODO(i23504) Cleanup
  def validate(
      txFilter: ProtoTransactionFilter,
      verbose: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, EventFormat] =
    validate(ProtoEventFormat(txFilter.filtersByParty, txFilter.filtersForAnyParty, verbose))

  // TODO(i23504) Cleanup
  def validateLegacyToUpdateFormat(
      txFilter: ProtoTransactionFilter,
      verbose: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, UpdateFormat] =
    for {
      eventFormat <- FormatValidator.validate(txFilter, verbose)
      filterPartiesO = eventFormat.filtersForAnyParty match {
        case Some(_) => None // wildcard
        case None => Some(eventFormat.filtersByParty.keySet)
      }
    } yield UpdateFormat(
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

  def validate(eventFormat: ProtoEventFormat)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, EventFormat] =
    if (eventFormat.filtersByParty.isEmpty && eventFormat.filtersForAnyParty.isEmpty) {
      Left(invalidArgument("filtersByParty and filtersForAnyParty cannot be empty simultaneously"))
    } else {
      for {
        convertedFilters <- eventFormat.filtersByParty.toList.traverse { case (party, filters) =>
          for {
            key <- requireParty(party)
            validatedFilters <- validateFilters(
              filters
            )
          } yield key -> validatedFilters
        }
        filtersForAnyParty <- eventFormat.filtersForAnyParty.toList
          .traverse(validateFilters)
          .map(_.headOption)
      } yield EventFormat(
        filtersByParty = convertedFilters.toMap,
        filtersForAnyParty = filtersForAnyParty,
        verbose = eventFormat.verbose,
      )
    }

  def validate(
      protoParticipantAuthorizationTopologyFormat: ProtoParticipantAuthorizationTopologyFormat
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ParticipantAuthorizationFormat] =
    protoParticipantAuthorizationTopologyFormat.parties.toList
      .traverse(requirePartyField(_, "parties"))
      .map(parties =>
        ParticipantAuthorizationFormat(
          // empty means: for all parties
          if (parties.isEmpty) None
          else Some(parties.toSet)
        )
      )

  def validate(protoTopologyFormat: ProtoTopologyFormat)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TopologyFormat] =
    for {
      participantAuthorizationPartiesO <- validateOptional(
        protoTopologyFormat.includeParticipantAuthorizationEvents
      )(validate)
    } yield TopologyFormat(participantAuthorizationPartiesO)

  def validate(protoTransactionShape: ProtoTransactionShape)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TransactionShape] = protoTransactionShape match {
    case ProtoTransactionShape.TRANSACTION_SHAPE_UNSPECIFIED =>
      Left(RequestValidationErrors.MissingField.Reject("transaction_shape").asGrpcError)
    case ProtoTransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS =>
      Right(TransactionShape.LedgerEffects)
    case ProtoTransactionShape.TRANSACTION_SHAPE_ACS_DELTA =>
      Right(TransactionShape.AcsDelta)
    case ProtoTransactionShape.Unrecognized(value) =>
      Left(
        RequestValidationErrors.InvalidArgument
          .Reject(s"transaction_shape is defined with invalid value $value")
          .asGrpcError
      )
  }

  def validate(protoTransactionFormat: ProtoTransactionFormat)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TransactionFormat] =
    for {
      transactionShape <- validate(protoTransactionFormat.transactionShape)
      eventFormat <- requireOptional(protoTransactionFormat.eventFormat, "event_format")(validate)
    } yield TransactionFormat(eventFormat, transactionShape)

  def validate(protoUpdateFormat: ProtoUpdateFormat)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, UpdateFormat] =
    for {
      includeTransactions <- validateOptional(protoUpdateFormat.includeTransactions)(validate)
      includeReassignments <- validateOptional(protoUpdateFormat.includeReassignments)(validate)
      includeTopologyEvents <- validateOptional(protoUpdateFormat.includeTopologyEvents)(validate)
    } yield UpdateFormat(includeTransactions, includeReassignments, includeTopologyEvents)

  // Allow using deprecated Protobuf fields for backwards compatibility
  private def validateFilters(filters: Filters)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, CumulativeFilter] = {
    val extractedFilters = filters.cumulative.map(_.identifierFilter)
    val empties = extractedFilters.filter(_.isEmpty)
    lazy val templateFilters = extractedFilters.collect { case IdentifierFilter.TemplateFilter(f) =>
      f
    }
    lazy val interfaceFilters = extractedFilters.collect {
      case IdentifierFilter.InterfaceFilter(f) =>
        f
    }
    lazy val wildcardFilters = extractedFilters.collect { case IdentifierFilter.WildcardFilter(f) =>
      f
    }

    if (empties.sizeIs == extractedFilters.size)
      Right(CumulativeFilter.templateWildcardFilter())
    else {
      for {
        _ <- validateNonEmptyFilters(
          templateFilters,
          interfaceFilters,
          wildcardFilters,
        )
        validatedTemplates <-
          templateFilters.toList.traverse(validateTemplateFilter(_))
        validatedInterfaces <-
          interfaceFilters.toList.traverse(validateInterfaceFilter(_))
        wildcardO = mergeWildcardFilters(wildcardFilters)
      } yield CumulativeFilter(
        validatedTemplates.toSet,
        validatedInterfaces.toSet,
        wildcardO,
      )
    }
  }

  private def validateTemplateFilter(filter: ProtoTemplateFilter)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TemplateFilter] =
    for {
      templateId <- requirePresence(filter.templateId, "templateId")
      typeConRef <- validateTypeConRef(templateId)
    } yield TemplateFilter(
      templateTypeRef = typeConRef,
      includeCreatedEventBlob = filter.includeCreatedEventBlob,
    )

  private def validateInterfaceFilter(filter: ProtoInterfaceFilter)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, InterfaceFilter] =
    for {
      interfaceId <- requirePresence(filter.interfaceId, "interfaceId")
      typeConRef <- validateTypeConRef(interfaceId)
    } yield InterfaceFilter(
      interfaceTypeRef = typeConRef,
      includeView = filter.includeInterfaceView,
      includeCreatedEventBlob = filter.includeCreatedEventBlob,
    )

  private def validateNonEmptyFilters(
      templateFilters: Seq[ProtoTemplateFilter],
      interfaceFilters: Seq[ProtoInterfaceFilter],
      wildcardFilters: Seq[WildcardFilter],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    Either.cond(
      !(templateFilters.isEmpty && interfaceFilters.isEmpty && wildcardFilters.isEmpty),
      (),
      RequestValidationErrors.InvalidArgument
        .Reject(
          "requests with empty template, interface and wildcard filters are not supported"
        )
        .asGrpcError,
    )

  private def mergeWildcardFilters(
      filters: Seq[WildcardFilter]
  ): Option[TemplateWildcardFilter] =
    if (filters.isEmpty) None
    else
      Some(
        TemplateWildcardFilter(
          includeCreatedEventBlob = filters.exists(_.includeCreatedEventBlob)
        )
      )

}
