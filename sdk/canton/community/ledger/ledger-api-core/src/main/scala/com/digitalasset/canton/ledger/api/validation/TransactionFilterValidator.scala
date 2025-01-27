// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  Filters,
  InterfaceFilter as ProtoInterfaceFilter,
  TemplateFilter as ProtoTemplateFilter,
  TransactionFilter as ProtoTransactionFilter,
  WildcardFilter,
}
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  InterfaceFilter,
  TemplateFilter,
  TemplateWildcardFilter,
  TransactionFilter,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import io.grpc.StatusRuntimeException
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

object TransactionFilterValidator {

  import FieldValidator.*
  import ValidationErrors.*

  def validate(
      txFilter: ProtoTransactionFilter
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, TransactionFilter] =
    if (txFilter.filtersByParty.isEmpty && txFilter.filtersForAnyParty.isEmpty) {
      Left(invalidArgument("filtersByParty and filtersForAnyParty cannot be empty simultaneously"))
    } else {
      for {
        convertedFilters <- txFilter.filtersByParty.toList.traverse { case (party, filters) =>
          for {
            key <- requireParty(party)
            validatedFilters <- validateFilters(
              filters
            )
          } yield key -> validatedFilters
        }
        filtersForAnyParty <- txFilter.filtersForAnyParty.toList
          .traverse(validateFilters)
          .map(_.headOption)
      } yield TransactionFilter(
        filtersByParty = convertedFilters.toMap,
        filtersForAnyParty = filtersForAnyParty,
      )
    }

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
