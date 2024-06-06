// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  Filters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
  WildcardFilter,
}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import io.grpc.StatusRuntimeException
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

object TransactionFilterValidator {

  import FieldValidator.*
  import ValidationErrors.*

  def validate(
      txFilter: TransactionFilter
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.TransactionFilter] =
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
      } yield domain.TransactionFilter(
        filtersByParty = convertedFilters.toMap,
        filtersForAnyParty = filtersForAnyParty,
      )
    }

  // Allow using deprecated Protobuf fields for backwards compatibility
  private def validateFilters(filters: Filters)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Filters] = {
    val extractedFilters = filters.cumulative.map(_.identifierFilter)
    val empties = extractedFilters.filter(_.isEmpty)
    lazy val templateFilters = extractedFilters.collect({ case IdentifierFilter.TemplateFilter(f) =>
      f
    })
    lazy val interfaceFilters = extractedFilters.collect({
      case IdentifierFilter.InterfaceFilter(f) =>
        f
    })
    lazy val wildcardFilters = extractedFilters.collect({ case IdentifierFilter.WildcardFilter(f) =>
      f
    })

    if (empties.size == extractedFilters.size) Right(domain.Filters(None))
    else {
      for {
        validatedTemplates <-
          templateFilters.toList.traverse(validateTemplateFilter(_))
        validatedInterfaces <-
          interfaceFilters.toList.traverse(validateInterfaceFilter(_))
        wildcardO = mergeWildcardFilters(wildcardFilters)
      } yield domain.Filters(
        Some(
          domain.CumulativeFilter(
            validatedTemplates.toSet,
            validatedInterfaces.toSet,
            wildcardO,
          )
        )
      )
    }
  }

  private def validateTemplateFilter(filter: TemplateFilter)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.TemplateFilter] =
    for {
      templateId <- requirePresence(filter.templateId, "templateId")
      validatedIds <- validateIdentifierWithPackageUpgrading(
        templateId,
        filter.includeCreatedEventBlob,
      )
    } yield validatedIds

  private def validateInterfaceFilter(filter: InterfaceFilter)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.InterfaceFilter] = {
    for {
      interfaceId <- requirePresence(filter.interfaceId, "interfaceId")
      validatedId <- validateIdentifier(interfaceId)
    } yield domain.InterfaceFilter(
      interfaceId = validatedId,
      includeView = filter.includeInterfaceView,
      includeCreatedEventBlob = filter.includeCreatedEventBlob,
    )
  }

  private def mergeWildcardFilters(
      filters: Seq[WildcardFilter]
  ): Option[domain.TemplateWildcardFilter] =
    if (filters.isEmpty) None
    else
      Some(
        domain.TemplateWildcardFilter(
          includeCreatedEventBlob = filters.exists(_.includeCreatedEventBlob)
        )
      )

}
