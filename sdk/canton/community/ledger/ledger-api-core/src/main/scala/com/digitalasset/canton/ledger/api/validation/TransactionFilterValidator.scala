// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.transaction_filter.{
  Filters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import io.grpc.StatusRuntimeException
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

class TransactionFilterValidator(upgradingEnabled: Boolean) {

  import FieldValidator.*
  import ValidationErrors.*

  def validate(
      txFilter: TransactionFilter
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.TransactionFilter] =
    if (txFilter.filtersByParty.isEmpty) {
      Left(invalidArgument("filtersByParty cannot be empty"))
    } else {
      for {
        convertedFilters <- txFilter.filtersByParty.toList.traverse { case (party, filters) =>
          for {
            key <- requireParty(party)
            validatedFilters <- validateFilters(
              filters,
              upgradingEnabled,
            )
          } yield key -> validatedFilters
        }
      } yield domain.TransactionFilter(convertedFilters.toMap)
    }

  // Allow using deprecated Protobuf fields for backwards compatibility
  private def validateFilters(
      filters: Filters,
      upgradingEnabled: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Filters] =
    filters.inclusive
      .fold[Either[StatusRuntimeException, domain.Filters]](Right(domain.Filters.noFilter)) {
        inclusive =>
          for {
            validatedTemplates <-
              inclusive.templateFilters.toList.traverse(validateTemplateFilter(_, upgradingEnabled))
            validatedInterfaces <-
              inclusive.interfaceFilters.toList traverse validateInterfaceFilter
          } yield domain.Filters(
            Some(
              domain.InclusiveFilters(
                validatedTemplates.toSet,
                validatedInterfaces.toSet,
              )
            )
          )
      }

  private def validateTemplateFilter(
      filter: TemplateFilter,
      upgradingEnabled: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.TemplateFilter] =
    for {
      templateId <- requirePresence(filter.templateId, "templateId")
      validatedIds <- validateIdentifierWithPackageUpgrading(
        templateId,
        filter.includeCreatedEventBlob,
      )(upgradingEnabled)
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
}
