// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.transaction_filter.{
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
        _ <- validateAllFilterDefinitionsAreEitherDeprecatedOrCurrent(txFilter)
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
  @annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.transaction_filter.*")
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
            validateIdentifiers <-
              inclusive.templateIds.toList
                .traverse(
                  validateIdentifierWithPackageUpgrading(
                    _,
                    includeCreatedEventBlob = false,
                  )(upgradingEnabled)
                )
            validatedTemplates <-
              inclusive.templateFilters.toList.traverse(validateTemplateFilter(_, upgradingEnabled))
            validatedInterfaces <-
              inclusive.interfaceFilters.toList traverse validateInterfaceFilter
          } yield domain.Filters(
            Some(
              domain.InclusiveFilters(
                (validateIdentifiers ++ validatedTemplates).toSet,
                validatedInterfaces.toSet,
              )
            )
          )
      }

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.transaction_filter.*")
  private def validateAllFilterDefinitionsAreEitherDeprecatedOrCurrent(txFilter: TransactionFilter)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    txFilter.filtersByParty.valuesIterator
      .flatMap(_.inclusive.iterator)
      .foldLeft(Right((false, false)): Either[StatusRuntimeException, (Boolean, Boolean)]) {
        case (Right((deprecatedAcc, currentAcc)), inclusiveFilters) =>
          val templateIdsPresent = inclusiveFilters.templateIds.nonEmpty
          val templateFiltersPresent = inclusiveFilters.templateFilters.nonEmpty
          val interfaceFiltersPayloadFlag =
            inclusiveFilters.interfaceFilters.exists(_.includeCreatedEventBlob)
          val deprecated = templateIdsPresent
          val current = templateFiltersPresent || interfaceFiltersPayloadFlag
          val deprecatedAggr = deprecated || deprecatedAcc
          val currentAggr = current || currentAcc
          if (deprecatedAggr && currentAggr)
            Left(
              invalidArgument(
                "Transaction filter should be defined entirely either with deprecated fields, or with non-deprecated fields. Mixed definition is not allowed."
              )
            )
          else Right((deprecatedAggr, currentAggr))
        case (err, _) => err
      }
      .map(_ => ())

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
