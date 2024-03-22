// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import io.grpc.StatusRuntimeException
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.syntax.traverse.*

class TransactionFilterValidator(
    resolveTemplateIds: Ref.QualifiedName => ContextualizedErrorLogger => Either[
      StatusRuntimeException,
      Iterable[Ref.Identifier],
    ],
    upgradingEnabled: Boolean,
) {

  import FieldValidator.*
  import ValidationErrors.*

  def validate(
      txFilter: TransactionFilter
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.TransactionFilter] = {
    if (txFilter.filtersByParty.isEmpty) {
      Left(invalidArgument("filtersByParty cannot be empty"))
    } else {
      for {
        _ <- validateAllFilterDefinitionsAreEitherDeprecatedOrCurrent(txFilter)
        convertedFilters <- txFilter.filtersByParty.toList.traverse { case (k, v) =>
          for {
            key <- requireParty(k)
            value <- validateFilters(
              v,
              resolveTemplateIds(_)(contextualizedErrorLogger),
              upgradingEnabled,
            )
          } yield key -> value
        }
      } yield domain.TransactionFilter(convertedFilters.toMap)
    }
  }

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.transaction_filter.*")
  private def validateFilters(
      filters: Filters,
      resolvePackageIds: Ref.QualifiedName => Either[StatusRuntimeException, Iterable[
        Ref.Identifier
      ]],
      upgradingEnabled: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Filters] = {
    filters.inclusive
      .fold[Either[StatusRuntimeException, domain.Filters]](Right(domain.Filters.noFilter)) {
        inclusive =>
          for {
            validatedIdents <-
              inclusive.templateIds.toList
                .traverse(
                  validatedTemplateIdWithPackageIdResolutionFallback(
                    _,
                    includeCreatedEventBlob = false,
                    resolvePackageIds,
                  )(upgradingEnabled)
                )
                .map(_.flatten)
            validatedTemplates <-
              inclusive.templateFilters.toList
                .traverse(validateTemplateFilter(_, resolvePackageIds, upgradingEnabled))
                .map(_.flatten)
            validatedInterfaces <-
              inclusive.interfaceFilters.toList traverse validateInterfaceFilter
          } yield domain.Filters(
            Some(
              domain.InclusiveFilters(
                (validatedIdents ++ validatedTemplates).toSet,
                validatedInterfaces.toSet,
              )
            )
          )
      }
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
      resolvePackageIds: Ref.QualifiedName => Either[StatusRuntimeException, Iterable[
        Ref.Identifier
      ]],
      upgradingEnabled: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Iterable[domain.TemplateFilter]] = {
    for {
      templateId <- requirePresence(filter.templateId, "templateId")
      validatedIds <- validatedTemplateIdWithPackageIdResolutionFallback(
        templateId,
        filter.includeCreatedEventBlob,
        resolvePackageIds,
      )(upgradingEnabled)
    } yield validatedIds
  }

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
