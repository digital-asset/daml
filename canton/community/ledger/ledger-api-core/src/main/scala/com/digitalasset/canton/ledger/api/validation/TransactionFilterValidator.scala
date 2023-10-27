// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
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
      val convertedFilters =
        txFilter.filtersByParty.toList.traverse { case (k, v) =>
          for {
            key <- requireParty(k)
            value <- validateFilters(
              v,
              resolveTemplateIds(_)(contextualizedErrorLogger),
              upgradingEnabled,
            )
          } yield key -> value
        }
      convertedFilters.map(m => domain.TransactionFilter(m.toMap))
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
            _ <- validatePresenceOfFilters(inclusive)
            validatedIdents <-
              inclusive.templateIds.toList
                .traverse(
                  validatedTemplateIdWithPackageIdResolutionFallback(
                    _,
                    includeCreateEventPayload = false,
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

  @annotation.nowarn(
    "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.transaction_filter\\.InclusiveFilters.*"
  )
  private def validatePresenceOfFilters(inclusive: InclusiveFilters)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] = {
    (inclusive.templateIds, inclusive.templateFilters) match {
      case (_ :: _, _ :: _) =>
        Left(invalidArgument("Either of `template_ids` or `template_filters` must be empty"))
      case (_, _) => Right(())
    }
  }

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
        filter.includeCreateEventPayload,
        resolvePackageIds,
      )(upgradingEnabled)
    } yield validatedIds
  }

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.transaction_filter.*")
  private def validateInterfaceFilter(filter: InterfaceFilter)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.InterfaceFilter] = {
    for {
      _ <- Either.cond(
        !filter.includeCreateArgumentsBlob || !filter.includeCreateEventPayload,
        (),
        invalidArgument(
          "Either of `includeCreateArgumentsBlob` and `includeCreateEventPayload` must be false"
        ),
      )
      interfaceId <- requirePresence(filter.interfaceId, "interfaceId")
      validatedId <- validateIdentifier(interfaceId)
    } yield domain.InterfaceFilter(
      interfaceId = validatedId,
      includeView = filter.includeInterfaceView,
      includeCreateArgumentsBlob = filter.includeCreateArgumentsBlob,
      includeCreateEventPayload = filter.includeCreateEventPayload,
    )
  }
}
