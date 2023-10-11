// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.transaction_filter.{Filters, InterfaceFilter, TransactionFilter}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.InclusiveFilters
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
                    resolvePackageIds,
                  )(upgradingEnabled)
                )
                .map(_.flatten)
            validatedInterfaces <-
              inclusive.interfaceFilters.toList traverse validateInterfaceFilter
          } yield domain.Filters(
            Some(InclusiveFilters(validatedIdents.toSet, validatedInterfaces.toSet))
          )
      }
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
      includeCreateArgumentsBlob = filter.includeCreateArgumentsBlob,
    )
  }
}
