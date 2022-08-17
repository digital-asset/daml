// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.InclusiveFilters
import com.daml.ledger.api.v1.transaction_filter.{Filters, InterfaceFilter, TransactionFilter}
import com.daml.lf.data.Ref
import com.daml.platform.packagemeta.PackageMetadata
import com.daml.platform.server.api.validation.FieldValidations
import io.grpc.StatusRuntimeException
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

object TransactionFilterValidator {

  import FieldValidations._
  import ValidationErrors._

  def validate(
      txFilter: TransactionFilter,
      packageMetadata: PackageMetadata,
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
            value <- validateFilters(v, packageMetadata)
          } yield key -> value
        }
      convertedFilters.map(m => domain.TransactionFilter(m.toMap))
    }
  }

  def validateFilters(filters: Filters, packageMetadata: PackageMetadata)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Filters] = {
    filters.inclusive
      .fold[Either[StatusRuntimeException, domain.Filters]](Right(domain.Filters.noFilter)) {
        inclusive =>
          for {
            validatedIdents <- inclusive.templateIds.toList traverse validateIdentifier
            _ <- validatedIdents traverse validateIdentifierExists(packageMetadata.templates)
            validatedInterfaces <-
              inclusive.interfaceFilters.toList traverse validateInterfaceFilter(
                packageMetadata.interfaces
              )
          } yield domain.Filters(
            Some(InclusiveFilters(validatedIdents.toSet, validatedInterfaces.toSet))
          )
      }
  }

  def validateInterfaceFilter(identifiers: Set[Ref.Identifier])(filter: InterfaceFilter)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.InterfaceFilter] = {
    for {
      interfaceId <- requirePresence(filter.interfaceId, "interfaceId")
      validatedId <- validateIdentifier(interfaceId)
      _ <- validateIdentifierExists(identifiers)(validatedId)
    } yield domain.InterfaceFilter(
      validatedId,
      filter.includeInterfaceView,
    )
  }

  def validateIdentifierExists(
      identifiers: Set[Ref.Identifier]
  )(identifier: Ref.Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] = {
    Either.cond(
      identifiers.contains(identifier),
      (),
      invalidArgument("Unknown identifier"),
    )
  }
}
