// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.InclusiveFilters
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import io.grpc.StatusRuntimeException
import scalaz.Traverse
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.map

class TransactionFilterValidator(identifierResolver: IdentifierResolver) {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def stringMap = map.mapInstance[String]

  def validate(
      txFilter: TransactionFilter,
      fieldName: String): Either[StatusRuntimeException, domain.TransactionFilter] = {
    val convertedFilters = stringMap.traverseU(txFilter.filtersByParty)(validateFilters)
    convertedFilters.map { m =>
      domain.TransactionFilter(m.map { case (k, v) => domain.Party(k) -> v })
    }

  }

  def validateFilters(filters: Filters): Either[StatusRuntimeException, domain.Filters] = {
    filters.inclusive
      .fold[Either[StatusRuntimeException, domain.Filters]](Right(domain.Filters.noFilter)) {
        inclusive =>
          val validatedIdents =
            Traverse[List].traverseU[Identifier, Either[StatusRuntimeException, domain.Identifier]](
              inclusive.templateIds.toList)((id: Identifier) =>
              identifierResolver.resolveIdentifier(id))
          validatedIdents.map(ids => domain.Filters(Some(InclusiveFilters(ids.toSet))))
      }
  }
}
