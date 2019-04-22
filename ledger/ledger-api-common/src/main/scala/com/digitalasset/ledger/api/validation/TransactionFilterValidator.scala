// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.InclusiveFilters
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.platform.server.api.validation.FieldValidations._
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.api.validation.FieldValidations._
import io.grpc.StatusRuntimeException
import scalaz.Traverse
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

class TransactionFilterValidator(identifierResolver: IdentifierResolver) {

  def validate(
      txFilter: TransactionFilter,
      fieldName: String): Either[StatusRuntimeException, domain.TransactionFilter] = {
    val convertedFilters =
      txFilter.filtersByParty.toList.traverseU {
        case (k, v) =>
          for {
            key <- requireSimpleString(k)
            value <- validateFilters(v)
          } yield key -> value
      }
    convertedFilters.map(m => domain.TransactionFilter(m.toMap))
  }

  def validateFilters(filters: Filters): Either[StatusRuntimeException, domain.Filters] = {
    filters.inclusive
      .fold[Either[StatusRuntimeException, domain.Filters]](Right(domain.Filters.noFilter)) {
        inclusive =>
          val validatedIdents =
            Traverse[List].traverseU[Identifier, Either[StatusRuntimeException, Ref.Identifier]](
              inclusive.templateIds.toList)((id: Identifier) =>
              identifierResolver.resolveIdentifier(id))
          validatedIdents.map(ids => domain.Filters(Some(InclusiveFilters(ids.toSet))))
      }
  }
}
