// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.InclusiveFilters
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.server.api.validation.FieldValidations._
import io.grpc.StatusRuntimeException
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

object TransactionFilterValidator {

  def validate(
      txFilter: TransactionFilter
  )(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.TransactionFilter] = {
    if (txFilter.filtersByParty.isEmpty) {
      Left(ErrorFactories.invalidArgument(None)("filtersByParty cannot be empty"))
    } else {
      val convertedFilters =
        txFilter.filtersByParty.toList.traverse { case (k, v) =>
          for {
            key <- requireParty(k)
            value <- validateFilters(v)
          } yield key -> value
        }
      convertedFilters.map(m => domain.TransactionFilter(m.toMap))
    }
  }

  def validateFilters(filters: Filters)(implicit
      errorCodeLoggingContext: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Filters] = {
    filters.inclusive
      .fold[Either[StatusRuntimeException, domain.Filters]](Right(domain.Filters.noFilter)) {
        inclusive =>
          val validatedIdents =
            inclusive.templateIds.toList traverse validateIdentifier
          validatedIdents.map(ids => domain.Filters(Some(InclusiveFilters(ids.toSet))))
      }
  }
}
