// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier

object StreamFilters {

  def transactionFilters(
      filters: Map[String, List[Identifier]]
  ): TransactionFilter = {
    val byParty: Map[String, Filters] = filters.map {
      case (party, Nil) =>
        party -> Filters.defaultInstance
      case (party, templateIds) =>
        party -> Filters.defaultInstance.withInclusive(
          InclusiveFilters.defaultInstance.addAllTemplateIds(templateIds)
        )
    }

    TransactionFilter.defaultInstance.withFiltersByParty(byParty)
  }

}
