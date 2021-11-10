// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.WorkflowConfig
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}

object StreamFilters {

  def transactionFilters(filters: List[WorkflowConfig.StreamConfig.PartyFilter]): TransactionFilter = {
    val byParty: Map[String, Filters] = filters.map { filter =>
      filter.templates match {
        case Nil =>
          filter.party -> Filters.defaultInstance
        case templateIds =>
          filter.party -> Filters.defaultInstance.withInclusive(
            InclusiveFilters.defaultInstance.addAllTemplateIds(templateIds)
          )
      }
    }.toMap

    TransactionFilter.defaultInstance.withFiltersByParty(byParty)
  }

}
