// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.http.domain.TemplateId
import com.daml.http.util.IdentifierConverters.apiIdentifier
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.refinements.{ApiTypes => lar}

object Transactions {
  def allCreatedEvents(transaction: Transaction): ImmArraySeq[CreatedEvent] =
    transaction.events.iterator.flatMap(_.event.created.toList).to[ImmArraySeq]

  def allArchivedEvents(transaction: Transaction): ImmArraySeq[ArchivedEvent] =
    transaction.events.iterator.flatMap(_.event.archived.toList).to[ImmArraySeq]

  def transactionFilterFor(
      party: lar.Party,
      templateIds: List[TemplateId.RequiredPkg]): TransactionFilter = {

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(InclusiveFilters(templateIds.map(apiIdentifier))))
    TransactionFilter(Map(lar.Party.unwrap(party) -> filters))
  }
}
