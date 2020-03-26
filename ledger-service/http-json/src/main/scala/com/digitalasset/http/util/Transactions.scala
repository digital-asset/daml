// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.http.domain.TemplateId
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}

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
