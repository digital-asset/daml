// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.http.domain.{PartySet, TemplateId}
import com.daml.fetchcontracts.util.IdentifierConverters.apiIdentifier
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.refinements.{ApiTypes => lar}

object Transactions {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def allCreatedEvents(transaction: Transaction): ImmArraySeq[CreatedEvent] =
    transaction.events.iterator.flatMap(_.event.created.toList).to(ImmArraySeq)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def allArchivedEvents(transaction: Transaction): ImmArraySeq[ArchivedEvent] =
    transaction.events.iterator.flatMap(_.event.archived.toList).to(ImmArraySeq)

  def transactionFilterFor(
      parties: PartySet,
      templateIds: List[TemplateId.RequiredPkg],
  ): TransactionFilter = {
    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(InclusiveFilters(templateIds.map(apiIdentifier))))
    TransactionFilter(lar.Party.unsubst((parties: Set[lar.Party]).toVector).map(_ -> filters).toMap)
  }
}
