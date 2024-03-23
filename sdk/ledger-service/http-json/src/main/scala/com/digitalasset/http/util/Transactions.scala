// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.http.domain.{ContractTypeId, PartySet}
import com.daml.fetchcontracts.AcsTxStreams.transactionFilter
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter

object Transactions {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def allCreatedEvents(transaction: Transaction): ImmArraySeq[CreatedEvent] =
    transaction.events.iterator.flatMap(_.event.created.toList).to(ImmArraySeq)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def allArchivedEvents(transaction: Transaction): ImmArraySeq[ArchivedEvent] =
    transaction.events.iterator.flatMap(_.event.archived.toList).to(ImmArraySeq)

  @deprecated("use AcsTxStreams.transactionFilter instead", since = "2.4.0")
  def transactionFilterFor(
      parties: PartySet,
      contractTypeIds: List[ContractTypeId.Resolved],
  ): TransactionFilter =
    transactionFilter(parties, contractTypeIds)
}
