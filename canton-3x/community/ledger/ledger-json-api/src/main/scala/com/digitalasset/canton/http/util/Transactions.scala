// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.canton.fetchcontracts.AcsTxStreams.transactionFilter
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.canton.http.domain.{ContractTypeId, PartySet}

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
