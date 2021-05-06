// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import io.grpc.Channel
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.Filters
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionsRequest,
  GetTransactionsResponse,
  GetTransactionTreesResponse,
}
import org.slf4j.LoggerFactory

final class TransactionService(channel: Channel, ledgerId: String) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: TransactionServiceGrpc.TransactionServiceStub =
    TransactionServiceGrpc.stub(channel)

  // TODO: add filters
  def transactions(party: String): LogOnlyObserver[GetTransactionsResponse] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = party,
      beginOffset = ledgerBeginOffset,
      endOffset = ledgerEndOffset,
    )
    val observer = new LogOnlyObserver[GetTransactionsResponse]
    service.getTransactions(request, observer)
    logger.info("Started fetching transactions")
    observer
  }

  def transactionTrees(party: String): LogOnlyObserver[GetTransactionTreesResponse] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = party,
      beginOffset = ledgerBeginOffset,
      endOffset = ledgerEndOffset,
    )
    val observer = new LogOnlyObserver[GetTransactionTreesResponse]
    service.getTransactionTrees(request, observer)
    logger.info("Started fetching transaction trees")
    observer
  }

  private def getTransactionsRequest(
      ledgerId: String,
      party: String,
      beginOffset: LedgerOffset,
      endOffset: LedgerOffset,
  ): GetTransactionsRequest =
    GetTransactionsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withBegin(beginOffset)
      .withEnd(endOffset)
      .withFilter(partyFilter(party))

  private def partyFilter(party: String): TransactionFilter = {
    // TODO: actual templates filter
    val templatesFilter = Filters.defaultInstance
    TransactionFilter()
      .withFiltersByParty(Map(party -> templatesFilter))
  }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

  private def ledgerEndOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)

}
