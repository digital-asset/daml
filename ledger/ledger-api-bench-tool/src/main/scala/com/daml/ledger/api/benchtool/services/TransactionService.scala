// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.metrics.{LogOnlyObserver, TransactionsStreamObserver}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  TransactionServiceGrpc,
}
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class TransactionService(channel: Channel, ledgerId: String) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: TransactionServiceGrpc.TransactionServiceStub =
    TransactionServiceGrpc.stub(channel)

  // TODO: add filters
  def transactions(party: String): Future[Unit] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = party,
      beginOffset = ledgerBeginOffset,
      endOffset = dummyEndOffset,
    )
    val reportingPeriod: Long = 500
    val observer = new TransactionsStreamObserver(reportingPeriod, logger)
    service.getTransactions(request, observer)
    logger.info("Started fetching transactions")
    observer.completion
  }

  def transactionTrees(party: String): Future[Unit] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = party,
      beginOffset = ledgerBeginOffset,
      endOffset = ledgerEndOffset,
    )
    val observer = new LogOnlyObserver[GetTransactionTreesResponse](logger)
    service.getTransactionTrees(request, observer)
    logger.info("Started fetching transaction trees")
    observer.completion
  }

  private def getTransactionsRequest(
      ledgerId: String,
      party: String,
      beginOffset: LedgerOffset,
      endOffset: LedgerOffset,
  ): GetTransactionsRequest = {
    println(endOffset)
    GetTransactionsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withBegin(beginOffset)
      .withEnd(endOffset)
      .withFilter(partyFilter(party))
  }

  private def partyFilter(party: String): TransactionFilter = {
    // TODO: actual templates filter
    val templatesFilter = Filters.defaultInstance
    TransactionFilter()
      .withFiltersByParty(Map(party -> templatesFilter))
  }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

  private def dummyEndOffset: LedgerOffset =
    LedgerOffset().withAbsolute("0000000000000038")

  private def ledgerEndOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)

}
