// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.WorkflowConfig
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc,
}
import com.daml.ledger.api.v1.value.Identifier
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class TransactionService(
    channel: Channel,
    ledgerId: String,
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: TransactionServiceGrpc.TransactionServiceStub =
    TransactionServiceGrpc.stub(channel)

  def transactions[Result](
      config: WorkflowConfig.StreamConfig.TransactionsStreamConfig,
      observer: ObserverWithResult[GetTransactionsResponse, Result],
  ): Future[Result] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      filters = config.filters,
      beginOffset = config.beginOffset,
      endOffset = config.endOffset,
    )
    service.getTransactions(request, observer)
    logger.info("Started fetching transactions")
    observer.result
  }

  def transactionTrees[Result](
      config: WorkflowConfig.StreamConfig.TransactionTreesStreamConfig,
      observer: ObserverWithResult[
        GetTransactionTreesResponse,
        Result,
      ],
  ): Future[Result] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      filters = config.filters,
      beginOffset = config.beginOffset,
      endOffset = config.endOffset,
    )
    service.getTransactionTrees(request, observer)
    logger.info("Started fetching transaction trees")
    observer.result
  }

  private def getTransactionsRequest(
      ledgerId: String,
      filters: Map[String, List[Identifier]],
      beginOffset: Option[LedgerOffset],
      endOffset: Option[LedgerOffset],
  ): GetTransactionsRequest = {
    val getTransactionsRequest = GetTransactionsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withBegin(beginOffset.getOrElse(ledgerBeginOffset))
      .withFilter(StreamFilters.transactionFilters(filters))

    endOffset match {
      case Some(end) => getTransactionsRequest.withEnd(end)
      case None => getTransactionsRequest
    }
  }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

}
