// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.Config
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
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
      config: Config.StreamConfig,
      observer: ObserverWithResult[GetTransactionsResponse, Result],
  ): Future[Result] = {
    val request = getTransactionsRequest(ledgerId, config)
    service.getTransactions(request, observer)
    logger.info("Started fetching transactions")
    observer.result
  }

  def transactionTrees[Result](
      config: Config.StreamConfig,
      observer: ObserverWithResult[
        GetTransactionTreesResponse,
        Result,
      ],
  ): Future[Result] = {
    val request = getTransactionsRequest(ledgerId, config)
    service.getTransactionTrees(request, observer)
    logger.info("Started fetching transaction trees")
    observer.result
  }

  private def getTransactionsRequest(
      ledgerId: String,
      config: Config.StreamConfig,
  ): GetTransactionsRequest = {
    GetTransactionsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withBegin(config.beginOffset.getOrElse(ledgerBeginOffset))
      .withEnd(config.endOffset.getOrElse(ledgerEndOffset))
      .withFilter(partyFilter(config.party, config.templateIds))
  }

  private def partyFilter(
      party: String,
      templateIds: Option[List[Identifier]],
  ): TransactionFilter = {
    val templatesFilter = templateIds match {
      case Some(ids) =>
        Filters.defaultInstance.withInclusive(
          InclusiveFilters.defaultInstance.addAllTemplateIds(ids)
        )
      case None =>
        Filters.defaultInstance
    }
    TransactionFilter()
      .withFiltersByParty(Map(party -> templatesFilter))
  }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

  private def ledgerEndOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)

}
