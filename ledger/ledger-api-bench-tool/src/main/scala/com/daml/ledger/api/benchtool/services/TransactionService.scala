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
      config: Config.StreamConfig.TransactionsStreamConfig,
      observer: ObserverWithResult[GetTransactionsResponse, Result],
  ): Future[Result] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = config.party,
      templateIds = config.templateIds,
      beginOffset = config.beginOffset,
      endOffset = config.endOffset,
    )
    service.getTransactions(request, observer)
    logger.info("Started fetching transactions")
    observer.result
  }

  def transactionTrees[Result](
      config: Config.StreamConfig.TransactionTreesStreamConfig,
      observer: ObserverWithResult[
        GetTransactionTreesResponse,
        Result,
      ],
  ): Future[Result] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = config.party,
      templateIds = config.templateIds,
      beginOffset = config.beginOffset,
      endOffset = config.endOffset,
    )
    service.getTransactionTrees(request, observer)
    logger.info("Started fetching transaction trees")
    observer.result
  }

  private def getTransactionsRequest(
      ledgerId: String,
      party: String,
      templateIds: Option[List[Identifier]],
      beginOffset: Option[LedgerOffset],
      endOffset: Option[LedgerOffset],
  ): GetTransactionsRequest = {
    val base = GetTransactionsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withBegin(beginOffset.getOrElse(ledgerBeginOffset))
      .withFilter(partyFilter(party, templateIds))

    endOffset match {
      case Some(end) => base.withEnd(end)
      case None => base
    }
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

}
