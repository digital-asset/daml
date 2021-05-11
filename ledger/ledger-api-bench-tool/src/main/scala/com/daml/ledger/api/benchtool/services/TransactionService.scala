// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.Config
import com.daml.ledger.api.benchtool.metrics.{Metric, MetricalStreamObserver}
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
import scala.concurrent.duration.Duration

final class TransactionService(channel: Channel, ledgerId: String, reportingPeriod: Duration) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: TransactionServiceGrpc.TransactionServiceStub =
    TransactionServiceGrpc.stub(channel)

  // TODO: add filters
  def transactions(config: Config.StreamConfig): Future[Unit] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = config.party,
      templateIds = config.templateIds,
      beginOffset = ledgerBeginOffset,
      endOffset = dummyEndOffset,
    )
    val metrics: List[Metric[GetTransactionsResponse]] = List[Metric[GetTransactionsResponse]](
      Metric.TransactionCountingMetric[GetTransactionsResponse](
        reportingPeriod.toMillis,
        _.transactions.length,
      ),
      Metric.TransactionSizingMetric[GetTransactionsResponse](
        reportingPeriod.toMillis,
        _.serializedSize,
      ),
    )
    val observer =
      new MetricalStreamObserver[GetTransactionsResponse](
        config.name,
        reportingPeriod,
        metrics,
        logger,
      )
    service.getTransactions(request, observer)
    logger.info("Started fetching transactions")
    observer.result
  }

  def transactionTrees(config: Config.StreamConfig): Future[Unit] = {
    val request = getTransactionsRequest(
      ledgerId = ledgerId,
      party = config.party,
      templateIds = config.templateIds,
      beginOffset = ledgerBeginOffset,
      endOffset = ledgerEndOffset,
    )
    val metrics: List[Metric[GetTransactionTreesResponse]] =
      List[Metric[GetTransactionTreesResponse]](
        Metric.TransactionCountingMetric[GetTransactionTreesResponse](
          reportingPeriod.toMillis,
          _.transactions.length,
        ),
        Metric.TransactionSizingMetric[GetTransactionTreesResponse](
          reportingPeriod.toMillis,
          _.serializedSize,
        ),
      )
    val observer =
      new MetricalStreamObserver[GetTransactionTreesResponse](
        config.name,
        reportingPeriod,
        metrics,
        logger,
      )
    service.getTransactionTrees(request, observer)
    logger.info("Started fetching transaction trees")
    observer.result
  }

  private def getTransactionsRequest(
      ledgerId: String,
      party: String,
      templateIds: List[Identifier],
      beginOffset: LedgerOffset,
      endOffset: LedgerOffset,
  ): GetTransactionsRequest = {
    GetTransactionsRequest.defaultInstance
      .withLedgerId(ledgerId)
      .withBegin(beginOffset)
      .withEnd(endOffset)
      .withFilter(partyFilter(party, templateIds))
  }

  private def partyFilter(party: String, templateIds: List[Identifier]): TransactionFilter = {
    val templatesFilter = Filters.defaultInstance
      .withInclusive(InclusiveFilters.defaultInstance.addAllTemplateIds(templateIds))
    TransactionFilter()
      .withFiltersByParty(Map(party -> templatesFilter))
  }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

  private def dummyEndOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)
//  LedgerOffset().withAbsolute("0000000000000038")

  private def ledgerEndOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)

}
