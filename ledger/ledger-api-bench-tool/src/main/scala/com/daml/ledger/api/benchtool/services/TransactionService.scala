// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.Config
import com.daml.ledger.api.benchtool.metrics.{
  MetricalStreamObserver,
  MetricsManager,
  TransactionMetrics,
}
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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

final class TransactionService(
    channel: Channel,
    implicit val system: ActorSystem[SpawnProtocol.Command], //TODO: abstract over this
    ledgerId: String,
    reportingPeriod: FiniteDuration,
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: TransactionServiceGrpc.TransactionServiceStub =
    TransactionServiceGrpc.stub(channel)

  // TODO: move this to the abstraction ^^^
  implicit val timeout: Timeout = Timeout(3.seconds)
  import akka.actor.typed.scaladsl.AskPattern._

  def transactions(config: Config.StreamConfig): Future[Unit] = {
    val request = getTransactionsRequest(ledgerId, config)

    // TODO: move this to an abstraction
    val manager: Future[ActorRef[MetricsManager.Message[GetTransactionsResponse]]] = system.ask(
      SpawnProtocol.Spawn(
        behavior = MetricsManager(
          streamName = config.name,
          metrics = TransactionMetrics.transactionMetrics(reportingPeriod),
          logInterval = reportingPeriod,
        ),
        name = s"${config.name}-manager",
        props = Props.empty,
        _,
      )
    )
    manager.flatMap { manager =>
      val observer =
        new MetricalStreamObserver[GetTransactionsResponse](config.name, logger, manager)
      service.getTransactions(request, observer)
      logger.info("Started fetching transactions")
      observer.result
    }
  }

  def transactionTrees(config: Config.StreamConfig): Future[Unit] = {
    val request = getTransactionsRequest(ledgerId, config)

    // TODO: move this to an abstraction
    val manager: Future[ActorRef[MetricsManager.Message[GetTransactionTreesResponse]]] = system.ask(
      SpawnProtocol.Spawn(
        behavior = MetricsManager(
          streamName = config.name,
          metrics = TransactionMetrics.transactionTreesMetrics(reportingPeriod),
          logInterval = reportingPeriod,
        ),
        name = s"${config.name}-manager",
        props = Props.empty,
        _,
      )
    )
    manager.flatMap { manager =>
      val observer =
        new MetricalStreamObserver[GetTransactionTreesResponse](config.name, logger, manager)
      service.getTransactionTrees(request, observer)
      logger.info("Started fetching transaction trees")
      observer.result
    }
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

  private def partyFilter(party: String, templateIds: List[Identifier]): TransactionFilter = {
    val templatesFilter = Filters.defaultInstance
      .withInclusive(InclusiveFilters.defaultInstance.addAllTemplateIds(templateIds))
    TransactionFilter()
      .withFiltersByParty(Map(party -> templatesFilter))
  }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

  private def ledgerEndOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)

}
