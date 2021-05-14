// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.Config
import com.daml.ledger.api.benchtool.metrics.{
  Metric,
  MetricalStreamObserver,
  MetricsManager,
  TransactionsMetricalStreamObserver,
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
    val metrics: List[Metric[GetTransactionsResponse]] = List[Metric[GetTransactionsResponse]](
      Metric.TransactionCountMetric[GetTransactionsResponse](
        reportingPeriod.toMillis,
        _.transactions.length,
      ),
      Metric.TransactionSizeMetric[GetTransactionsResponse](
        reportingPeriod.toMillis,
        _.serializedSize,
      ),
      Metric.ConsumptionDelayMetric(_.transactions.collect {
        case t if t.effectiveAt.isDefined => t.getEffectiveAt
      }),
      Metric.ConsumptionSpeedMetric(
        reportingPeriod.toMillis,
        _.transactions.collect {
          case t if t.effectiveAt.isDefined => t.getEffectiveAt
        },
      ),
    )

    val manager1: Future[ActorRef[MetricsManager.Message]] = system.ask(
      SpawnProtocol.Spawn(
        behavior = MetricsManager(
          streamName = config.name,
          metrics = metrics,
          logInterval = reportingPeriod,
        ),
        name = "manager-1",
        props = Props.empty,
        _,
      )
    )
    manager1.flatMap { manager =>
      val observer = new TransactionsMetricalStreamObserver(logger, manager)
      service.getTransactions(request, observer)
      logger.info("Started fetching transactions")
      observer.result
    }
  }

  def transactionTrees(config: Config.StreamConfig): Future[Unit] = {
    val request = getTransactionsRequest(ledgerId, config)
    val metrics: List[Metric[GetTransactionTreesResponse]] =
      List[Metric[GetTransactionTreesResponse]](
        Metric.TransactionCountMetric[GetTransactionTreesResponse](
          reportingPeriod.toMillis,
          _.transactions.length,
        ),
        Metric.TransactionSizeMetric[GetTransactionTreesResponse](
          reportingPeriod.toMillis,
          _.serializedSize,
        ),
        Metric.ConsumptionDelayMetric(_.transactions.collect {
          case t if t.effectiveAt.isDefined => t.getEffectiveAt
        }),
        Metric.ConsumptionSpeedMetric(
          reportingPeriod.toMillis,
          _.transactions.collect {
            case t if t.effectiveAt.isDefined => t.getEffectiveAt
          },
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
