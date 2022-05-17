// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc,
}
import io.grpc.Channel
import org.slf4j.LoggerFactory

import scala.concurrent.Future

final class TransactionService(
    channel: Channel,
    ledgerId: String,
    authorizationToken: Option[String],
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: TransactionServiceGrpc.TransactionServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(TransactionServiceGrpc.stub(channel))

  def transactions[Result](
      config: WorkflowConfig.StreamConfig.TransactionsStreamConfig,
      observer: ObserverWithResult[GetTransactionsResponse, Result],
  ): Future[Result] =
    getTransactionsRequest(
      ledgerId = ledgerId,
      filters = config.filters,
      beginOffset = config.beginOffset,
      endOffset = config.endOffset,
    ) match {
      case Right(request) =>
        service.getTransactions(request, observer)
        logger.info("Started fetching transactions")
        observer.result
      case Left(error) =>
        Future.failed(new RuntimeException(error))
    }

  def transactionTrees[Result](
      config: WorkflowConfig.StreamConfig.TransactionTreesStreamConfig,
      observer: ObserverWithResult[
        GetTransactionTreesResponse,
        Result,
      ],
  ): Future[Result] =
    getTransactionsRequest(
      ledgerId = ledgerId,
      filters = config.filters,
      beginOffset = config.beginOffset,
      endOffset = config.endOffset,
    ) match {
      case Right(request) =>
        service.getTransactionTrees(request, observer)
        logger.info("Started fetching transaction trees")
        observer.result
      case Left(error) =>
        Future.failed(new RuntimeException(error))
    }

  private def getTransactionsRequest(
      ledgerId: String,
      filters: List[WorkflowConfig.StreamConfig.PartyFilter],
      beginOffset: Option[LedgerOffset],
      endOffset: Option[LedgerOffset],
  ): Either[String, GetTransactionsRequest] =
    StreamFilters
      .transactionFilters(filters)
      .map { filters =>
        val getTransactionsRequest = GetTransactionsRequest.defaultInstance
          .withLedgerId(ledgerId)
          .withBegin(beginOffset.getOrElse(ledgerBeginOffset))
          .withFilter(filters)

        endOffset match {
          case Some(end) => getTransactionsRequest.withEnd(end)
          case None => getTransactionsRequest
        }
      }

  private def ledgerBeginOffset: LedgerOffset =
    LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

}
