// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.messages.transaction.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesRequest,
  GetTransactionsRequest,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetLatestPrunedOffsetsResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.logging.LoggingContext

import scala.concurrent.Future

trait TransactionService {

  def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute]

  def getTransactions(req: GetTransactionsRequest)(implicit
      loggingContext: LoggingContext
  ): Source[GetTransactionsResponse, NotUsed]

  def getTransactionTrees(
      req: GetTransactionTreesRequest
  )(implicit
      loggingContext: LoggingContext
  ): Source[GetTransactionTreesResponse, NotUsed]

  def getTransactionById(req: GetTransactionByIdRequest)(implicit
      loggingContext: LoggingContext
  ): Future[GetTransactionResponse]

  def getTransactionByEventId(req: GetTransactionByEventIdRequest)(implicit
      loggingContext: LoggingContext
  ): Future[GetTransactionResponse]

  def getFlatTransactionById(req: GetTransactionByIdRequest)(implicit
      loggingContext: LoggingContext
  ): Future[GetFlatTransactionResponse]

  def getFlatTransactionByEventId(
      req: GetTransactionByEventIdRequest
  )(implicit loggingContext: LoggingContext): Future[GetFlatTransactionResponse]

  def getLatestPrunedOffsets: Future[GetLatestPrunedOffsetsResponse]
}
