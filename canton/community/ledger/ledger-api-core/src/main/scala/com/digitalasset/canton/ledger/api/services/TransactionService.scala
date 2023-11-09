// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.transaction_service.*
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.ledger.api.messages.transaction.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesRequest,
  GetTransactionsRequest,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait TransactionService {

  def getLedgerEnd(ledgerId: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[LedgerOffset.Absolute]

  def getTransactions(req: GetTransactionsRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetTransactionsResponse, NotUsed]

  def getTransactionTrees(
      req: GetTransactionTreesRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetTransactionTreesResponse, NotUsed]

  def getTransactionById(req: GetTransactionByIdRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetTransactionResponse]

  def getTransactionByEventId(req: GetTransactionByEventIdRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetTransactionResponse]

  def getFlatTransactionById(req: GetTransactionByIdRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetFlatTransactionResponse]

  def getFlatTransactionByEventId(
      req: GetTransactionByEventIdRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetFlatTransactionResponse]

  def getLatestPrunedOffsets()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetLatestPrunedOffsetsResponse]
}
