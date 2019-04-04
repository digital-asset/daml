// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.domain.LedgerOffset
import com.digitalasset.ledger.api.messages.transaction.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesRequest,
  GetTransactionsRequest
}
import com.digitalasset.ledger.api.v1.transaction_service.GetTransactionsResponse
import com.digitalasset.platform.server.api.WithOffset
import com.digitalasset.platform.server.services.transaction.VisibleTransaction

import scala.concurrent.Future

trait TransactionService {

  //TODO: DEL-6426 change the output types to domain layer too
  def getTransactions(req: GetTransactionsRequest): Source[GetTransactionsResponse, NotUsed]

  def getTransactionTrees(
      req: GetTransactionTreesRequest): Source[WithOffset[VisibleTransaction], NotUsed]

  def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute]

  def offsetOrdering: Ordering[LedgerOffset.Absolute]

  def getTransactionById(req: GetTransactionByIdRequest): Future[Option[VisibleTransaction]]

  def getTransactionByEventId(
      req: GetTransactionByEventIdRequest): Future[Option[VisibleTransaction]]
}
