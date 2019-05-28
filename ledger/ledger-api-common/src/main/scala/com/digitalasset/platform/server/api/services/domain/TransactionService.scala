// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.domain.{LedgerOffset, Transaction, TransactionTree}
import com.digitalasset.ledger.api.messages.transaction.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesRequest,
  GetTransactionsRequest
}

import scala.concurrent.Future

trait TransactionService {

  def getTransactions(req: GetTransactionsRequest): Source[Transaction, NotUsed]

  def getTransactionTrees(req: GetTransactionTreesRequest): Source[TransactionTree, NotUsed]

  def getLedgerEnd(ledgerId: String): Future[LedgerOffset.Absolute]

  def offsetOrdering: Ordering[LedgerOffset.Absolute]

  def getTransactionById(req: GetTransactionByIdRequest): Future[TransactionTree]

  def getTransactionByEventId(req: GetTransactionByEventIdRequest): Future[TransactionTree]

  def getFlatTransactionById(req: GetTransactionByIdRequest): Future[Transaction]

  def getFlatTransactionByEventId(req: GetTransactionByEventIdRequest): Future[Transaction]
}
