// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.transactions

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.api.v1.transaction_service._

import scala.concurrent.{ExecutionContext, Future}

import scalaz.syntax.tag._

final class TransactionClient(ledgerId: LedgerId, transactionService: TransactionService)(
    implicit esf: ExecutionSequencerFactory) {

  def getTransactionTrees(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false
  ): Source[TransactionTree, NotUsed] =
    TransactionSource.trees(
      transactionService.getTransactionTrees,
      GetTransactionsRequest(ledgerId.unwrap, Some(start), end, Some(transactionFilter), verbose))

  def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false
  ): Source[Transaction, NotUsed] = {

    TransactionSource.flat(
      transactionService.getTransactions,
      GetTransactionsRequest(ledgerId.unwrap, Some(start), end, Some(transactionFilter), verbose))
  }

  def getTransactionById(transactionId: String, parties: Seq[String])(
      implicit ec: ExecutionContext): Future[GetTransactionResponse] = {
    transactionService
      .getTransactionById(GetTransactionByIdRequest(ledgerId.unwrap, transactionId, parties))
  }

  def getTransactionByEventId(eventId: String, parties: Seq[String])(
      implicit ec: ExecutionContext): Future[GetTransactionResponse] =
    transactionService
      .getTransactionByEventId(GetTransactionByEventIdRequest(ledgerId.unwrap, eventId, parties))

  def getFlatTransactionById(transactionId: String, parties: Seq[String])(
      implicit ec: ExecutionContext): Future[GetFlatTransactionResponse] = {
    transactionService
      .getFlatTransactionById(GetTransactionByIdRequest(ledgerId.unwrap, transactionId, parties))
  }

  def getFlatTransactionByEventId(eventId: String, parties: Seq[String])(
      implicit ec: ExecutionContext): Future[GetFlatTransactionResponse] =
    transactionService
      .getFlatTransactionByEventId(
        GetTransactionByEventIdRequest(ledgerId.unwrap, eventId, parties))

  def getLedgerEnd: Future[GetLedgerEndResponse] =
    transactionService.getLedgerEnd(GetLedgerEndRequest(ledgerId.unwrap))
}
