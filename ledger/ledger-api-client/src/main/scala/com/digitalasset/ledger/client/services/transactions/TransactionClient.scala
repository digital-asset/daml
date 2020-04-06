// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.transactions

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionServiceStub
import com.daml.ledger.api.v1.transaction_service._
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

final class TransactionClient(ledgerId: LedgerId, service: TransactionServiceStub)(
    implicit esf: ExecutionSequencerFactory) {

  def getTransactionTrees(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None
  ): Source[TransactionTree, NotUsed] =
    TransactionSource.trees(
      LedgerClient.stub(service, token).getTransactionTrees,
      GetTransactionsRequest(ledgerId.unwrap, Some(start), end, Some(transactionFilter), verbose))

  def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None
  ): Source[Transaction, NotUsed] =
    TransactionSource.flat(
      LedgerClient.stub(service, token).getTransactions,
      GetTransactionsRequest(ledgerId.unwrap, Some(start), end, Some(transactionFilter), verbose))

  def getTransactionById(transactionId: String, parties: Seq[String], token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[GetTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getTransactionById(GetTransactionByIdRequest(ledgerId.unwrap, transactionId, parties))

  def getTransactionByEventId(eventId: String, parties: Seq[String], token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[GetTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getTransactionByEventId(GetTransactionByEventIdRequest(ledgerId.unwrap, eventId, parties))

  def getFlatTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[GetFlatTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getFlatTransactionById(GetTransactionByIdRequest(ledgerId.unwrap, transactionId, parties))

  def getFlatTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None)(
      implicit ec: ExecutionContext): Future[GetFlatTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getFlatTransactionByEventId(
        GetTransactionByEventIdRequest(ledgerId.unwrap, eventId, parties))

  def getLedgerEnd(token: Option[String] = None): Future[GetLedgerEndResponse] =
    LedgerClient.stub(service, token).getLedgerEnd(GetLedgerEndRequest(ledgerId.unwrap))

}
