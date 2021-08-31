// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import scala.concurrent.Future

final class TransactionClient(val ledgerId: LedgerId, service: TransactionServiceStub)(implicit
    esf: ExecutionSequencerFactory
) {
  private val it = new withoutledgerid.TransactionClient(service)

  def getTransactionTrees(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[TransactionTree, NotUsed] =
    it.getTransactionTrees(start, end, transactionFilter, verbose, token, ledgerId)

  def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[Transaction, NotUsed] =
    it.getTransactions(start, end, transactionFilter, verbose, token, ledgerId)

  def getTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetTransactionResponse] =
    it.getTransactionById(transactionId, parties, token, ledgerId)

  def getTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetTransactionResponse] =
    it.getTransactionByEventId(eventId, parties, token, ledgerId)

  def getFlatTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetFlatTransactionResponse] =
    it.getFlatTransactionById(transactionId, parties, token, ledgerId)

  def getFlatTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetFlatTransactionResponse] =
    it.getFlatTransactionByEventId(eventId, parties, token, ledgerId)

  def getLedgerEnd(
      token: Option[String] = None
  ): Future[GetLedgerEndResponse] =
    it.getLedgerEnd(token, ledgerId)

}
