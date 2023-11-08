// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.transactions

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionServiceStub
import com.daml.ledger.api.v1.transaction_service._

import scala.concurrent.Future

final class TransactionClient(ledgerId: LedgerId, service: TransactionServiceStub)(implicit
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
    it.getTransactionTrees(start, end, transactionFilter, ledgerId, verbose, token)

  def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[Transaction, NotUsed] =
    it.getTransactions(start, end, transactionFilter, ledgerId, verbose, token)

  def getTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetTransactionResponse] =
    it.getTransactionById(transactionId, parties, ledgerId, token)

  def getTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetTransactionResponse] =
    it.getTransactionByEventId(eventId, parties, ledgerId, token)

  def getFlatTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetFlatTransactionResponse] =
    it.getFlatTransactionById(transactionId, parties, ledgerId, token)

  def getFlatTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetFlatTransactionResponse] =
    it.getFlatTransactionByEventId(eventId, parties, ledgerId, token)

  def getLedgerEnd(token: Option[String] = None): Future[GetLedgerEndResponse] =
    it.getLedgerEnd(ledgerId, token)

}
