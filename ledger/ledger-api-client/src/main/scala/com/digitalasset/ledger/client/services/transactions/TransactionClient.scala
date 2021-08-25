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
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.Future

sealed class TransactionClientWithoutLedgerId(service: TransactionServiceStub)(implicit
    esf: ExecutionSequencerFactory
) {

  def getTransactionTrees(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Source[TransactionTree, NotUsed] =
    TransactionSource.trees(
      LedgerClient.stub(service, token).getTransactionTrees,
      GetTransactionsRequest(
        ledgerIdToUse.unwrap,
        Some(start),
        end,
        Some(transactionFilter),
        verbose,
      ),
    )

  def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Source[Transaction, NotUsed] =
    TransactionSource.flat(
      LedgerClient.stub(service, token).getTransactions,
      GetTransactionsRequest(
        ledgerIdToUse.unwrap,
        Some(start),
        end,
        Some(transactionFilter),
        verbose,
      ),
    )

  def getTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getTransactionById(
        GetTransactionByIdRequest(
          ledgerIdToUse.unwrap,
          transactionId,
          parties,
        )
      )

  def getTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getTransactionByEventId(
        GetTransactionByEventIdRequest(
          ledgerIdToUse.unwrap,
          eventId,
          parties,
        )
      )

  def getFlatTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetFlatTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getFlatTransactionById(
        GetTransactionByIdRequest(
          ledgerIdToUse.unwrap,
          transactionId,
          parties,
        )
      )

  def getFlatTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetFlatTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getFlatTransactionByEventId(
        GetTransactionByEventIdRequest(
          ledgerIdToUse.unwrap,
          eventId,
          parties,
        )
      )

  def getLedgerEnd(
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetLedgerEndResponse] =
    LedgerClient
      .stub(service, token)
      .getLedgerEnd(GetLedgerEndRequest(ledgerIdToUse.unwrap))

}

final class TransactionClient(val ledgerId: LedgerId, service: TransactionServiceStub)(implicit
    esf: ExecutionSequencerFactory
) extends TransactionClientWithoutLedgerId(service) {

  override def getTransactionTrees(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Source[TransactionTree, NotUsed] =
    super.getTransactionTrees(start, end, transactionFilter, verbose, token, ledgerId)

  override def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Source[Transaction, NotUsed] =
    super.getTransactions(start, end, transactionFilter, verbose, token, ledgerId)

  override def getTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetTransactionResponse] =
    super.getTransactionById(transactionId, parties, token, ledgerId)

  override def getTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetTransactionResponse] =
    super.getTransactionByEventId(eventId, parties, token, ledgerId)

  override def getFlatTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetFlatTransactionResponse] =
    super.getFlatTransactionById(transactionId, parties, token, ledgerId)

  override def getFlatTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetFlatTransactionResponse] =
    super.getFlatTransactionByEventId(eventId, parties, token, ledgerId)

  override def getLedgerEnd(
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetLedgerEndResponse] =
    super.getLedgerEnd(token, ledgerId)

}
