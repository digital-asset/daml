// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.transactions

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionServiceStub
import com.daml.ledger.api.v1.transaction_service.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.services.transactions.TransactionSource
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

final class TransactionClient(service: TransactionServiceStub)(implicit
    esf: ExecutionSequencerFactory
) {

  def getTransactionTrees(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[TransactionTree, NotUsed] =
    TransactionSource.trees(
      LedgerClient.stub(service, token).getTransactionTrees,
      GetTransactionsRequest(
        begin = Some(start),
        end = end,
        filter = Some(transactionFilter),
        verbose = verbose,
      ),
    )

  def getTransactions(
      start: LedgerOffset,
      end: Option[LedgerOffset],
      transactionFilter: TransactionFilter,
      verbose: Boolean = false,
      token: Option[String] = None,
  ): Source[Transaction, NotUsed] =
    TransactionSource.flat(
      LedgerClient.stub(service, token).getTransactions,
      GetTransactionsRequest(
        begin = Some(start),
        end = end,
        filter = Some(transactionFilter),
        verbose = verbose,
      ),
    )

  def getTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getTransactionById(
        GetTransactionByIdRequest(
          transactionId = transactionId,
          requestingParties = parties,
        )
      )

  def getTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getTransactionByEventId(
        GetTransactionByEventIdRequest(
          eventId = eventId,
          requestingParties = parties,
        )
      )

  def getFlatTransactionById(
      transactionId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetFlatTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getFlatTransactionById(
        GetTransactionByIdRequest(
          transactionId = transactionId,
          requestingParties = parties,
        )
      )

  def getFlatTransactionByEventId(
      eventId: String,
      parties: Seq[String],
      token: Option[String] = None,
  ): Future[GetFlatTransactionResponse] =
    LedgerClient
      .stub(service, token)
      .getFlatTransactionByEventId(
        GetTransactionByEventIdRequest(
          eventId = eventId,
          requestingParties = parties,
        )
      )

  def getLedgerEnd(
      token: Option[String] = None
  ): Future[GetLedgerEndResponse] =
    LedgerClient
      .stub(service, token)
      .getLedgerEnd(GetLedgerEndRequest())

}
