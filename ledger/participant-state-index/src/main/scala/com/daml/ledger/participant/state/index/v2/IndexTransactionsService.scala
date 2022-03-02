// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.{LedgerOffset, TransactionFilter, TransactionId}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService]]
  */
trait IndexTransactionsService extends LedgerEndService {
  def transactions(
      begin: LedgerOffset,
      endAt: Option[LedgerOffset],
      filter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed]

  def transactionTrees(
      begin: LedgerOffset,
      endAt: Option[LedgerOffset],
      filter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed]

  def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]]

  def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]]
}
