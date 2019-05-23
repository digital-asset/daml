// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.{LedgerOffset, TransactionFilter, TransactionId}

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService]]
  **/
trait TransactionsService {
  def transactions(
      begin: LedgerOffset,
      endAt: Option[LedgerOffset],
      filter: TransactionFilter
  ): Source[Transaction, NotUsed]

  def currentLedgerEnd(): Future[LedgerOffset.Absolute]

  def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party]
  ): Future[Option[Transaction]]
}
