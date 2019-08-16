// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService]]
  **/
trait TransactionsService {
  // FIXME(JM): Cleaner name/types for this
  // FIXME(JM): Fold BlindingInfo into TransactionAccepted, or introduce
  // new type in IndexService?
  def getAcceptedTransactions(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed]

  def getLedgerBeginning(): Future[Offset]

  def getLedgerEnd(): Future[Offset]

  /*
  def transactionTreeById(
      ,
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[TransactionAccepted]]
 */
}
