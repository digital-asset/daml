// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.ledger.api.domain.TransactionFilter

trait TransactionsService {
  // FIXME(JM): Cleaner name/types for this
  // FIXME(JM): Fold BlindingInfo into TransactionAccepted, or introduce
  // new type in IndexService?
  def getAcceptedTransactions(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed]

  /*
  def getTransactionById(
      ,
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[TransactionAccepted]]
 */
}
