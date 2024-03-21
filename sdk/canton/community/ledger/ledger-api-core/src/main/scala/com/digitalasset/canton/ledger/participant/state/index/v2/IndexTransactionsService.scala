// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.{LedgerOffset, TransactionFilter, TransactionId}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

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
      multiDomainEnabled: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdatesResponse, NotUsed]

  def transactionTrees(
      begin: LedgerOffset,
      endAt: Option[LedgerOffset],
      filter: TransactionFilter,
      verbose: Boolean,
      multiDomainEnabled: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdateTreesResponse, NotUsed]

  def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]]

  def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]]

  def latestPrunedOffsets()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(LedgerOffset.Absolute, LedgerOffset.Absolute)]
}
