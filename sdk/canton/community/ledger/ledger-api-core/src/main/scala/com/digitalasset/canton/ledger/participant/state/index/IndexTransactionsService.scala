// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.ledger.api.domain.types.ParticipantOffset
import com.digitalasset.canton.ledger.api.domain.{TransactionFilter, UpdateId}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService]]
  */
trait IndexTransactionsService extends LedgerEndService {
  def transactions(
      begin: ParticipantOffset,
      endAt: Option[ParticipantOffset],
      filter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdatesResponse, NotUsed]

  def transactionTrees(
      begin: ParticipantOffset,
      endAt: Option[ParticipantOffset],
      filter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdateTreesResponse, NotUsed]

  def getTransactionById(
      updateId: UpdateId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]]

  def getTransactionTreeById(
      updateId: UpdateId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]]

  def latestPrunedOffsets()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Option[Long], Option[Long])] // TODO(#18685) replace Long with data.Offset
}
