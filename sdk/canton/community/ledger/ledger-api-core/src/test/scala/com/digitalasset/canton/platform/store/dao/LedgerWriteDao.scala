// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.CommittedTransaction

import scala.concurrent.Future

private[platform] trait LedgerWriteDao extends ReportsHealth {

  /** Initializes the database with the given ledger identity. If the database was already
    * intialized, instead compares the given identity parameters to the existing ones, and returns a
    * Future failed with [[MismatchException]] if they don't match.
    *
    * This method is idempotent. This method is NOT safe to call concurrently.
    *
    * This method must succeed at least once before other LedgerWriteDao methods may be used.
    *
    * @param participantId
    *   the participant id to be stored
    */
  def initialize(
      participantId: ParticipantId
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit]

  def storeRejection(
      completionInfo: Option[state.CompletionInfo],
      recordTime: Timestamp,
      offset: Offset,
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** Stores a party allocation or rejection thereof. */
  def storePartyAdded(
      offset: Offset,
      submissionIdOpt: Option[SubmissionId],
      recordTime: Timestamp,
      partyDetails: IndexerPartyDetails,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

  /** This is a combined store transaction method to support only tests !!! Usage of this is
    * discouraged.
    */
  def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      updateId: UpdateId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      recordTime: Timestamp,
      contractActivenessChanged: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse]

}
