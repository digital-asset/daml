// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}

import scala.concurrent.Future

/**
  * Sends commits to [[cheapTransactionsDelegate]] in case estimated interpretation cost is below
  * [[minimumEstimatedInterpretationCost]] otherwise to [[expensiveTransactionsDelegate]].
  * Submissions that don't have an estimated interpretation cost will be forwarded to
  * [[expensiveTransactionsDelegate]].
  *
  * @param minimumEstimatedInterpretationCost all transactions that have a greater than equal estimated interpretation
  *                                           cost will be forwarded to [[expensiveTransactionsDelegate]]
  */
class InterpretationCostBasedLedgerWriterChooser(
    minimumEstimatedInterpretationCost: Long,
    cheapTransactionsDelegate: LedgerWriter,
    expensiveTransactionsDelegate: LedgerWriter)
    extends LedgerWriter {
  assert(cheapTransactionsDelegate.participantId == expensiveTransactionsDelegate.participantId)

  override def participantId: ParticipantId = cheapTransactionsDelegate.participantId

  override def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    if (metadata.estimatedInterpretationCost.exists(_ >= minimumEstimatedInterpretationCost)) {
      expensiveTransactionsDelegate.commit(correlationId, envelope, metadata)
    } else {
      cheapTransactionsDelegate.commit(correlationId, envelope, metadata)
    }

  override def currentHealth(): HealthStatus =
    cheapTransactionsDelegate.currentHealth() and expensiveTransactionsDelegate
      .currentHealth()
}
