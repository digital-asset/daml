// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}

import scala.concurrent.Future

/**
  * Sends commits to [[cheapTransactionsLedgerWriter]] in case estimated interpretation cost is below
  * [[minimumEstimatedInterpretationCost]] otherwise to [[expensiveTransactionsLedgerWriter]].
  * Submissions that don't have an estimated interpretation cost will be forwarded to
  * [[expensiveTransactionsLedgerWriter]].
  *
  * @param minimumEstimatedInterpretationCost all transactions that have a greater than equal estimated interpretation
  *                                           cost will be forwarded to [[expensiveTransactionsLedgerWriter]]
  */
class InterpretationCostBasedLedgerWriterChooser(
    minimumEstimatedInterpretationCost: Long,
    cheapTransactionsLedgerWriter: LedgerWriter,
    expensiveTransactionsLedgerWriter: LedgerWriter)
    extends LedgerWriter {
  assert(
    cheapTransactionsLedgerWriter.participantId == expensiveTransactionsLedgerWriter.participantId)

  override def participantId: ParticipantId = cheapTransactionsLedgerWriter.participantId

  override def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    if (metadata.estimatedInterpretationCost.exists(_ >= minimumEstimatedInterpretationCost)) {
      expensiveTransactionsLedgerWriter.commit(correlationId, envelope, metadata)
    } else {
      cheapTransactionsLedgerWriter.commit(correlationId, envelope, metadata)
    }

  override def currentHealth(): HealthStatus =
    cheapTransactionsLedgerWriter.currentHealth() and expensiveTransactionsLedgerWriter
      .currentHealth()
}
