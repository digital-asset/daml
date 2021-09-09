// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.telemetry.TelemetryContext

import scala.concurrent.Future

/** Sends commits to [[cheapTransactionsDelegate]] in case estimated interpretation cost is below
  * [[estimatedInterpretationCostThreshold]] otherwise to [[expensiveTransactionsDelegate]].
  * Submissions that don't have an estimated interpretation cost will be forwarded to
  * [[cheapTransactionsDelegate]].
  *
  * @param estimatedInterpretationCostThreshold all transactions that have a greater or equal estimated interpretation
  *                                             cost will be forwarded to [[expensiveTransactionsDelegate]]
  */
final class InterpretationCostBasedLedgerWriterChooser(
    estimatedInterpretationCostThreshold: Long,
    cheapTransactionsDelegate: LedgerWriter,
    expensiveTransactionsDelegate: LedgerWriter,
    incrementCheapCounter: () => Unit = () => (),
    incrementExpensiveCounter: () => Unit = () => (),
    addInterpretationCostBelowThreshold: Long => Unit = _ => (),
    addInterpretationCostAboveThreshold: Long => Unit = _ => (),
) extends LedgerWriter {
  assert(cheapTransactionsDelegate.participantId == expensiveTransactionsDelegate.participantId)

  override def participantId: Ref.ParticipantId = cheapTransactionsDelegate.participantId

  override def commit(
      correlationId: String,
      envelope: Raw.Envelope,
      metadata: CommitMetadata,
  )(implicit telemetryContext: TelemetryContext): Future[SubmissionResult] = {
    val estimatedInterpretationCost = metadata.estimatedInterpretationCost.getOrElse(0L)
    if (estimatedInterpretationCost >= estimatedInterpretationCostThreshold) {
      incrementExpensiveCounter()
      addInterpretationCostAboveThreshold(estimatedInterpretationCost)
      expensiveTransactionsDelegate.commit(correlationId, envelope, metadata)
    } else {
      incrementCheapCounter()
      addInterpretationCostBelowThreshold(estimatedInterpretationCost)
      cheapTransactionsDelegate.commit(correlationId, envelope, metadata)
    }
  }

  override def currentHealth(): HealthStatus =
    cheapTransactionsDelegate.currentHealth() and expensiveTransactionsDelegate
      .currentHealth()
}

object InterpretationCostBasedLedgerWriterChooser {
  def apply(
      estimatedInterpretationCostThreshold: Long,
      cheapTransactionsDelegate: LedgerWriter,
      expensiveTransactionsDelegate: LedgerWriter,
      damlMetrics: Metrics,
  ): InterpretationCostBasedLedgerWriterChooser = {
    val metrics = damlMetrics.daml.kvutils.writer
    new InterpretationCostBasedLedgerWriterChooser(
      estimatedInterpretationCostThreshold,
      cheapTransactionsDelegate,
      expensiveTransactionsDelegate,
      incrementCheapCounter = metrics.committedCount.inc,
      incrementExpensiveCounter = metrics.preExecutedCount.inc,
      addInterpretationCostBelowThreshold = metrics.committedInterpretationCosts.update,
      addInterpretationCostAboveThreshold = metrics.preExecutedInterpretationCosts.update,
    )
  }
}
