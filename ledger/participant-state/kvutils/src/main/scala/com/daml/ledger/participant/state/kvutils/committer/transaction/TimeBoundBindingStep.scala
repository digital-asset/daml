// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.committer.Committer.getCurrentConfiguration
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext

object TimeBoundBindingStep {

  def setTimeBoundsInContextStep(defaultConfig: Configuration): Step = new Step {
    override def apply(commitContext: CommitContext, transactionEntry: DamlTransactionEntrySummary)(
        implicit loggingContext: LoggingContext
    ): StepResult[DamlTransactionEntrySummary] = {
      val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
      val timeModel = config.timeModel

      if (commitContext.preExecute) {
        val minimumRecordTime = transactionMinRecordTime(
          transactionEntry.submissionTime,
          transactionEntry.ledgerEffectiveTime,
          timeModel,
        )
        val maximumRecordTime = transactionMaxRecordTime(
          transactionEntry.submissionTime,
          transactionEntry.ledgerEffectiveTime,
          timeModel,
        )
        commitContext.minimumRecordTime = Some(minimumRecordTime)
        commitContext.maximumRecordTime = Some(maximumRecordTime)
      }
      StepContinue(transactionEntry)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  private def transactionMinRecordTime(
      submissionTime: Timestamp,
      ledgerTime: Timestamp,
      timeModel: LedgerTimeModel,
  ): Timestamp =
    List(
      Some(timeModel.minRecordTime(ledgerTime)),
      Some(timeModel.minRecordTime(submissionTime)),
    ).flatten.max

  private def transactionMaxRecordTime(
      submissionTime: Timestamp,
      ledgerTime: Timestamp,
      timeModel: LedgerTimeModel,
  ): Timestamp =
    List(timeModel.maxRecordTime(ledgerTime), timeModel.maxRecordTime(submissionTime)).min

}
