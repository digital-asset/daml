// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Err
import com.daml.ledger.participant.state.kvutils.committer.Committer.getCurrentConfiguration
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejection,
  Rejections,
  Step,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntry
import com.daml.logging.LoggingContext

private[transaction] class LedgerTimeValidator(defaultConfig: Configuration)
    extends TransactionValidator {

  /** Creates a committer step that validates ledger effective time and the command's time-to-live. */
  override def createValidationStep(rejections: Rejections): Step =
    new Step {
      def apply(
          commitContext: CommitContext,
          transactionEntry: DamlTransactionEntrySummary,
      )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {

        commitContext.recordTime match {
          case Some(recordTime) =>
            val givenLedgerTime = transactionEntry.ledgerEffectiveTime
            val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
            val timeModel = config.timeModel

            timeModel
              .checkTime(ledgerTime = givenLedgerTime, recordTime = recordTime)
              .fold(
                outOfRange =>
                  rejections.reject(
                    transactionEntry,
                    Rejection.LedgerTimeOutOfRange(outOfRange),
                    commitContext.recordTime,
                  ),
                _ => StepContinue(transactionEntry),
              )
          case None => // Pre-execution: propagate the time bounds and defer the checks to post-execution.
            (commitContext.minimumRecordTime, commitContext.maximumRecordTime) match {
              case (Some(minimumRecordTime), Some(maximumRecordTime)) =>
                val outOfTimeBoundsLogEntry = DamlLogEntry.newBuilder
                  .setTransactionRejectionEntry(
                    rejections.preExecutionOutOfTimeBoundsRejectionEntry(
                      transactionEntry.submitterInfo,
                      minimumRecordTime,
                      maximumRecordTime,
                    )
                  )
                  .build
                commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
                StepContinue(transactionEntry)
              case _ =>
                throw Err.InternalError(
                  "Minimum and maximum record times were not set in the committer context"
                )
            }
        }
      }
    }
}
