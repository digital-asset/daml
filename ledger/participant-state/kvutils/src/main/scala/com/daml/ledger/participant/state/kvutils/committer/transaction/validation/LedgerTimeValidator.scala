// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Err
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejections,
  Step,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntry
import com.daml.logging.LoggingContext

private[transaction] class LedgerTimeValidator extends TransactionValidator {

  /** Creates a committer step that validates ledger effective time and the command's time-to-live. */
  override def createValidationStep(rejections: Rejections): Step =
    new Step {
      def apply(
          commitContext: CommitContext,
          transactionEntry: DamlTransactionEntrySummary,
      )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {

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
