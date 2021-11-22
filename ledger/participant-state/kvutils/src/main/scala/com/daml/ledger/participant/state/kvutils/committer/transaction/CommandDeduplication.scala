// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Instant

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildDuration,
  commandDedupKey,
  parseDuration,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.getCurrentConfiguration
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.{DamlCommandDedupValue, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlTransactionRejectionEntry,
  Duplicate,
}
import com.daml.logging.LoggingContext

private[transaction] object CommandDeduplication {

  def overwriteDeduplicationPeriodWithMaxDurationStep(
      defaultConfig: Configuration
  ): Step = new Step {
    override def apply(context: CommitContext, input: DamlTransactionEntrySummary)(implicit
        loggingContext: LoggingContext
    ): StepResult[DamlTransactionEntrySummary] = {
      val (_, currentConfig) = getCurrentConfiguration(defaultConfig, context)
      val submission = input.submission.toBuilder
      submission.getSubmitterInfoBuilder.setDeduplicationDuration(
        buildDuration(currentConfig.maxDeduplicationTime)
      )
      StepContinue(input.copyPreservingDecodedTransaction(submission.build()))
    }
  }

  /** Reject duplicate commands
    */
  def deduplicateCommandStep(rejections: Rejections): Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      commitContext.recordTime
        .map { recordTime =>
          val dedupKey = commandDedupKey(transactionEntry.submitterInfo)
          val dedupEntry = commitContext.get(dedupKey)
          if (dedupEntry.forall(isAfterDeduplicationTime(recordTime.toInstant, _))) {
            StepContinue(transactionEntry)
          } else {
            rejections.reject(
              DamlTransactionRejectionEntry.newBuilder
                .setSubmitterInfo(transactionEntry.submitterInfo)
                // No duplicate rejection is a definite answer as the deduplication entry will eventually expire.
                .setDefiniteAnswer(false)
                .setDuplicateCommand(Duplicate.newBuilder.setDetails("")),
              "the command is a duplicate",
              commitContext.recordTime,
            )
          }
        }
        .getOrElse(StepContinue(transactionEntry))
    }
  }

  def setDeduplicationEntryStep(defaultConfig: Configuration): Step =
    new Step {
      def apply(commitContext: CommitContext, transactionEntry: DamlTransactionEntrySummary)(
          implicit loggingContext: LoggingContext
      ): StepResult[DamlTransactionEntrySummary] = {
        val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
        // Deduplication duration must be explicitly overwritten in a previous step
        //  (see [[TransactionCommitter.overwriteDeduplicationPeriodWithMaxDuration]]) and set to ``config.maxDeduplicationTime``.
        if (!transactionEntry.submitterInfo.hasDeduplicationDuration) {
          throw Err.InvalidSubmission("Deduplication duration is not set.")
        }
        val deduplicateUntil = Conversions.buildTimestamp(
          transactionEntry.submissionTime
            .add(parseDuration(transactionEntry.submitterInfo.getDeduplicationDuration))
            .add(config.timeModel.minSkew)
        )
        val commandDedupBuilder = DamlCommandDedupValue.newBuilder
          .setDeduplicatedUntil(deduplicateUntil)
        // Set a deduplication entry.
        commitContext.set(
          commandDedupKey(transactionEntry.submitterInfo),
          DamlStateValue.newBuilder
            .setCommandDedup(
              commandDedupBuilder.build
            )
            .build,
        )
        StepContinue(transactionEntry)
      }
    }

  // Checks that the submission time of the command is after the
  // deduplicationTime represented by stateValue
  private def isAfterDeduplicationTime(
      submissionTime: Instant,
      stateValue: DamlStateValue,
  ): Boolean = {
    val cmdDedup = stateValue.getCommandDedup
    if (stateValue.hasCommandDedup && cmdDedup.hasDeduplicatedUntil) {
      val dedupTime = parseTimestamp(cmdDedup.getDeduplicatedUntil).toInstant
      dedupTime.isBefore(submissionTime)
    } else {
      false
    }
  }
}
