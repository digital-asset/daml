// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.participant.state.kvutils.Conversions.{
  commandDedupKey,
  parseDuration,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.DamlCommandDedupValue.TimeCase
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlTransactionRejectionEntry,
  Duplicate,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlCommandDedupValue,
  DamlLogEntry,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.logging.LoggingContext

private[transaction] object CommandDeduplication {

  /** Reject duplicate commands
    */
  def deduplicateCommandStep(rejections: Rejections): Step =
    new Step {
      def apply(
          commitContext: CommitContext,
          transactionEntry: DamlTransactionEntrySummary,
      )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
        val commandDeduplicationDuration =
          if (transactionEntry.submitterInfo.hasDeduplicationDuration)
            parseDuration(transactionEntry.submitterInfo.getDeduplicationDuration)
          else
            throw Err.InternalError(
              "Deduplication period not supported, only durations are supported"
            )
        val dedupKey = commandDedupKey(transactionEntry.submitterInfo)
        val dedupEntry = commitContext.get(dedupKey)
        val maybeDedupValue = dedupEntry
          .filter(_.hasCommandDedup)
          .map(_.getCommandDedup)
        val recordTimeOrMinimumRecordTime = commitContext.recordTime match {
          case Some(recordTime) =>
            // During the normal execution, in the deduplication state value we stored the record time
            // This allows us to compare the record times directly
            recordTime.toInstant
          case None =>
            // We select minimum record time for pre-execution
            // During pre-execution in the deduplication state value we stored the maximum record time
            // To guarantee the deduplication duration, we basically compare the maximum record time of the previous transaction
            // with the minimum record time of the current transaction. This gives us the smallest possible interval between two transactions.
            commitContext.minimumRecordTime
              .getOrElse(throw Err.InternalError("Minimum record time is not set for pre-execution"))
              .toInstant
        }
        val isDuplicate = maybeDedupValue
          .flatMap(commandDeduplication =>
            commandDeduplication.getTimeCase match {
              // Backward-compatibility, will not  be set for new entries
              case TimeCase.DEDUPLICATED_UNTIL =>
                Some(parseTimestamp(commandDeduplication.getDeduplicatedUntil).toInstant)
              // Set during normal execution, no time skews are added
              case TimeCase.RECORD_TIME =>
                val storedDuplicateRecordTime =
                  parseTimestamp(commandDeduplication.getRecordTime).toInstant
                Some(
                  storedDuplicateRecordTime
                    .plus(commandDeduplicationDuration)
                )
              // Set during pre-execution, time skews are already accounted for
              case TimeCase.MAX_RECORD_TIME =>
                val maxRecordTime =
                  parseTimestamp(commandDeduplication.getMaxRecordTime).toInstant
                Some(
                  maxRecordTime
                    .plus(commandDeduplicationDuration)
                )
              case TimeCase.TIME_NOT_SET =>
                None
            }
          )
          .forall(deduplicatedUntil => recordTimeOrMinimumRecordTime.isAfter(deduplicatedUntil))
        if (isDuplicate) {
          StepContinue(transactionEntry)
        } else {
          if (commitContext.preExecute) {
            // The out of time bounds entry is required in the committer, so we set it to the default value as we stop the steps here with the duplicate rejection
            commitContext.outOfTimeBoundsLogEntry = Some(DamlLogEntry.getDefaultInstance)
          }
          duplicateRejection(commitContext, transactionEntry)
        }
      }

      private def duplicateRejection(
          commitContext: CommitContext,
          transactionEntry: DamlTransactionEntrySummary,
      )(implicit loggingContext: LoggingContext) = {
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

  def setDeduplicationEntryStep(): Step =
    new Step {
      def apply(commitContext: CommitContext, transactionEntry: DamlTransactionEntrySummary)(
          implicit loggingContext: LoggingContext
      ): StepResult[DamlTransactionEntrySummary] = {
        if (!transactionEntry.submitterInfo.hasDeduplicationDuration) {
          throw Err.InvalidSubmission("Deduplication duration is not set.")
        }
        val commandDedupBuilder = DamlCommandDedupValue.newBuilder
        commitContext.recordTime
          .map(Conversions.buildTimestamp) match {
          case Some(recordTime) =>
            commandDedupBuilder.setRecordTime(recordTime)
          case None =>
            val maxRecordTime = commitContext.maximumRecordTime.getOrElse(
              throw Err.InternalError("Maximum record time is not set for pre-execution")
            )
            commandDedupBuilder.setMaxRecordTime(
              Conversions.buildTimestamp(maximumRecordTime)
            )
        }

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
}
