// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Duration

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildDuration,
  commandDedupKey,
  parseDuration,
  parseInstant,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.getCurrentConfiguration
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.DamlCommandDedupValue.TimeCase
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlSubmitterInfo,
  DamlTransactionRejectionEntry,
  Duplicate,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlCommandDedupValue,
  DamlLogEntry,
  DamlStateValue,
  PreExecutionDeduplicationBounds,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.logging.LoggingContext

import scala.annotation.nowarn

private[transaction] object CommandDeduplication {

  /** Reject duplicate commands
    */
  @nowarn("msg=deprecated")
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
        val isNotADuplicate =
          isTheCommandNotADuplicate(commitContext, commandDeduplicationDuration, maybeDedupValue)
        if (isNotADuplicate) {
          StepContinue(transactionEntry)
        } else {
          if (commitContext.preExecute) {
            // The out of time bounds entry is required in the committer, so we set it to the default value as we stop the steps here with the duplicate rejection
            commitContext.outOfTimeBoundsLogEntry = Some(DamlLogEntry.getDefaultInstance)
            preExecutionDuplicateRejection(
              commitContext,
              transactionEntry,
              commandDeduplicationDuration,
              maybeDedupValue,
            )
          } else {
            duplicateRejection(commitContext, transactionEntry.submitterInfo, maybeDedupValue)
          }
        }
      }

      private def isTheCommandNotADuplicate(
          commitContext: CommitContext,
          commandDeduplicationDuration: Duration,
          maybeDedupValue: Option[DamlCommandDedupValue],
      ) = {
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
              .getOrElse(
                throw Err.InternalError("Minimum record time is not set for pre-execution")
              )
              .toInstant
        }
        maybeDedupValue
          .flatMap(commandDeduplication =>
            commandDeduplication.getTimeCase match {
              // Backward-compatibility, will not be set for new entries
              case TimeCase.DEDUPLICATED_UNTIL =>
                Some(parseTimestamp(commandDeduplication.getDeduplicatedUntil))
              // Set during normal execution, no time skews are added
              case TimeCase.RECORD_TIME =>
                val storedDuplicateRecordTime =
                  parseTimestamp(commandDeduplication.getRecordTime)
                Some(
                  storedDuplicateRecordTime
                    .add(commandDeduplicationDuration)
                )
              // Set during pre-execution, time skews are already accounted for
              case TimeCase.RECORD_TIME_BOUNDS =>
                val maxRecordTime =
                  parseTimestamp(commandDeduplication.getRecordTimeBounds.getMaxRecordTime)
                Some(
                  maxRecordTime
                    .add(commandDeduplicationDuration)
                )
              case TimeCase.TIME_NOT_SET =>
                None
            }
          )
          .forall(deduplicatedUntil =>
            recordTimeOrMinimumRecordTime.isAfter(deduplicatedUntil.toInstant)
          )
      }

      private def preExecutionDuplicateRejection(
          commitContext: CommitContext,
          transactionEntry: DamlTransactionEntrySummary,
          commandDeduplicationDuration: Duration,
          maybeDedupValue: Option[DamlCommandDedupValue],
      )(implicit loggingContext: LoggingContext) = {
        maybeDedupValue.collect {
          case dedupValue if dedupValue.hasRecordTimeBounds => dedupValue.getRecordTimeBounds
        } match {
          case Some(recordTimeBounds) =>
            val currentCommandMaxRecordTime = commitContext.maximumRecordTime.getOrElse(
              throw Err.InternalError("Maximum record time is not set for pre-execution")
            )
            val maxDurationBetweenRecords = Duration.between(
              parseInstant(recordTimeBounds.getMinRecordTime),
              currentCommandMaxRecordTime.toInstant,
            )
            // We use min and max record time to determine if a command is a duplicate.
            // These boundaries account for time skews.
            // To guarantee that the interval between the the previous command record time and the rejection record time
            // is bigger or equal compared to rejection deduplication duration we select the max duration between
            // the passed deduplication duration and the maximum possible interval between the two commands.
            val rejectionDeduplicationDuration =
              Seq(maxDurationBetweenRecords, commandDeduplicationDuration).max
            duplicateRejection(
              commitContext,
              transactionEntry.submitterInfo.toBuilder
                .setDeduplicationDuration(
                  buildDuration(rejectionDeduplicationDuration)
                )
                .build,
              maybeDedupValue,
            )
          case None =>
            duplicateRejection(commitContext, transactionEntry.submitterInfo, maybeDedupValue)
        }
      }

      private def duplicateRejection(
          commitContext: CommitContext,
          submitterInfo: DamlSubmitterInfo,
          dedupValue: Option[DamlCommandDedupValue],
      )(implicit loggingContext: LoggingContext) = {
        rejections.reject(
          DamlTransactionRejectionEntry.newBuilder
            .setSubmitterInfo(submitterInfo)
            // No duplicate rejection is a definite answer as the deduplication entry will eventually expire.
            .setDefiniteAnswer(false)
            .setDuplicateCommand(
              Duplicate.newBuilder
                .setDetails("")
                .setSubmissionId(dedupValue.map(_.getSubmissionId).getOrElse(""))
            ),
          "the command is a duplicate",
          commitContext.recordTime,
        )
      }
    }

  def setDeduplicationEntryStep(defaultConfig: Configuration): Step =
    new Step {
      def apply(commitContext: CommitContext, transactionEntry: DamlTransactionEntrySummary)(
          implicit loggingContext: LoggingContext
      ): StepResult[DamlTransactionEntrySummary] = {
        if (!transactionEntry.submitterInfo.hasDeduplicationDuration) {
          throw Err.InvalidSubmission("Deduplication duration is not set.")
        }
        val commandDedupBuilder = DamlCommandDedupValue.newBuilder.setSubmissionId(
          transactionEntry.submitterInfo.getSubmissionId
        )
        val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
        // build the maximum interval for which we might use the deduplication entry
        // we account for both time skews even if it means that the expiry time would be slightly longer than required
        val expireInterval =
          config.maxDeduplicationTime.plus(config.timeModel.maxSkew).plus(config.timeModel.minSkew)
        commitContext.recordTime match {
          case Some(recordTime) =>
            val expireAt = recordTime.add(expireInterval)
            commandDedupBuilder
              .setRecordTime(Conversions.buildTimestamp(recordTime))
              .setExpireAt(Conversions.buildTimestamp(expireAt))
          case None =>
            val maxRecordTime = commitContext.maximumRecordTime.getOrElse(
              throw Err.InternalError("Maximum record time is not set for pre-execution")
            )
            val minRecordTime = commitContext.minimumRecordTime.getOrElse(
              throw Err.InternalError("Minimum record time is not set for pre-execution")
            )
            val expireAt = maxRecordTime.add(expireInterval)
            commandDedupBuilder
              .setRecordTimeBounds(
                PreExecutionDeduplicationBounds.newBuilder
                  .setMaxRecordTime(Conversions.buildTimestamp(maxRecordTime))
                  .setMinRecordTime(Conversions.buildTimestamp(minRecordTime))
              )
              .setExpireAt(Conversions.buildTimestamp(expireAt))
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
