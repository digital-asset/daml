// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.Conversions.{commandDedupKey, parseTimestamp}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry
import com.daml.ledger.participant.state.kvutils.committer.Committer.getCurrentConfiguration
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Step,
  TransactionRejector,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReasonV0, TimeModel}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext

private[transaction] class LedgerTimeValidator(defaultConfig: Configuration)
    extends TransactionValidator {

  /** Creates a committer step that validates ledger effective time and the command's time-to-live. */
  override def createValidationStep(rejector: TransactionRejector): Step =
    new Step {
      def apply(
          commitContext: CommitContext,
          transactionEntry: DamlTransactionEntrySummary,
      )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
        val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
        val timeModel = config.timeModel

        commitContext.recordTime match {
          case Some(recordTime) =>
            val givenLedgerTime = transactionEntry.ledgerEffectiveTime.toInstant

            timeModel
              .checkTime(ledgerTime = givenLedgerTime, recordTime = recordTime.toInstant)
              .fold(
                reason =>
                  rejector.reject(
                    rejector.buildRejectionEntry(
                      transactionEntry,
                      RejectionReasonV0.InvalidLedgerTime(reason),
                    ),
                    commitContext.recordTime,
                  ),
                _ => StepContinue(transactionEntry),
              )
          case None => // Pre-execution: propagate the time bounds and defer the checks to post-execution.
            val maybeDeduplicateUntil =
              getLedgerDeduplicateUntil(transactionEntry, commitContext)
            val minimumRecordTime = transactionMinRecordTime(
              transactionEntry.submissionTime.toInstant,
              transactionEntry.ledgerEffectiveTime.toInstant,
              maybeDeduplicateUntil,
              timeModel,
            )
            val maximumRecordTime = transactionMaxRecordTime(
              transactionEntry.submissionTime.toInstant,
              transactionEntry.ledgerEffectiveTime.toInstant,
              timeModel,
            )
            commitContext.deduplicateUntil = maybeDeduplicateUntil
            commitContext.minimumRecordTime = Some(minimumRecordTime)
            commitContext.maximumRecordTime = Some(maximumRecordTime)
            val outOfTimeBoundsLogEntry = DamlLogEntry.newBuilder
              .setTransactionRejectionEntry(
                rejector.buildRejectionEntry(
                  transactionEntry,
                  RejectionReasonV0.InvalidLedgerTime(
                    s"Record time is outside of valid range [$minimumRecordTime, $maximumRecordTime]"
                  ),
                )
              )
              .build
            commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
            StepContinue(transactionEntry)
        }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  private def transactionMinRecordTime(
      submissionTime: Instant,
      ledgerTime: Instant,
      maybeDeduplicateUntil: Option[Instant],
      timeModel: TimeModel,
  ): Instant =
    List(
      maybeDeduplicateUntil
        .map(
          _.plus(Timestamp.Resolution)
        ), // DeduplicateUntil defines a rejection window, endpoints inclusive
      Some(timeModel.minRecordTime(ledgerTime)),
      Some(timeModel.minRecordTime(submissionTime)),
    ).flatten.max

  private def transactionMaxRecordTime(
      submissionTime: Instant,
      ledgerTime: Instant,
      timeModel: TimeModel,
  ): Instant =
    List(timeModel.maxRecordTime(ledgerTime), timeModel.maxRecordTime(submissionTime)).min

  private def getLedgerDeduplicateUntil(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext,
  ): Option[Instant] =
    for {
      dedupEntry <- commitContext.get(commandDedupKey(transactionEntry.submitterInfo))
      dedupTimestamp <- PartialFunction.condOpt(dedupEntry.getCommandDedup.hasDeduplicatedUntil) {
        case true => dedupEntry.getCommandDedup.getDeduplicatedUntil
      }
    } yield parseTimestamp(dedupTimestamp).toInstant
}
