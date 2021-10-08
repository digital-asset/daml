// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildTimestamp,
  configDedupKey,
  configurationStateKey,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils.store.{DamlStateValue, DamlSubmissionDedupValue}
import com.daml.ledger.participant.state.kvutils.store.events.DamlConfigurationEntry
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.entries.LoggingEntries
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

private[kvutils] object ConfigCommitter {

  final case class Result(
      submission: DamlConfigurationSubmission,
      currentConfig: (Option[DamlConfigurationEntry], Configuration),
  )

  type Step = CommitStep[Result]
}

private[kvutils] class ConfigCommitter(
    defaultConfig: Configuration,
    maximumRecordTime: Timestamp,
    override protected val metrics: Metrics,
) extends Committer[ConfigCommitter.Result] {

  import ConfigCommitter._

  private final val logger = ContextualizedLogger.get(getClass)

  override protected val committerName = "config"

  override protected def extraLoggingContext(result: Result): LoggingEntries =
    LoggingEntries("generation" -> result.submission.getConfiguration.getGeneration)

  override protected def init(
      ctx: CommitContext,
      submission: DamlSubmission,
  )(implicit loggingContext: LoggingContext): Result =
    ConfigCommitter.Result(
      submission.getConfigurationSubmission,
      getCurrentConfiguration(defaultConfig, ctx),
    )

  private def rejectionTraceLog(message: String)(implicit loggingContext: LoggingContext): Unit =
    logger.trace(s"Configuration rejected: $message.")

  private[committer] val checkTtl: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] =
      // Check the maximum record time against the record time of the commit.
      // This mechanism allows the submitter to detect lost submissions and retry
      // with a submitter controlled rate.
      if (ctx.recordTime.exists(_ > maximumRecordTime)) {
        rejectionTraceLog(s"submission timed out (${ctx.recordTime} > $maximumRecordTime)")
        reject(
          ctx.recordTime,
          result.submission,
          _.setTimedOut(TimedOut.newBuilder.setMaximumRecordTime(buildTimestamp(maximumRecordTime))),
        )
      } else {
        if (ctx.preExecute) {
          // Propagate the time bounds and defer the checks to post-execution.
          ctx.maximumRecordTime = Some(maximumRecordTime.toInstant)
          setOutOfTimeBoundsLogEntry(result.submission, ctx)
        }
        StepContinue(result)
      }
  }

  private val authorizeSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      // Submission is authorized when:
      //      the provided participant id matches source participant id
      //  AND (
      //      there exists no current configuration
      //   OR the current configuration's participant matches the submitting participant.
      //  )
      val authorized = result.submission.getParticipantId == ctx.participantId

      val wellFormed =
        result.currentConfig._1.forall(_.getParticipantId == ctx.participantId)

      if (!authorized) {
        val message =
          s"participant id ${result.submission.getParticipantId} did not match authenticated participant id ${ctx.participantId}"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result.submission,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder.setDetails(message)),
        )
      } else if (!wellFormed) {
        val message = s"${ctx.participantId} is not authorized to change configuration."
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result.submission,
          _.setParticipantNotAuthorized(ParticipantNotAuthorized.newBuilder.setDetails(message)),
        )
      } else {
        StepContinue(result)
      }
    }
  }

  private val validateSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] =
      Configuration
        .decode(result.submission.getConfiguration)
        .fold(
          err =>
            reject(
              ctx.recordTime,
              result.submission,
              _.setInvalidConfiguration(Invalid.newBuilder.setDetails(err)),
            ),
          config =>
            if (config.generation != (1 + result.currentConfig._2.generation))
              reject(
                ctx.recordTime,
                result.submission,
                _.setGenerationMismatch(
                  GenerationMismatch.newBuilder
                    .setExpectedGeneration(1 + result.currentConfig._2.generation)
                ),
              )
            else
              StepContinue(result),
        )
  }

  private val deduplicateSubmission: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      val submissionKey = configDedupKey(ctx.participantId, result.submission.getSubmissionId)
      if (ctx.get(submissionKey).isEmpty) {
        StepContinue(result)
      } else {
        val message = s"duplicate submission='${result.submission.getSubmissionId}'"
        rejectionTraceLog(message)
        reject(
          ctx.recordTime,
          result.submission,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(message)),
        )
      }
    }
  }

  private[committer] def buildLogEntry: Step = new Step {
    def apply(
        ctx: CommitContext,
        result: Result,
    )(implicit loggingContext: LoggingContext): StepResult[Result] = {
      metrics.daml.kvutils.committer.config.accepts.inc()
      logger.trace("Configuration accepted.")

      val configurationEntry = DamlConfigurationEntry.newBuilder
        .setSubmissionId(result.submission.getSubmissionId)
        .setParticipantId(result.submission.getParticipantId)
        .setConfiguration(result.submission.getConfiguration)
        .build
      ctx.set(
        configurationStateKey,
        DamlStateValue.newBuilder
          .setConfigurationEntry(configurationEntry)
          .build,
      )

      ctx.set(
        configDedupKey(ctx.participantId, result.submission.getSubmissionId),
        DamlStateValue.newBuilder
          .setSubmissionDedup(DamlSubmissionDedupValue.newBuilder)
          .build,
      )

      val successLogEntry = buildLogEntryWithOptionalRecordTime(
        ctx.recordTime,
        _.setConfigurationEntry(configurationEntry),
      )
      StepStop(successLogEntry)
    }
  }

  private def reject[PartialResult](
      recordTime: Option[Timestamp],
      submission: DamlConfigurationSubmission,
      addErrorDetails: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder,
  ): StepResult[PartialResult] = {
    metrics.daml.kvutils.committer.config.rejections.inc()
    StepStop(buildRejectionLogEntry(recordTime, submission, addErrorDetails))
  }

  private def setOutOfTimeBoundsLogEntry(
      submission: DamlConfigurationSubmission,
      commitContext: CommitContext,
  ): Unit = {
    commitContext.outOfTimeBoundsLogEntry = Some(
      buildRejectionLogEntry(recordTime = None, submission, identity)
    )
  }

  private def buildRejectionLogEntry(
      recordTime: Option[Timestamp],
      submission: DamlConfigurationSubmission,
      addErrorDetails: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder,
  ): DamlLogEntry = {
    buildLogEntryWithOptionalRecordTime(
      recordTime,
      _.setConfigurationRejectionEntry(
        addErrorDetails(
          DamlConfigurationRejectionEntry.newBuilder
            .setSubmissionId(submission.getSubmissionId)
            .setParticipantId(submission.getParticipantId)
            .setConfiguration(submission.getConfiguration)
        )
      ),
    )
  }

  override protected val steps: Steps[Result] = Iterable(
    "check_ttl" -> checkTtl,
    "authorize_submission" -> authorizeSubmission,
    "validate_submission" -> validateSubmission,
    "deduplicate_submission" -> deduplicateSubmission,
    "build_log_entry" -> buildLogEntry,
  )
}
