// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildTimestamp,
  configDedupKey,
  configurationStateKey
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics

private[kvutils] object ConfigCommitter {
  case class Result(
      submission: DamlConfigurationSubmission,
      currentConfig: (Option[DamlConfigurationEntry], Configuration),
  )
}

private[kvutils] class ConfigCommitter(
    defaultConfig: Configuration,
    maximumRecordTime: Timestamp,
    override protected val metrics: Metrics
) extends Committer[ConfigCommitter.Result] {

  override protected val committerName = "config"

  private def rejectionTraceLog(msg: String, submission: DamlConfigurationSubmission): Unit =
    logger.trace(s"Configuration rejected, $msg, correlationId=${submission.getSubmissionId}")

  private[committer] val checkTtl: Step = (ctx, result) => {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    if (ctx.recordTime.exists(_ > maximumRecordTime)) {
      rejectionTraceLog(
        s"submission timed out (${ctx.recordTime} > $maximumRecordTime)",
        result.submission)
      reject(
        ctx.recordTime,
        result.submission,
        _.setTimedOut(
          TimedOut.newBuilder
            .setMaximumRecordTime(buildTimestamp(maximumRecordTime))
        )
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

  private val authorizeSubmission: Step = (ctx, result) => {
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
      val msg =
        s"participant id ${result.submission.getParticipantId} did not match authenticated participant id ${ctx.participantId}"
      rejectionTraceLog(msg, result.submission)
      reject(
        ctx.recordTime,
        result.submission,
        _.setParticipantNotAuthorized(
          ParticipantNotAuthorized.newBuilder
            .setDetails(msg)
        )
      )
    } else if (!wellFormed) {
      val msg = s"${ctx.participantId} is not authorized to change configuration."
      rejectionTraceLog(msg, result.submission)
      reject(
        ctx.recordTime,
        result.submission,
        _.setParticipantNotAuthorized(
          ParticipantNotAuthorized.newBuilder
            .setDetails(msg)
        )
      )
    } else {
      StepContinue(result)
    }
  }

  private val validateSubmission: Step = (ctx, result) => {
    Configuration
      .decode(result.submission.getConfiguration)
      .fold(
        err =>
          reject(
            ctx.recordTime,
            result.submission,
            _.setInvalidConfiguration(
              Invalid.newBuilder
                .setDetails(err))
        ),
        config =>
          if (config.generation != (1 + result.currentConfig._2.generation))
            reject(
              ctx.recordTime,
              result.submission,
              _.setGenerationMismatch(
                GenerationMismatch.newBuilder
                  .setExpectedGeneration(1 + result.currentConfig._2.generation))
            )
          else
            StepContinue(result)
      )
  }

  private val deduplicateSubmission: Step = (ctx, result) => {
    val submissionKey = configDedupKey(ctx.participantId, result.submission.getSubmissionId)
    if (ctx.get(submissionKey).isEmpty) {
      StepContinue(result)
    } else {
      val msg = s"duplicate submission='${result.submission.getSubmissionId}'"
      rejectionTraceLog(msg, result.submission)
      reject(
        ctx.recordTime,
        result.submission,
        _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(msg))
      )
    }
  }

  private[committer] def buildLogEntry: Step = (ctx, result) => {
    metrics.daml.kvutils.committer.config.accepts.inc()
    logger.trace(
      s"Configuration accepted, generation=${result.submission.getConfiguration.getGeneration} correlationId=${result.submission.getSubmissionId}")

    val configurationEntry = DamlConfigurationEntry.newBuilder
      .setSubmissionId(result.submission.getSubmissionId)
      .setParticipantId(result.submission.getParticipantId)
      .setConfiguration(result.submission.getConfiguration)
      .build
    ctx.set(
      configurationStateKey,
      DamlStateValue.newBuilder
        .setConfigurationEntry(configurationEntry)
        .build
    )

    ctx.set(
      configDedupKey(ctx.participantId, result.submission.getSubmissionId),
      DamlStateValue.newBuilder
        .setSubmissionDedup(DamlSubmissionDedupValue.newBuilder)
        .build
    )

    val successLogEntry = buildLogEntryWithOptionalRecordTime(
      ctx.recordTime,
      _.setConfigurationEntry(configurationEntry))
    StepStop(successLogEntry)
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
      commitContext: CommitContext): Unit = {
    commitContext.outOfTimeBoundsLogEntry = Some(
      buildRejectionLogEntry(recordTime = None, submission, identity))
  }

  private def buildRejectionLogEntry(
      recordTime: Option[Timestamp],
      submission: DamlConfigurationSubmission,
      addErrorDetails: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder
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
      )
    )
  }

  override protected def init(
      ctx: CommitContext,
      submission: DamlSubmission,
  ): ConfigCommitter.Result =
    ConfigCommitter.Result(
      submission.getConfigurationSubmission,
      getCurrentConfiguration(defaultConfig, ctx, logger)
    )

  override protected val steps: Iterable[(StepInfo, Step)] = Iterable(
    "check_ttl" -> checkTtl,
    "authorize_submission" -> authorizeSubmission,
    "validate_submission" -> validateSubmission,
    "deduplicate_submission" -> deduplicateSubmission,
    "build_log_entry" -> buildLogEntry
  )

}
