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
import com.daml.metrics.Metrics

private[kvutils] object ConfigCommitter {

  case class Result(
      submission: DamlConfigurationSubmission,
      currentConfig: (Option[DamlConfigurationEntry], Configuration),
  )

}

private[kvutils] class ConfigCommitter(
    defaultConfig: Configuration,
    override protected val metrics: Metrics,
) extends Committer[DamlConfigurationSubmission, ConfigCommitter.Result] {

  override protected val committerName = "config"

  private def rejectionTraceLog(msg: String, submission: DamlConfigurationSubmission): Unit =
    logger.trace(s"Configuration rejected, $msg, correlationId=${submission.getSubmissionId}")

  private val checkTtl: Step = (ctx, result) => {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    if (ctx.getRecordTime > ctx.getMaximumRecordTime) {
      rejectionTraceLog(
        s"submission timed out (${ctx.getRecordTime} > ${ctx.getMaximumRecordTime})",
        result.submission)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.submission,
          _.setTimedOut(
            TimedOut.newBuilder
              .setMaximumRecordTime(buildTimestamp(ctx.getMaximumRecordTime))
          )
        ))
    } else {
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
    val authorized = result.submission.getParticipantId == ctx.getParticipantId

    val wellFormed =
      result.currentConfig._1.forall(_.getParticipantId == ctx.getParticipantId)

    if (!authorized) {
      val msg =
        s"participant id ${result.submission.getParticipantId} did not match authenticated participant id ${ctx.getParticipantId}"
      rejectionTraceLog(msg, result.submission)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.submission,
          _.setParticipantNotAuthorized(
            ParticipantNotAuthorized.newBuilder
              .setDetails(msg)
          )))
    } else if (!wellFormed) {
      val msg = s"${ctx.getParticipantId} is not authorized to change configuration."
      rejectionTraceLog(msg, result.submission)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.submission,
          _.setParticipantNotAuthorized(
            ParticipantNotAuthorized.newBuilder
              .setDetails(msg)
          )))
    } else {
      StepContinue(result)
    }
  }

  private val validateSubmission: Step = (ctx, result) => {
    Configuration
      .decode(result.submission.getConfiguration)
      .fold(
        err =>
          StepStop(
            buildRejectionLogEntry(
              ctx,
              result.submission,
              _.setInvalidConfiguration(Invalid.newBuilder
                .setDetails(err)))),
        config =>
          if (config.generation != (1 + result.currentConfig._2.generation))
            StepStop(
              buildRejectionLogEntry(
                ctx,
                result.submission,
                _.setGenerationMismatch(GenerationMismatch.newBuilder
                  .setExpectedGeneration(1 + result.currentConfig._2.generation))
              )
            )
          else
            StepContinue(result)
      )
  }

  private val deduplicateSubmission: Step = (ctx, result) => {
    val submissionKey = configDedupKey(ctx.getParticipantId, result.submission.getSubmissionId)
    if (ctx.get(submissionKey).isEmpty) {
      StepContinue(result)
    } else {
      val msg = s"duplicate submission='${result.submission.getSubmissionId}'"
      rejectionTraceLog(msg, result.submission)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.submission,
          _.setDuplicateSubmission(Duplicate.newBuilder.setDetails(msg))
        )
      )
    }
  }

  private def buildLogEntry: Step = (ctx, result) => {

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
      configDedupKey(ctx.getParticipantId, result.submission.getSubmissionId),
      DamlStateValue.newBuilder
        .setSubmissionDedup(
          DamlSubmissionDedupValue.newBuilder
            .setRecordTime(buildTimestamp(ctx.getRecordTime))
            .build)
        .build
    )

    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setConfigurationEntry(configurationEntry)
        .build
    )
  }

  private def buildRejectionLogEntry(
      ctx: CommitContext,
      submission: DamlConfigurationSubmission,
      addErrorDetails: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder,
  ): DamlLogEntry = {
    metrics.daml.kvutils.committer.config.rejections.inc()
    DamlLogEntry.newBuilder
      .setRecordTime(buildTimestamp(ctx.getRecordTime))
      .setConfigurationRejectionEntry(
        addErrorDetails(
          DamlConfigurationRejectionEntry.newBuilder
            .setSubmissionId(submission.getSubmissionId)
            .setParticipantId(submission.getParticipantId)
            .setConfiguration(submission.getConfiguration)
        )
      )
      .build
  }

  override protected def init(
      ctx: CommitContext,
      configurationSubmission: DamlConfigurationSubmission,
  ): ConfigCommitter.Result =
    ConfigCommitter.Result(
      configurationSubmission,
      getCurrentConfiguration(defaultConfig, ctx.inputs, logger)
    )

  override protected val steps: Iterable[(StepInfo, Step)] = Iterable(
    "check_ttl" -> checkTtl,
    "authorize_submission" -> authorizeSubmission,
    "validate_submission" -> validateSubmission,
    "deduplicate_submission" -> deduplicateSubmission,
    "build_log_entry" -> buildLogEntry
  )

}
