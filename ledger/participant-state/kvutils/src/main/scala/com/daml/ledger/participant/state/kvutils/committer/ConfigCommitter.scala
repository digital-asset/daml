// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committing.Common.getCurrentConfiguration
import com.daml.ledger.participant.state.v1.Configuration

private[kvutils] object ConfigCommitter {
  case class Result(
      builder: DamlConfigurationEntry.Builder,
      currentConfig: (Option[DamlConfigurationEntry], Configuration))
}

private[kvutils] case class ConfigCommitter(
    defaultConfig: Configuration
) extends Committer[DamlConfigurationSubmission, ConfigCommitter.Result] {

  private object Metrics {
    // kvutils.ConfigCommitter.*
    val accepts = metricsRegistry.counter("accepts")
    val rejections = metricsRegistry.counter("rejections")
  }

  private def rejectionTraceLog(
      msg: String,
      configurationEntry: DamlConfigurationEntry.Builder): Unit =
    logger.trace(
      s"Configuration rejected, $msg, correlationId=${configurationEntry.getSubmissionId}")

  private val checkTtl: Step = (ctx, result) => {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    if (ctx.getRecordTime > ctx.getMaximumRecordTime) {
      rejectionTraceLog(
        s"submission timed out (${ctx.getRecordTime} > ${ctx.getMaximumRecordTime})",
        result.builder)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.builder,
          _.setTimedOut(
            DamlConfigurationRejectionEntry.TimedOut.newBuilder
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
    val authorized = result.builder.getParticipantId == ctx.getParticipantId

    val wellFormed =
      result.currentConfig._1.forall(_.getParticipantId == ctx.getParticipantId)

    if (!authorized) {
      val msg =
        s"participant id ${result.builder.getParticipantId} did not match authenticated participant id ${ctx.getParticipantId}"
      rejectionTraceLog(msg, result.builder)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.builder,
          _.setParticipantNotAuthorized(
            DamlConfigurationRejectionEntry.ParticipantNotAuthorized.newBuilder
              .setDetails(msg)
          )))
    } else if (!wellFormed) {
      val msg = s"${ctx.getParticipantId} is not authorized to change configuration."
      rejectionTraceLog(msg, result.builder)
      StepStop(
        buildRejectionLogEntry(
          ctx,
          result.builder,
          _.setParticipantNotAuthorized(
            DamlConfigurationRejectionEntry.ParticipantNotAuthorized.newBuilder
              .setDetails(msg)
          )))
    } else {
      StepContinue(result)
    }
  }

  private val validateSubmission: Step = (ctx, result) => {
    Configuration
      .decode(result.builder.getConfiguration)
      .fold(
        err =>
          StepStop(
            buildRejectionLogEntry(
              ctx,
              result.builder,
              _.setInvalidConfiguration(
                DamlConfigurationRejectionEntry.InvalidConfiguration.newBuilder
                  .setError(err)))),
        config =>
          if (config.generation != (1 + result.currentConfig._2.generation))
            StepStop(
              buildRejectionLogEntry(
                ctx,
                result.builder,
                _.setGenerationMismatch(
                  DamlConfigurationRejectionEntry.GenerationMismatch.newBuilder
                    .setExpectedGeneration(1 + result.currentConfig._2.generation))
              )
            )
          else
            StepContinue(result)
      )
  }

  private def buildLogEntry: Step = (ctx, result) => {

    Metrics.accepts.inc()
    logger.trace(
      s"Configuration accepted, generation=${result.builder.getConfiguration.getGeneration} correlationId=${result.builder.getSubmissionId}")

    ctx.set(
      configurationStateKey,
      DamlStateValue.newBuilder
        .setConfigurationEntry(result.builder.build)
        .build
    )

    StepStop(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(ctx.getRecordTime))
        .setConfigurationEntry(result.builder)
        .build
    )
  }

  private def buildRejectionLogEntry(
      ctx: CommitContext,
      configurationEntry: DamlConfigurationEntry.Builder,
      addErrorDetails: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder)
    : DamlLogEntry = {
    Metrics.rejections.inc()
    DamlLogEntry.newBuilder
      .setRecordTime(buildTimestamp(ctx.getRecordTime))
      .setConfigurationRejectionEntry(
        addErrorDetails(
          DamlConfigurationRejectionEntry.newBuilder
            .setSubmissionId(configurationEntry.getSubmissionId)
            .setParticipantId(configurationEntry.getParticipantId)
            .setConfiguration(configurationEntry.getConfiguration)
        )
      )
      .build
  }

  override def init(
      ctx: CommitContext,
      configurationSubmission: DamlConfigurationSubmission): ConfigCommitter.Result =
    ConfigCommitter.Result(
      DamlConfigurationEntry.newBuilder
        .setSubmissionId(configurationSubmission.getSubmissionId)
        .setParticipantId(configurationSubmission.getParticipantId)
        .setConfiguration(configurationSubmission.getConfiguration),
      getCurrentConfiguration(defaultConfig, ctx.inputs, logger)
    )

  override val steps: Iterable[(StepInfo, Step)] = Iterable(
    "checkTTL" -> checkTtl,
    "authorizeSubmission" -> authorizeSubmission,
    "validateSubmission" -> validateSubmission,
    "buildLogEntry" -> buildLogEntry
  )

}
