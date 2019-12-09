// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.DamlStateMap
import com.daml.ledger.participant.state.kvutils.committing.Common.getCurrentConfiguration
import com.daml.ledger.participant.state.v1.Configuration

private[kvutils] case class ConfigCommitter(
    defaultConfig: Configuration,
    inputState: DamlStateMap //TODO(MZ) move to accumulator
) extends Committer[DamlConfigurationSubmission, DamlConfigurationEntry.Builder] {

  private object Metrics {
    // kvutils.ConfigCommitter.*
    val accepts = metricsRegistry.counter("accepts")
    val rejections = metricsRegistry.counter("rejections")
  }

  private val (currentConfigEntry, currentConfig) =
    getCurrentConfiguration(defaultConfig, inputState, logger)

  private def rejectionTraceLog(
      msg: String,
      configurationEntry: DamlConfigurationEntry.Builder): Unit =
    logger.trace(
      s"Configuration rejected, $msg, correlationId=${configurationEntry.getSubmissionId}")

  private val checkTtl: Step = (ctx, configurationEntry) => {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    if (ctx.getRecordTime > ctx.getMaximumRecordTime) {
      rejectionTraceLog(
        s"submission timed out (${ctx.getRecordTime} > ${ctx.getMaximumRecordTime})",
        configurationEntry)
      StepStop(
        buildRejectionLogEntry(
          configurationEntry,
          _.setTimedOut(
            DamlConfigurationRejectionEntry.TimedOut.newBuilder
              .setMaximumRecordTime(buildTimestamp(ctx.getMaximumRecordTime))
          )))
    } else {
      StepContinue(configurationEntry)
    }
  }

  private val authorizeSubmission: Step = (ctx, configurationEntry) => {
    // Submission is authorized when:
    //      the provided participant id matches source participant id
    //  AND (
    //      there exists no current configuration
    //   OR the current configuration's participant matches the submitting participant.
    //  )
    val authorized = configurationEntry.getParticipantId == ctx.getParticipantId

    val wellFormed =
      currentConfigEntry.forall(_.getParticipantId == ctx.getParticipantId)

    if (!authorized) {
      val msg =
        s"participant id ${configurationEntry.getParticipantId} did not match authenticated participant id ${ctx.getParticipantId}"
      rejectionTraceLog(msg, configurationEntry)
      StepStop(
        buildRejectionLogEntry(
          configurationEntry,
          _.setParticipantNotAuthorized(
            DamlConfigurationRejectionEntry.ParticipantNotAuthorized.newBuilder
              .setDetails(msg)
          )))
    } else if (!wellFormed) {
      val msg = s"${ctx.getParticipantId} is not authorized to change configuration."
      rejectionTraceLog(msg, configurationEntry)
      StepStop(
        buildRejectionLogEntry(
          configurationEntry,
          _.setParticipantNotAuthorized(
            DamlConfigurationRejectionEntry.ParticipantNotAuthorized.newBuilder
              .setDetails(msg)
          )))
    } else {
      StepContinue(configurationEntry)
    }
  }

  private val validateSubmission: Step = (ctx, configurationEntry) => {
    Configuration
      .decode(configurationEntry.getConfiguration)
      .fold(
        err =>
          StepStop(
            buildRejectionLogEntry(
              configurationEntry,
              _.setInvalidConfiguration(
                DamlConfigurationRejectionEntry.InvalidConfiguration.newBuilder
                  .setError(err)))),
        config =>
          if (config.generation != (1 + currentConfig.generation))
            StepStop(
              buildRejectionLogEntry(
                configurationEntry,
                _.setGenerationMismatch(
                  DamlConfigurationRejectionEntry.GenerationMismatch.newBuilder
                    .setExpectedGeneration(1 + currentConfig.generation))
              )
            )
          else
            StepContinue(configurationEntry)
      )
  }

  private def buildLogEntry: Step = (ctx, configurationEntry) => {

    Metrics.accepts.inc()
    logger.trace(
      s"Configuration accepted, generation=${configurationEntry.getConfiguration.getGeneration} correlationId=${configurationEntry.getSubmissionId}")

    ctx.set(
      configurationStateKey,
      DamlStateValue.newBuilder
        .setConfigurationEntry(configurationEntry.build)
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
      configurationEntry: DamlConfigurationEntry.Builder,
      addErrorDetails: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder)
    : DamlLogEntry = {
    Metrics.rejections.inc()
    DamlLogEntry.newBuilder
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
      configurationSubmission: DamlConfigurationSubmission): DamlConfigurationEntry.Builder =
    DamlConfigurationEntry.newBuilder
      .setSubmissionId(configurationSubmission.getSubmissionId)
      .setParticipantId(configurationSubmission.getParticipantId)
      .setConfiguration(configurationSubmission.getConfiguration)

  override val steps: Iterable[(StepInfo, Step)] = Iterable(
    "checkTTL" -> checkTtl,
    "authorizeSubmission" -> authorizeSubmission,
    "validateSubmission" -> validateSubmission,
    "buildLogEntry" -> buildLogEntry
  )

}
