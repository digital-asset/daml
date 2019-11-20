// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import com.codahale.metrics
import com.daml.ledger.participant.state.kvutils.Pretty
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory

private[kvutils] case class ProcessConfigSubmission(
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    defaultConfig: Configuration,
    participantId: ParticipantId,
    configSubmission: DamlConfigurationSubmission,
    inputState: Map[DamlStateKey, Option[DamlStateValue]]) {

  import Common._
  import Commit._
  import ProcessConfigSubmission._

  private implicit val logger =
    LoggerFactory.getLogger(
      s"ProcessConfigSubmission[entryId=${Pretty.prettyEntryId(entryId)}, submId=${configSubmission.getSubmissionId}]")
  private val currentConfig =
    Common.getCurrentConfiguration(defaultConfig, inputState, logger)

  private val newConfig = configSubmission.getConfiguration

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = Metrics.runTimer.time { () =>
    runSequence(
      inputState = Map.empty,
      "Check TTL" -> checkTtl,
      "Authorize" -> authorizeSubmission,
      "Validate" -> validateSubmission,
      "Build" -> buildLogEntry
    )
  }

  private val checkTtl: Commit[Unit] = delay {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    val maxRecordTime = parseTimestamp(configSubmission.getMaximumRecordTime)
    if (recordTime > maxRecordTime) {
      logger.warn(
        s"Rejected configuration submission. The submission timed out ($recordTime > $maxRecordTime)")
      reject(
        _.setTimedOut(
          DamlConfigurationRejectionEntry.TimedOut.newBuilder
            .setMaximumRecordTime(configSubmission.getMaximumRecordTime)
        ))
    } else {
      pass
    }
  }

  private val authorizeSubmission: Commit[Unit] = delay {
    // Submission is authorized when:
    //      the provided participant id matches source participant id
    //  AND (
    //      the authorized participant is unset
    //   OR the authorized participant matches the submitting participant.
    //  )
    val wellFormed =
      participantId == configSubmission.getParticipantId

    val authorized =
      currentConfig.authorizedParticipantId
        .fold(true)(authPid => authPid == participantId)

    if (wellFormed && authorized) {
      pass
    } else {
      logger.warn(
        s"Rejected configuration submission. Authorized participant (${currentConfig.authorizedParticipantId}) does not match submitting participant $participantId.")

      reject(
        _.setParticipantNotAuthorized(
          DamlConfigurationRejectionEntry.ParticipantNotAuthorized.newBuilder
            .setDetails(
              s"Participant $participantId is not the authorized participant " +
                currentConfig.authorizedParticipantId.getOrElse("<missing>")
            )
        ))
    }
  }

  private val validateSubmission: Commit[Unit] =
    Configuration
      .decode(newConfig)
      .fold(
        err =>
          reject(
            _.setInvalidConfiguration(
              DamlConfigurationRejectionEntry.InvalidConfiguration.newBuilder
                .setError(err))),
        pure)
      .flatMap { config =>
        if (config.generation != (1 + currentConfig.generation))
          reject(
            _.setGenerationMismatch(DamlConfigurationRejectionEntry.GenerationMismatch.newBuilder
              .setExpectedGeneration(1 + currentConfig.generation)))
        else
          pass
      }

  private def buildLogEntry(): Commit[Unit] = sequence2(
    delay {
      Metrics.accepts.inc()
      logger.trace(s"New configuration with generation ${newConfig.getGeneration} accepted.")
      set(
        configurationStateKey ->
          DamlStateValue.newBuilder
            .setConfiguration(newConfig)
            .build)
    },
    done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setConfigurationEntry(DamlConfigurationEntry.newBuilder
          .setSubmissionId(configSubmission.getSubmissionId)
          .setConfiguration(configSubmission.getConfiguration))
        .build)
  )

  private def rejectGenerationMismatch(expected: Long): Commit[Unit] = {
    Metrics.rejections.inc()
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setSubmissionId(configSubmission.getSubmissionId)
            .setConfiguration(configSubmission.getConfiguration)
            .setGenerationMismatch(
              DamlConfigurationRejectionEntry.GenerationMismatch.newBuilder
                .setExpectedGeneration(expected)
            )
        )
        .setConfigurationEntry(
          DamlConfigurationEntry.newBuilder
            .setSubmissionId(configSubmission.getSubmissionId)
            .setParticipantId(participantId)
            .setConfiguration(configSubmission.getConfiguration))
        .build
  )
  }

  private def reject[A](
                         addReason: DamlConfigurationRejectionEntry.Builder => DamlConfigurationRejectionEntry.Builder)
  : Commit[A] =
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          addReason(
            DamlConfigurationRejectionEntry.newBuilder
              .setSubmissionId(configSubmission.getSubmissionId)
              .setParticipantId(participantId)
              .setConfiguration(configSubmission.getConfiguration)
          )
        )
        .build
    )

  private def rejectParticipantNotAuthorized[A]: Commit[A] = {
    Metrics.rejections.inc()
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setSubmissionId(configSubmission.getSubmissionId)
            .setConfiguration(configSubmission.getConfiguration)
            .setParticipantNotAuthorized(
              DamlConfigurationRejectionEntry.ParticipantNotAuthorized.newBuilder
                .setDetails(
                  s"Participant $participantId is not the authorized participant " +
                    currentConfig.authorizedParticipantId.getOrElse("<missing>")
                )
            )
        )
        .build
    )
  }

  private def rejectTimedOut[A]: Commit[A] = {
    Metrics.rejections.inc()
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setSubmissionId(configSubmission.getSubmissionId)
            .setConfiguration(configSubmission.getConfiguration)
            .setTimedOut(
              DamlConfigurationRejectionEntry.TimedOut.newBuilder
                .setMaximumRecordTime(configSubmission.getMaximumRecordTime)
            )
        )
        .build
    )
  }
}

private[kvutils] object ProcessConfigSubmission {
  private[committing] object Metrics {
    private val registry = metrics.SharedMetricRegistries.getOrCreate("kvutils")
    private val prefix = "kvutils.committing.config"
    val runTimer = registry.timer("run-timer")
    val accepts = registry.counter("accepts")
    val rejections = registry.counter("rejections")
  }
}
