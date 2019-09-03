// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

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

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val currentConfig =
    Common.getCurrentConfiguration(defaultConfig, inputState, logger)

  private val newConfig = configSubmission.getConfiguration

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    runSequence(
      inputState = Map.empty,
      checkTtl,
      authorizeSubmission,
      validateSubmission,
      buildLogEntry
    )

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

  private val buildLogEntry: Commit[Unit] = sequence(
    delay {
      logger.trace(
        s"processSubmission[entryId=${Pretty.prettyEntryId(entryId)}]: New configuration committed.")
      set(
        configurationStateKey ->
          DamlStateValue.newBuilder
            .setConfiguration(newConfig)
            .build)
    },
    done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setConfigurationEntry(
          DamlConfigurationEntry.newBuilder
            .setSubmissionId(configSubmission.getSubmissionId)
            .setParticipantId(participantId)
            .setConfiguration(configSubmission.getConfiguration))
        .build
    )
  )

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
}
