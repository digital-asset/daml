// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    runSequence(
      checkTtl,
      authorizeSubmission,
      validateSubmission,
      buildLogEntry
    )

  private def checkTtl(): Commit[Unit] = delay {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    val maxRecordTime = parseTimestamp(configSubmission.getMaximumRecordTime)
    if (recordTime > maxRecordTime) {
      logger.warn(
        s"Rejected configuration submission. The submission timed out ($recordTime > $maxRecordTime)")
      rejectTimedOut
    } else {
      pass
    }
  }

  private def authorizeSubmission(): Commit[Unit] = delay {
    // Submission is authorized when:
    //   1) The authorized participant is unset
    //   2) The authorized participant matches the submitting participant.
    val authorized =
      currentConfig.authorizedParticipantId
        .fold(true)(authPid => authPid == participantId)

    if (!authorized) {
      logger.warn(
        s"Rejected configuration submission. Authorized participant (${currentConfig.authorizedParticipantId}) does not match submitting participant $participantId.")
      rejectParticipantNotAuthorized
    } else {
      pass
    }
  }

  private def validateSubmission(): Commit[Unit] =
    parseDamlConfiguration(configSubmission.getConfiguration)
      .fold(exc => rejectInvalidConfiguration(exc.getMessage), pure)
      .flatMap { newConfig =>
        if (newConfig.generation != (1 + currentConfig.generation))
          rejectGenerationMismatch(1 + currentConfig.generation)
        else
          pass
      }

  private def buildLogEntry(): Commit[Unit] = sequence(
    delay {
      logger.trace(
        s"processSubmission[entryId=${Pretty.prettyEntryId(entryId)}]: New configuration committed.")

      set(
        configurationStateKey ->
          DamlStateValue.newBuilder
            .setConfiguration(configSubmission.getConfiguration)
            .build)
    },
    done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setConfigurationEntry(configSubmission.getConfiguration)
        .build)
  )

  private def rejectGenerationMismatch(expected: Long): Commit[Unit] =
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setConfiguration(configSubmission.getConfiguration)
            .setGenerationMismatch(
              DamlConfigurationRejectionEntry.GenerationMismatch.newBuilder
                .setExpectedGeneration(expected)
            )
        )
        .build
    )

  private def rejectInvalidConfiguration(error: String): Commit[Configuration] =
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setConfiguration(configSubmission.getConfiguration)
            .setInvalidConfiguration(
              DamlConfigurationRejectionEntry.InvalidConfiguration.newBuilder
                .setError(error)
            )
        )
        .build
    )

  private def rejectParticipantNotAuthorized[A]: Commit[A] =
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setConfiguration(configSubmission.getConfiguration)
            .setParticipantNotAuthorized(
              ParticipantNotAuthorized.newBuilder
                .setDetails(
                  s"Participant $participantId is not the authorized participant " +
                    currentConfig.authorizedParticipantId.getOrElse("<missing>")
                )
            )
        )
        .build
    )

  private def rejectTimedOut[A]: Commit[A] =
    done(
      DamlLogEntry.newBuilder
        .setConfigurationRejectionEntry(
          DamlConfigurationRejectionEntry.newBuilder
            .setConfiguration(configSubmission.getConfiguration)
            .setTimedOut(
              DamlConfigurationRejectionEntry.TimedOut.newBuilder
                .setMaximumRecordTime(configSubmission.getMaximumRecordTime)
            )
        )
        .build
    )

}
