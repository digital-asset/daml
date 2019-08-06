// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.prettyEntryId
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

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    runSequence(
      checkTtl,
      authorizeSubmission,
      compareConfig,
      buildNewLogEntry
    )

  // Build a new configuration log entry from the current configuration.
  // FIXME(JM): We should just add a "configuration rejected" instead...
  private def buildCurrentConfigLogEntry: DamlLogEntry =
    DamlLogEntry.newBuilder
      .setRecordTime(buildTimestamp(recordTime))
      .setConfigurationEntry(buildDamlConfiguration(currentConfig))
      .build

  private def checkTtl(): Commit[Unit] = delay {
    // Check the maximum record time against the record time of the commit.
    // This mechanism allows the submitter to detect lost submissions and retry
    // with a submitter controlled rate.
    val maxRecordTime = parseTimestamp(configSubmission.getMaximumRecordTime)
    if (recordTime > maxRecordTime) {
      logger.warn(
        s"Rejected configuration submission. The submission timed out ($recordTime > $maxRecordTime)")
      done(buildCurrentConfigLogEntry)
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
      done(buildCurrentConfigLogEntry)
    } else {
      pass
    }
  }

  private def compareConfig(): Commit[Unit] = delay {
    // Compare the expected with the actual current configuration. This mechanism
    // gives us idempotency and safeguards against duplicate or reordered submissions.
    val expectedConfig = parseDamlConfiguration(configSubmission.getCurrentConfig).get
    if (expectedConfig != currentConfig)
      done(buildCurrentConfigLogEntry)
    else
      pass
  }

  private def buildNewLogEntry(): Commit[Unit] = sequence(
    delay {
      logger.trace(
        s"processSubmission[entryId=${prettyEntryId(entryId)}]: New configuration committed.")

      set(
        configurationStateKey ->
          DamlStateValue.newBuilder
            .setConfiguration(configSubmission.getNewConfig)
            .build)
    },
    done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setConfigurationEntry(configSubmission.getNewConfig)
        .build)
  )

  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val currentConfig
    : Configuration = // FIXME(JM): Share code, used in ProcessTransactionSubmission
    inputState
      .get(Conversions.configurationStateKey)
      .flatten
      .flatMap { v =>
        Conversions
          .parseDamlConfiguration(v.getConfiguration)
          .fold({ err =>
            logger.error(s"Failed to parse configuration: $err, using default configuration.")
            None
          }, Some(_))
      }
      .getOrElse(defaultConfig)

}
