// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationRejectionEntry,
  DamlLogEntry,
  DamlLogEntryId,
  DamlOutOfTimeBoundsEntry,
  DamlPackageUploadEntry,
  DamlSubmitterInfo,
  DamlTransactionRejectionEntry
}
import org.scalatest.{Matchers, WordSpec}
import KeyValueConsumption.{logEntryToUpdate, outOfTimeBoundsEntryToUpdate}
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason, Update}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

class KeyValueConsumptionSpec extends WordSpec with Matchers {
  private val aLogEntryId = DamlLogEntryId.getDefaultInstance
  private val aLogEntryWithoutRecordTime = DamlLogEntry.newBuilder
    .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
    .build
  private val aRecordTimeForUpdate = Timestamp(123456789)
  private val aRecordTimeForUpdateInstant = aRecordTimeForUpdate.toInstant
  private val aLogEntryWithRecordTime = DamlLogEntry.newBuilder
    .setRecordTime(com.google.protobuf.Timestamp.newBuilder.setSeconds(100))
    .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
    .build
  private val someSubmitterInfo = DamlSubmitterInfo.newBuilder
    .setSubmitter("a submitter")
    .setApplicationId("test")
    .setCommandId("a command ID")
    .setDeduplicateUntil(com.google.protobuf.Timestamp.getDefaultInstance)
    .build
  private val aLogEntryWithTransactionRejectionEntry = DamlLogEntry.newBuilder
    .setTransactionRejectionEntry(
      DamlTransactionRejectionEntry.newBuilder
        .setSubmitterInfo(someSubmitterInfo))
    .build
  private val aLogEntryWithConfigurationRejectionEntry = DamlLogEntry.newBuilder
    .setConfigurationRejectionEntry(
      DamlConfigurationRejectionEntry.newBuilder
        .setConfiguration(Configuration.encode(LedgerReader.DefaultConfiguration))
        .setSubmissionId("a submission")
        .setParticipantId("a participant"))
    .build

  "logEntryToUpdate" should {
    "throw in case no record time is available from the log entry or input argument" in {
      assertThrows[Err](
        logEntryToUpdate(aLogEntryId, aLogEntryWithoutRecordTime, recordTimeForUpdate = None))
    }

    "use record time provided as input instead of log entry's" in {
      val actual :: Nil = logEntryToUpdate(
        aLogEntryId,
        aLogEntryWithRecordTime,
        recordTimeForUpdate = Some(aRecordTimeForUpdate))

      actual.recordTime shouldBe aRecordTimeForUpdate
    }

    "use record time from log entry if not provided as input" in {
      val actual :: Nil =
        logEntryToUpdate(aLogEntryId, aLogEntryWithRecordTime, recordTimeForUpdate = None)

      actual.recordTime shouldBe Timestamp.assertFromInstant(Instant.ofEpochSecond(100))
    }
  }

  "outOfTimeBoundsEntryToUpdate" should {
    "not generate an update for a deduplicated transaction entry" in {
      val inputEntry = DamlOutOfTimeBoundsEntry.newBuilder
        .setEntry(aLogEntryWithTransactionRejectionEntry)
        .setDuplicateUntil(
          Conversions.buildTimestamp(Timestamp.assertFromInstant(aRecordTimeForUpdateInstant)))
        .build

      outOfTimeBoundsEntryToUpdate(aRecordTimeForUpdate, inputEntry) shouldBe None
    }

    "generate a rejection entry for a transaction if record time is too late" in {
      val inputEntry = DamlOutOfTimeBoundsEntry.newBuilder
        .setEntry(aLogEntryWithTransactionRejectionEntry)
        .setTooLateFrom(Conversions.buildTimestamp(
          Timestamp.assertFromInstant(aRecordTimeForUpdateInstant.minusMillis(1))))
        .build

      val expectedUpdate = Update.CommandRejected(
        recordTime = aRecordTimeForUpdate,
        submitterInfo = Conversions.parseSubmitterInfo(someSubmitterInfo),
        reason = RejectionReason.InvalidLedgerTime("Ledger time outside of valid range")
      )
      outOfTimeBoundsEntryToUpdate(aRecordTimeForUpdate, inputEntry) shouldBe Some(expectedUpdate)
    }

    "generate a rejection entry for a transaction if record time is too early" in {
      val inputEntry = DamlOutOfTimeBoundsEntry.newBuilder
        .setEntry(aLogEntryWithTransactionRejectionEntry)
        .setTooEarlyUntil(Conversions.buildTimestamp(
          Timestamp.assertFromInstant(aRecordTimeForUpdateInstant.plusMillis(1))))
        .build

      val expectedUpdate = Update.CommandRejected(
        recordTime = aRecordTimeForUpdate,
        submitterInfo = Conversions.parseSubmitterInfo(someSubmitterInfo),
        reason = RejectionReason.InvalidLedgerTime("Ledger time outside of valid range")
      )
      outOfTimeBoundsEntryToUpdate(aRecordTimeForUpdate, inputEntry) shouldBe Some(expectedUpdate)
    }

    "generate a rejection entry for a configuration if record time is too late" in {
      val inputEntry = DamlOutOfTimeBoundsEntry.newBuilder
        .setEntry(aLogEntryWithConfigurationRejectionEntry)
        .setTooLateFrom(Conversions.buildTimestamp(
          Timestamp.assertFromInstant(aRecordTimeForUpdateInstant.minusMillis(1))))
        .build

      val expectedUpdate = Update.ConfigurationChangeRejected(
        recordTime = aRecordTimeForUpdate,
        submissionId = v1.SubmissionId.assertFromString(
          aLogEntryWithConfigurationRejectionEntry.getConfigurationRejectionEntry.getSubmissionId),
        participantId = Ref.ParticipantId.assertFromString(
          aLogEntryWithConfigurationRejectionEntry.getConfigurationRejectionEntry.getParticipantId),
        proposedConfiguration = Configuration
          .decode(
            aLogEntryWithConfigurationRejectionEntry.getConfigurationRejectionEntry.getConfiguration)
          .getOrElse(fail),
        rejectionReason =
          s"Configuration change timed out: ${aRecordTimeForUpdateInstant.minusMillis(1)} < $aRecordTimeForUpdateInstant"
      )
      outOfTimeBoundsEntryToUpdate(aRecordTimeForUpdate, inputEntry) shouldBe Some(expectedUpdate)
    }
  }
}
