// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.error.ValueSwitch
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.ledger.participant.state.kvutils.Conversions.{buildTimestamp, parseInstant}
import com.daml.ledger.participant.state.kvutils.KeyValueConsumption.{
  TimeBounds,
  logEntryToUpdate,
  outOfTimeBoundsEntryToUpdate,
}
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntry.PayloadCase._
import com.daml.ledger.participant.state.kvutils.store.events.PackageUpload.{
  DamlPackageUploadEntry,
  DamlPackageUploadRejectionEntry,
}
import com.daml.ledger.participant.state.kvutils.store.events._
import com.daml.ledger.participant.state.kvutils.store.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlOutOfTimeBoundsEntry,
}
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.CommandRejected
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.lf.data.Time.Timestamp
import com.google.protobuf.any.{Any => AnyProto}
import com.google.protobuf.{ByteString, Empty}
import com.google.rpc.code.Code
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.{TableFor4, TableFor5}
import org.scalatest.prop.Tables.Table
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class KeyValueConsumptionSpec extends AnyWordSpec with Matchers {
  private val aLogEntryIdString = "test"
  private val aLogEntryId =
    DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(aLogEntryIdString)).build()
  private val aLogEntryWithoutRecordTime = DamlLogEntry.newBuilder
    .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
    .build
  private val aRecordTime = Timestamp(123456789)
  private val aRecordTimeInstant = aRecordTime.toInstant
  private val aRecordTimeFromLogEntry = Timestamp.assertFromInstant(Instant.ofEpochSecond(100))
  private val aLogEntryWithRecordTime = DamlLogEntry.newBuilder
    .setRecordTime(Conversions.buildTimestamp(aRecordTimeFromLogEntry))
    .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
    .build

  "logEntryToUpdate" should {
    "throw in case no record time is available from the log entry or input argument" in {
      forAll(
        Table[ValueSwitch[Status]](
          "Error Version",
          v1ErrorSwitch,
          v2ErrorSwitch,
        )
      ) { errorSwitch =>
        assertThrows[Err](
          logEntryToUpdate(
            aLogEntryId,
            aLogEntryWithoutRecordTime,
            errorSwitch,
            recordTimeForUpdate = None,
          )
        )
      }
    }

    "use log entry's record time instead of one provided as input" in {
      forAll(
        Table[ValueSwitch[Status]](
          "Error Version",
          v1ErrorSwitch,
          v2ErrorSwitch,
        )
      ) { errorSwitch =>
        val actual :: Nil = logEntryToUpdate(
          aLogEntryId,
          aLogEntryWithRecordTime,
          errorSwitch,
          recordTimeForUpdate = Some(aRecordTime),
        )

        actual.recordTime shouldBe aRecordTimeFromLogEntry
      }
    }

    "use record time from log entry if not provided as input" in {
      forAll(
        Table[ValueSwitch[Status]](
          "Error Version",
          v1ErrorSwitch,
          v2ErrorSwitch,
        )
      ) { errorSwitch =>
        val actual :: Nil =
          logEntryToUpdate(
            aLogEntryId,
            aLogEntryWithRecordTime,
            errorSwitch,
            recordTimeForUpdate = None,
          )

        actual.recordTime shouldBe Timestamp.assertFromInstant(Instant.ofEpochSecond(100))
      }
    }

    "not generate an update from a time update entry" in {
      forAll(
        Table[ValueSwitch[Status]](
          "Error Version",
          v1ErrorSwitch,
          v2ErrorSwitch,
        )
      ) { errorSwitch =>
        val timeUpdateEntry = DamlLogEntry.newBuilder
          .setRecordTime(Conversions.buildTimestamp(aRecordTime))
          .setTimeUpdateEntry(Empty.getDefaultInstance)
          .build
        logEntryToUpdate(
          aLogEntryId,
          timeUpdateEntry,
          errorSwitch,
          recordTimeForUpdate = None,
        ) shouldBe Nil
      }
    }
  }

  private def verifyNoUpdateIsGenerated(actual: Option[Update]): Unit = {
    actual should be(None)
    ()
  }

  case class Assertions(
      verify: Option[Update] => Unit = verifyNoUpdateIsGenerated,
      throwsInternalError: Boolean = false,
  )

  "outOfTimeBoundsEntryToUpdate" should {
    "generate update only for rejected and deduplicated transaction" in {
      val testCases = Table(
        ("Time Bounds", "Record Time", "Log Entry Type", "Assertions"),
        (
          TimeBounds(deduplicateUntil = Some(aRecordTime)),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(update =>
            inside(update) { case Some(CommandRejected(_, _, FinalReason(status))) =>
              status.code shouldBe Code.ALREADY_EXISTS.value
              status.details shouldBe Seq(
                AnyProto.pack[ErrorInfo](
                  ErrorInfo(metadata = Map(GrpcStatuses.DefiniteAnswerKey -> "false"))
                )
              )
              ()
            }
          ),
        ),
        (
          TimeBounds(
            deduplicateUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1)))
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(deduplicateUntil = Some(aRecordTime)),
          aRecordTime,
          PACKAGE_UPLOAD_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(deduplicateUntil = Some(aRecordTime)),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(deduplicateUntil = Some(aRecordTime)),
          aRecordTime,
          PARTY_ALLOCATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
      )
      runAll(testCases)
    }

    "generate update for deduplicated transaction with definite answer set to true" in {
      forAll(
        Table[ValueSwitch[Status]](
          "Error Version",
          v1ErrorSwitch,
          v2ErrorSwitch,
        )
      ) { errorSwitch =>
        val inputEntry = buildOutOfTimeBoundsEntry(
          TimeBounds(deduplicateUntil = Some(aRecordTime)),
          TRANSACTION_REJECTION_ENTRY,
          definiteAnswer = Some(true),
        )
        val actual = outOfTimeBoundsEntryToUpdate(aRecordTime, inputEntry, errorSwitch)
        inside(actual) { case Some(CommandRejected(_, _, FinalReason(status))) =>
          status.code shouldBe Code.ALREADY_EXISTS.value
          status.details shouldBe Seq(
            AnyProto.pack[ErrorInfo](
              ErrorInfo(metadata = Map(GrpcStatuses.DefiniteAnswerKey -> "true"))
            )
          )
        }
      }
    }

    "generate a rejection entry for a transaction if record time is out of time bounds" in {
      def verifyCommandRejection(actual: Option[Update]): Unit = actual match {
        case Some(Update.CommandRejected(recordTime, completionInfo, FinalReason(status))) =>
          recordTime shouldBe aRecordTime
          completionInfo shouldBe Conversions.parseCompletionInfo(
            parseInstant(recordTime),
            someSubmitterInfo,
          )
          completionInfo.submissionId shouldBe Some(someSubmitterInfo.getSubmissionId)
          status.details shouldBe Seq(
            AnyProto.pack[ErrorInfo](
              ErrorInfo(metadata = Map(GrpcStatuses.DefiniteAnswerKey -> "false"))
            )
          )
          ()
        case _ => fail()
      }
      def verifyStatusCode(actual: Option[Update], code: Code): Unit = actual match {
        case Some(Update.CommandRejected(_, _, FinalReason(status))) =>
          status.code shouldBe code.value
          ()
        case _ => fail()
      }
      val testCases = Table(
        ("Error Version", "Time Bounds", "Record Time", "Log Entry Type", "Assertions"),
        (
          v1ErrorSwitch,
          TimeBounds(
            tooLateFrom = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1)))
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = { update =>
            verifyCommandRejection(update)
            verifyStatusCode(update, Code.ABORTED)
          }),
        ),
        (
          v1ErrorSwitch,
          TimeBounds(
            tooEarlyUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.plusMillis(1)))
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = { update =>
            verifyCommandRejection(update)
            verifyStatusCode(update, Code.ABORTED)
          }),
        ),
        (
          v1ErrorSwitch,
          TimeBounds(tooLateFrom = Some(aRecordTime)),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          v1ErrorSwitch,
          TimeBounds(tooEarlyUntil = Some(aRecordTime)),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          v1ErrorSwitch,
          TimeBounds(
            tooEarlyUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1))),
            tooLateFrom = Some(Timestamp.assertFromInstant(aRecordTimeInstant.plusMillis(1))),
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          v2ErrorSwitch,
          TimeBounds(
            tooLateFrom = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1)))
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = { update =>
            verifyCommandRejection(update)
            verifyStatusCode(update, Code.FAILED_PRECONDITION)
          }),
        ),
        (
          v2ErrorSwitch,
          TimeBounds(
            tooEarlyUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.plusMillis(1)))
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = { update =>
            verifyCommandRejection(update)
            verifyStatusCode(update, Code.FAILED_PRECONDITION)
          }),
        ),
        (
          v2ErrorSwitch,
          TimeBounds(tooLateFrom = Some(aRecordTime)),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          v2ErrorSwitch,
          TimeBounds(tooEarlyUntil = Some(aRecordTime)),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        // Record time within time bounds.
        (
          v2ErrorSwitch,
          TimeBounds(
            tooEarlyUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1))),
            tooLateFrom = Some(Timestamp.assertFromInstant(aRecordTimeInstant.plusMillis(1))),
          ),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
      )
      runAll(testCases)
    }

    "generate a rejection entry for a configuration if record time is out of time bounds" in {
      def verifyConfigurationRejection(actual: Option[Update]): Unit = actual match {
        case Some(
              Update.ConfigurationChangeRejected(
                recordTime,
                submissionId,
                participantId,
                proposedConfiguration,
                rejectionReason,
              )
            ) =>
          recordTime shouldBe aRecordTime
          submissionId shouldBe aConfigurationRejectionEntry.getSubmissionId
          participantId shouldBe aConfigurationRejectionEntry.getParticipantId
          proposedConfiguration shouldBe Configuration
            .decode(aConfigurationRejectionEntry.getConfiguration)
            .getOrElse(fail())
          rejectionReason should include("Configuration change timed out")
          ()
        case _ => fail()
      }
      val testCases = Table(
        ("Time Bounds", "Record Time", "Log Entry Type", "Assertions"),
        (
          TimeBounds(
            tooLateFrom = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1)))
          ),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyConfigurationRejection),
        ),
        (
          TimeBounds(
            tooEarlyUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.plusMillis(1)))
          ),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyConfigurationRejection),
        ),
        (
          TimeBounds(tooLateFrom = Some(aRecordTime)),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(tooEarlyUntil = Some(aRecordTime)),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        // Record time within time bounds.
        (
          TimeBounds(
            tooEarlyUntil = Some(Timestamp.assertFromInstant(aRecordTimeInstant.minusMillis(1))),
            tooLateFrom = Some(Timestamp.assertFromInstant(aRecordTimeInstant.plusMillis(1))),
          ),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
      )
      runAll(testCases)
    }

    "not generate an update for rejected entries" in {
      val testCases = Table(
        ("Time Bounds", "Record Time", "Log Entry Type", "Assertions"),
        (
          TimeBounds(),
          aRecordTime,
          TRANSACTION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(),
          aRecordTime,
          PACKAGE_UPLOAD_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(),
          aRecordTime,
          CONFIGURATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
        (
          TimeBounds(),
          aRecordTime,
          PARTY_ALLOCATION_REJECTION_ENTRY,
          Assertions(verify = verifyNoUpdateIsGenerated),
        ),
      )
      runAll(testCases)
    }

    "throw in case a normal log entry is seen" in {
      val testCases = Table(
        ("Time Bounds", "Record Time", "Log Entry Type", "Assertions"),
        (TimeBounds(), aRecordTime, TRANSACTION_ENTRY, Assertions(throwsInternalError = true)),
        (TimeBounds(), aRecordTime, PACKAGE_UPLOAD_ENTRY, Assertions(throwsInternalError = true)),
        (TimeBounds(), aRecordTime, CONFIGURATION_ENTRY, Assertions(throwsInternalError = true)),
        (TimeBounds(), aRecordTime, PARTY_ALLOCATION_ENTRY, Assertions(throwsInternalError = true)),
        (
          TimeBounds(),
          aRecordTime,
          OUT_OF_TIME_BOUNDS_ENTRY,
          Assertions(throwsInternalError = true),
        ),
      )
      runAll(testCases)
    }
  }

  private def runAll(
      table: TableFor5[ValueSwitch[
        Status
      ], TimeBounds, Timestamp, DamlLogEntry.PayloadCase, Assertions]
  ): Unit =
    forAll(table) {
      (
          errorVersionSwitch: ValueSwitch[Status],
          timeBounds: TimeBounds,
          recordTime: Timestamp,
          logEntryType: DamlLogEntry.PayloadCase,
          assertions: Assertions,
      ) =>
        val inputEntry = buildOutOfTimeBoundsEntry(timeBounds, logEntryType)
        if (assertions.throwsInternalError) {
          assertThrows[Err.InternalError](
            outOfTimeBoundsEntryToUpdate(recordTime, inputEntry, errorVersionSwitch)
          )
        } else {
          val actual = outOfTimeBoundsEntryToUpdate(recordTime, inputEntry, errorVersionSwitch)
          assertions.verify(actual)
          ()
        }
    }

  private def runAll(
      table: TableFor4[TimeBounds, Timestamp, DamlLogEntry.PayloadCase, Assertions]
  ): Unit = {
    val (head1, head2, head3, head4) = table.heading
    runAll(
      Table(
        heading = ("Error Version", head1, head2, head3, head4),
        rows = table.flatMap {
          case (
                timeBounds: TimeBounds,
                recordTime: Timestamp,
                logEntryType: DamlLogEntry.PayloadCase,
                assertions: Assertions,
              ) =>
            Seq(
              (v1ErrorSwitch, timeBounds, recordTime, logEntryType, assertions),
              (v2ErrorSwitch, timeBounds, recordTime, logEntryType, assertions),
            )
        }: _*,
      )
    )
  }

  private def buildOutOfTimeBoundsEntry(
      timeBounds: TimeBounds,
      logEntryType: DamlLogEntry.PayloadCase,
      definiteAnswer: Option[Boolean] = None,
  ): DamlOutOfTimeBoundsEntry = {
    val builder = DamlOutOfTimeBoundsEntry.newBuilder
    timeBounds.tooEarlyUntil.foreach(value => builder.setTooEarlyUntil(buildTimestamp(value)))
    timeBounds.tooLateFrom.foreach(value => builder.setTooLateFrom(buildTimestamp(value)))
    timeBounds.deduplicateUntil.foreach(value => builder.setDuplicateUntil(buildTimestamp(value)))
    builder.setEntry(buildLogEntry(logEntryType, definiteAnswer))
    builder.build
  }

  private def someSubmitterInfo: DamlSubmitterInfo =
    DamlSubmitterInfo.newBuilder
      .addSubmitters("a submitter")
      .setApplicationId("test")
      .setCommandId("a command ID")
      .setSubmissionId("submission id")
      .build

  private def aTransactionRejectionEntry(
      maybeDefiniteAnswer: Option[Boolean]
  ): DamlTransactionRejectionEntry = {
    val builder = DamlTransactionRejectionEntry.newBuilder.setSubmitterInfo(someSubmitterInfo)
    maybeDefiniteAnswer.foreach(builder.setDefiniteAnswer)
    builder.build
  }

  private def aConfigurationRejectionEntry: DamlConfigurationRejectionEntry =
    DamlConfigurationRejectionEntry.newBuilder
      .setConfiguration(Configuration.encode(LedgerReader.DefaultConfiguration))
      .setSubmissionId("a submission")
      .setParticipantId("a participant")
      .build

  private def buildLogEntry(
      payloadCase: DamlLogEntry.PayloadCase,
      definiteAnswer: Option[Boolean],
  ): DamlLogEntry = {
    val builder = DamlLogEntry.newBuilder
    payloadCase match {
      case TRANSACTION_ENTRY =>
        builder.setTransactionEntry(DamlTransactionEntry.getDefaultInstance)
      case TRANSACTION_REJECTION_ENTRY =>
        builder.setTransactionRejectionEntry(aTransactionRejectionEntry(definiteAnswer))
      case PACKAGE_UPLOAD_ENTRY =>
        builder.setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
      case PACKAGE_UPLOAD_REJECTION_ENTRY =>
        builder.setPackageUploadRejectionEntry(DamlPackageUploadRejectionEntry.getDefaultInstance)
      case CONFIGURATION_ENTRY =>
        builder.setConfigurationEntry(DamlConfigurationEntry.getDefaultInstance)
      case CONFIGURATION_REJECTION_ENTRY =>
        builder.setConfigurationRejectionEntry(aConfigurationRejectionEntry)
      case PARTY_ALLOCATION_ENTRY =>
        builder.setPartyAllocationEntry(DamlPartyAllocationEntry.getDefaultInstance)
      case PARTY_ALLOCATION_REJECTION_ENTRY =>
        builder.setPartyAllocationRejectionEntry(
          DamlPartyAllocationRejectionEntry.getDefaultInstance
        )
      case OUT_OF_TIME_BOUNDS_ENTRY =>
        builder.setOutOfTimeBoundsEntry(DamlOutOfTimeBoundsEntry.getDefaultInstance)
      case TIME_UPDATE_ENTRY =>
        builder.setTimeUpdateEntry(Empty.getDefaultInstance)
      case PAYLOAD_NOT_SET =>
        ()
    }
    builder.build
  }

  private lazy val v1ErrorSwitch = new ValueSwitch[Status](enableSelfServiceErrorCodes = false) {
    override def toString: String = "1"
  }
  private lazy val v2ErrorSwitch = new ValueSwitch[Status](enableSelfServiceErrorCodes = true) {
    override def toString: String = "2"
  }
}
