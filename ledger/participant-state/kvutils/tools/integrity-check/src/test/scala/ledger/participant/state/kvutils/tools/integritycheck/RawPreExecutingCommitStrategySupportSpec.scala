// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.time.{Duration, Instant, ZoneOffset, ZonedDateTime}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationSubmission,
  DamlLogEntry,
  DamlPartyAllocationEntry,
  DamlStateKey,
  DamlSubmission,
  DamlSubmissionDedupKey,
}
import com.daml.ledger.participant.state.kvutils.`export`.WriteSet
import com.daml.ledger.participant.state.kvutils.export.SubmissionInfo
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.RawPreExecutingCommitStrategySupportSpec._
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.participant.state.v1
import com.daml.metrics.Metrics
import com.google.protobuf.{Empty, Timestamp}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class RawPreExecutingCommitStrategySupportSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll {
  "support" should {
    "commit, and provide the write set" in {
      val metrics = new Metrics(new MetricRegistry)
      val baseTime = ZonedDateTime.of(2021, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC).toInstant
      val support = new RawPreExecutingCommitStrategySupport(metrics)

      val participantId = v1.ParticipantId.assertFromString("participant")
      val allocateAlice = newPartySubmission(
        recordTime = baseTime.plusSeconds(1),
        participantId = participantId,
        submissionId = "AAA",
        correlationId = "submission-A",
        partyId = "Alice",
        "Alice the Aviator",
      )
      val allocateBob = newPartySubmission(
        recordTime = baseTime.plusSeconds(2),
        participantId = participantId,
        submissionId = "BBB",
        correlationId = "submission-B",
        partyId = "Bob",
        "Bob the Builder",
      )

      for {
        writeSetA <- support.commit(allocateAlice)
        writeSetB <- support.commit(allocateBob)
      } yield {
        extractLogEntry(writeSetA).getPayloadCase should be(
          DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY
        )
        writeSetA.map(_._1) should contain(
          Raw.Key(DamlStateKey.newBuilder.setParty("Alice").build().toByteString)
        )
        extractLogEntry(writeSetB).getPayloadCase should be(
          DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY
        )
        writeSetB.map(_._1) should contain(
          Raw.Key(DamlStateKey.newBuilder.setParty("Bob").build().toByteString)
        )
      }
    }

    "go out of bounds if the MRT is invalid with respect to the submission record time" in {
      val metrics = new Metrics(new MetricRegistry)
      val baseTime = ZonedDateTime.of(2021, 2, 1, 12, 0, 0, 0, ZoneOffset.UTC).toInstant
      val support = new RawPreExecutingCommitStrategySupport(metrics)

      val participantId = v1.ParticipantId.assertFromString("participant")
      val updateConfiguration = newConfigurationSubmission(
        recordTime = baseTime,
        participantId = participantId,
        submissionId = "update-1",
        correlationId = "update-1",
        maximumRecordTime = baseTime.plusSeconds(60),
        configuration = v1.Configuration(
          generation = 1,
          timeModel = v1.TimeModel.reasonableDefault,
          maxDeduplicationTime = Duration.ofMinutes(1),
        ),
      )
      val updateConfigurationWithInvalidMrt = newConfigurationSubmission(
        recordTime = baseTime,
        participantId = participantId,
        submissionId = "update-2",
        correlationId = "update-2",
        maximumRecordTime = baseTime.minusSeconds(60),
        configuration = v1.Configuration(
          generation = 2,
          timeModel = v1.TimeModel.reasonableDefault,
          maxDeduplicationTime = Duration.ofMinutes(1),
        ),
      )

      for {
        writeSet1 <- support.commit(updateConfiguration)
        writeSet2 <- support.commit(updateConfigurationWithInvalidMrt)
      } yield {
        writeSet1.map(_._1) should contain(
          Raw.Key(
            DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance).build().toByteString
          )
        )
        extractLogEntry(writeSet1).getPayloadCase should be(
          DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        )
        extractLogEntry(writeSet2).getPayloadCase should be(
          DamlLogEntry.PayloadCase.OUT_OF_TIME_BOUNDS_ENTRY
        )
      }
    }
  }

  private def extractLogEntry(writeSet: WriteSet): DamlLogEntry =
    writeSet
      .flatMap { case (_, value) =>
        Envelope.openLogEntry(value) match {
          case Left(_) => Seq.empty
          case Right(logEntry) => Seq(logEntry)
        }
      }
      .headOption
      .getOrElse {
        fail("Expected a log entry.")
      }

}

object RawPreExecutingCommitStrategySupportSpec {

  private def newPartySubmission(
      recordTime: Instant,
      participantId: v1.ParticipantId,
      submissionId: String,
      correlationId: String,
      partyId: String,
      displayName: String,
  ): SubmissionInfo = {
    val submissionInfo = SubmissionInfo(
      participantId = participantId,
      correlationId = correlationId,
      submissionEnvelope = Envelope.enclose(
        DamlSubmission.newBuilder
          .addInputDamlState(
            DamlStateKey.newBuilder.setSubmissionDedup(
              DamlSubmissionDedupKey.newBuilder
                .setSubmissionId(submissionId)
                .setParticipantId(participantId)
            )
          )
          .addInputDamlState(DamlStateKey.newBuilder.setParty(partyId))
          .setPartyAllocationEntry(
            DamlPartyAllocationEntry.newBuilder
              .setSubmissionId(submissionId)
              .setParty(partyId)
              .setParticipantId(participantId)
              .setDisplayName(displayName)
          )
          .build()
      ),
      recordTimeInstant = recordTime,
    )
    submissionInfo
  }

  private def newConfigurationSubmission(
      recordTime: Instant,
      participantId: v1.ParticipantId,
      submissionId: String,
      correlationId: String,
      maximumRecordTime: Instant,
      configuration: v1.Configuration,
  ): SubmissionInfo = {
    val submissionInfo = SubmissionInfo(
      participantId = participantId,
      correlationId = correlationId,
      submissionEnvelope = Envelope.enclose(
        DamlSubmission.newBuilder
          .addInputDamlState(
            DamlStateKey.newBuilder.setSubmissionDedup(
              DamlSubmissionDedupKey.newBuilder
                .setSubmissionKind(DamlSubmissionDedupKey.SubmissionKind.CONFIGURATION)
                .setSubmissionId(submissionId)
                .setParticipantId(participantId)
            )
          )
          .addInputDamlState(DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance))
          .setConfigurationSubmission(
            DamlConfigurationSubmission.newBuilder
              .setSubmissionId(submissionId)
              .setParticipantId(participantId)
              .setMaximumRecordTime(toTimestamp(maximumRecordTime))
              .setConfiguration(v1.Configuration.encode(configuration))
          )
          .build()
      ),
      recordTimeInstant = recordTime,
    )
    submissionInfo
  }

  private def toTimestamp(instant: Instant): Timestamp = Timestamp.newBuilder
    .setSeconds(instant.getEpochSecond)
    .setNanos(instant.getNano)
    .build()

}
