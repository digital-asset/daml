// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Fingerprint}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlPartyAllocationEntry,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.StepInfo
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import org.mockito.Mockito._

class CommitterSpec extends WordSpec with Matchers with MockitoSugar {
  private val aRecordTime = Timestamp(100)
  private val aDamlSubmission = DamlSubmission.getDefaultInstance
  private val aParticipantId = Ref.ParticipantId.assertFromString("a participant")

  "preexecute" should {
    "set pre-execution results from context" in {
      val expectedMinRecordTime = Instant.ofEpochSecond(100)
      val expectedMaxRecordTime = Instant.ofEpochSecond(200)
      val mockContext = mock[CommitContext]
      val expectedOutputs =
        Map(DamlStateKey.getDefaultInstance -> DamlStateValue.getDefaultInstance)
      when(mockContext.getOutputs).thenReturn(expectedOutputs)
      when(mockContext.minimumRecordTime).thenReturn(Some(expectedMinRecordTime))
      when(mockContext.maximumRecordTime).thenReturn(Some(expectedMaxRecordTime))
      val expectedReadSet = Set(
        DamlStateKey.newBuilder.setContractId("1").build -> ByteString.copyFromUtf8("fp1"),
        DamlStateKey.newBuilder.setContractId("2").build -> ByteString.copyFromUtf8("fp2")
      )
      when(mockContext.getAccessedInputKeysWithFingerprints).thenReturn(expectedReadSet)
      val instance = createCommitter()

      val actual = instance.preexecute(aDamlSubmission, aParticipantId, Map.empty, mockContext)

      verify(mockContext, times(1)).getOutputs
      verify(mockContext, times(1)).getAccessedInputKeysWithFingerprints
      actual.readSet shouldBe expectedReadSet.toMap
      actual.successfulLogEntry shouldBe aLogEntry
      actual.stateUpdates shouldBe expectedOutputs
      actual.minimumRecordTime shouldBe Timestamp.assertFromInstant(expectedMinRecordTime)
      actual.maximumRecordTime shouldBe Timestamp.assertFromInstant(expectedMaxRecordTime)
      actual.involvedParticipants shouldBe Committer.AllParticipants
    }

    "set min/max record time to default values in case they are not available from context" in {
      val mockContext = mock[CommitContext]
      when(mockContext.minimumRecordTime).thenReturn(None)
      when(mockContext.maximumRecordTime).thenReturn(None)
      when(mockContext.getOutputs).thenReturn(Map.empty)
      when(mockContext.getAccessedInputKeysWithFingerprints)
        .thenReturn(Set.empty[(DamlStateKey, Fingerprint)])
      val instance = createCommitter()

      val actual = instance.preexecute(aDamlSubmission, aParticipantId, Map.empty, mockContext)

      actual.minimumRecordTime shouldBe Timestamp.MinValue
      actual.maximumRecordTime shouldBe Timestamp.MaxValue
    }

    "construct out-of-time-bounds log entry" in {
      val mockContext = mock[CommitContext]
      when(mockContext.getOutputs).thenReturn(Iterable.empty)
      when(mockContext.getAccessedInputKeysWithFingerprints)
        .thenReturn(Set.empty[(DamlStateKey, Fingerprint)])
      when(mockContext.minimumRecordTime).thenReturn(None)
      when(mockContext.maximumRecordTime).thenReturn(None)
      val instance = createCommitter()
      val expectedOutOfTimeBoundsLogEntry = DamlLogEntry.newBuilder.build

      val actual = instance.preexecute(aDamlSubmission, aParticipantId, Map.empty, mockContext)

      actual.outOfTimeBoundsLogEntry shouldBe expectedOutOfTimeBoundsLogEntry
    }
  }

  "buildLogEntryWithOptionalRecordTime" should {
    "set record time in log entry if record time is available" in {
      val actualLogEntry =
        Committer.buildLogEntryWithOptionalRecordTime(recordTime = Some(aRecordTime), identity)

      actualLogEntry.hasRecordTime shouldBe true
      actualLogEntry.getRecordTime shouldBe buildTimestamp(aRecordTime)
    }

    "skip setting record time in log entry when it is not available" in {
      val actualLogEntry =
        Committer.buildLogEntryWithOptionalRecordTime(recordTime = None, identity)

      actualLogEntry.hasRecordTime shouldBe false
    }
  }

  "runSteps" should {
    "stop at first StepStop" in {
      val expectedLogEntry = aLogEntry
      val instance = new Committer[Int] {
        override protected def steps: Iterable[(StepInfo, Step)] = Iterable[(StepInfo, Step)](
          ("first", (_, _) => StepContinue(1)),
          ("second", (_, _) => StepStop(expectedLogEntry)),
          ("third", (_, _) => StepStop(DamlLogEntry.getDefaultInstance))
        )

        override protected val committerName: String = "test"

        override protected def init(ctx: CommitContext, submission: DamlSubmission): Int = 0

        override protected val metrics: Metrics = newMetrics()
      }

      instance.runSteps(mock[CommitContext], aDamlSubmission) shouldBe expectedLogEntry
    }

    "throw in case there was no StepStop yielded" in {
      val instance = new Committer[Int] {
        override protected def steps: Iterable[(StepInfo, Step)] = Iterable(
          ("first", (_, _) => StepContinue(1)),
          ("second", (_, _) => StepContinue(2))
        )

        override protected val committerName: String = "test"

        override protected def init(ctx: CommitContext, submission: DamlSubmission): Int = 0

        override protected val metrics: Metrics = newMetrics()
      }

      assertThrows[RuntimeException](instance.runSteps(mock[CommitContext], aDamlSubmission))
    }
  }

  private val aLogEntry = DamlLogEntry.newBuilder
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder
        .setParticipantId("a participant")
        .setParty("a party")
    )
    .build

  private def newMetrics() = new Metrics(new MetricRegistry)

  private def createCommitter(): Committer[Int] = new Committer[Int] {
    override protected val committerName: String = "test"

    override protected def steps: Iterable[(StepInfo, Step)] =
      Iterable(("result", (_, _) => StepStop[Int](aLogEntry)))

    override protected def init(ctx: CommitContext, submission: DamlKvutils.DamlSubmission): Int = 0

    override protected val metrics: Metrics = newMetrics()
  }
}
