// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer.StepInfo
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class CommitterSpec
    extends WordSpec
    with TableDrivenPropertyChecks
    with Matchers
    with MockitoSugar {
  "preExecute" should {
    "set pre-execution results from context" in {
      val mockContext = mock[CommitContext]
      val expectedOutputs =
        Map(DamlStateKey.getDefaultInstance -> DamlStateValue.getDefaultInstance)
      when(mockContext.getOutputs).thenReturn(expectedOutputs)
      val expectedMinRecordTime = Instant.ofEpochSecond(100)
      val expectedMaxRecordTime = Instant.ofEpochSecond(200)
      when(mockContext.minimumRecordTime).thenReturn(Some(expectedMinRecordTime))
      when(mockContext.maximumRecordTime).thenReturn(Some(expectedMaxRecordTime))
      when(mockContext.deduplicateUntil).thenReturn(None)
      when(mockContext.outOfTimeBoundsLogEntry).thenReturn(Some(aRejectionLogEntry))
      val expectedReadSet = Set(
        DamlStateKey.newBuilder.setContractId("1").build,
        DamlStateKey.newBuilder.setContractId("2").build
      )
      when(mockContext.getAccessedInputKeys).thenReturn(expectedReadSet)
      val instance = createCommitter()

      val actual = instance.preExecute(aDamlSubmission, mockContext)

      verify(mockContext, times(1)).getOutputs
      verify(mockContext, times(1)).getAccessedInputKeys
      actual.readSet shouldBe expectedReadSet
      actual.successfulLogEntry shouldBe aLogEntry
      actual.stateUpdates shouldBe expectedOutputs
      actual.minimumRecordTime shouldBe Some(Timestamp.assertFromInstant(expectedMinRecordTime))
      actual.maximumRecordTime shouldBe Some(Timestamp.assertFromInstant(expectedMaxRecordTime))
    }

    "set min/max record time to None in case they are not available from context" in {
      val mockContext = mock[CommitContext]
      when(mockContext.minimumRecordTime).thenReturn(None)
      when(mockContext.maximumRecordTime).thenReturn(None)
      when(mockContext.deduplicateUntil).thenReturn(None)
      when(mockContext.outOfTimeBoundsLogEntry).thenReturn(Some(aRejectionLogEntry))
      when(mockContext.getOutputs).thenReturn(Map.empty)
      when(mockContext.getAccessedInputKeys).thenReturn(Set.empty[DamlStateKey])
      val instance = createCommitter()

      val actual = instance.preExecute(aDamlSubmission, mockContext)

      actual.minimumRecordTime should not be defined
      actual.maximumRecordTime should not be defined
    }

    "construct out-of-time-bounds log entry" in {
      val mockContext = mock[CommitContext]
      when(mockContext.outOfTimeBoundsLogEntry).thenReturn(Some(aRejectionLogEntry))
      when(mockContext.getOutputs).thenReturn(Iterable.empty)
      when(mockContext.getAccessedInputKeys).thenReturn(Set.empty[DamlStateKey])
      val expectedMinRecordTime = Instant.ofEpochSecond(100)
      val expectedMaxRecordTime = Instant.ofEpochSecond(200)
      val expectedDuplicateUntil = Instant.ofEpochSecond(99)
      when(mockContext.minimumRecordTime).thenReturn(Some(expectedMinRecordTime))
      when(mockContext.maximumRecordTime).thenReturn(Some(expectedMaxRecordTime))
      when(mockContext.deduplicateUntil).thenReturn(Some(expectedDuplicateUntil))
      val instance = createCommitter()
      val actual = instance.preExecute(aDamlSubmission, mockContext)

      actual.outOfTimeBoundsLogEntry.hasOutOfTimeBoundsEntry shouldBe true
      val actualOutOfTimeBoundsLogEntry = actual.outOfTimeBoundsLogEntry.getOutOfTimeBoundsEntry
      actualOutOfTimeBoundsLogEntry.getTooEarlyUntil shouldBe buildTimestamp(expectedMinRecordTime)
      actualOutOfTimeBoundsLogEntry.getTooLateFrom shouldBe buildTimestamp(expectedMaxRecordTime)
      actualOutOfTimeBoundsLogEntry.getDuplicateUntil shouldBe buildTimestamp(
        expectedDuplicateUntil)
      actualOutOfTimeBoundsLogEntry.hasEntry shouldBe true
      actualOutOfTimeBoundsLogEntry.getEntry shouldBe aRejectionLogEntry
    }

    "do not throw in case neither out-of-time-bounds log entry nor min/max record time are set" in {
      val mockContext = mock[CommitContext]
      when(mockContext.outOfTimeBoundsLogEntry).thenReturn(None)
      when(mockContext.getOutputs).thenReturn(Iterable.empty)
      when(mockContext.getAccessedInputKeys).thenReturn(Set.empty[DamlStateKey])
      when(mockContext.minimumRecordTime).thenReturn(None)
      when(mockContext.maximumRecordTime).thenReturn(None)
      val instance = createCommitter()

      instance.preExecute(aDamlSubmission, mockContext)
      succeed
    }

    "throw in case out-of-time-bounds log entry is not set but min/max record time is" in {
      val anInstant = Instant.ofEpochSecond(1234)
      val combinations = Table(
        "min/max record time",
        Some(anInstant) -> Some(anInstant),
        Some(anInstant) -> None,
        None -> Some(anInstant)
      )

      forAll(combinations) {
        case (minRecordTimeMaybe, maxRecordTimeMaybe) =>
          val mockContext = mock[CommitContext]
          when(mockContext.outOfTimeBoundsLogEntry).thenReturn(None)
          when(mockContext.getOutputs).thenReturn(Iterable.empty)
          when(mockContext.getAccessedInputKeys).thenReturn(Set.empty[DamlStateKey])
          when(mockContext.minimumRecordTime).thenReturn(minRecordTimeMaybe)
          when(mockContext.maximumRecordTime).thenReturn(maxRecordTimeMaybe)
          val instance = createCommitter()

          assertThrows[IllegalArgumentException](instance.preExecute(aDamlSubmission, mockContext))
      }
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

  private val aRecordTime = Timestamp(100)
  private val aDamlSubmission = DamlSubmission.getDefaultInstance
  private val aParticipantId = Ref.ParticipantId.assertFromString("a participant")
  private val aLogEntry = DamlLogEntry.newBuilder
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder
        .setParticipantId("a participant")
        .setParty("a party")
    )
    .build
  private val aRejectionLogEntry = DamlLogEntry.newBuilder
    .setPackageUploadRejectionEntry(
      DamlPackageUploadRejectionEntry.newBuilder
        .setSubmissionId("an ID")
        .setParticipantId("a participant"))
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
