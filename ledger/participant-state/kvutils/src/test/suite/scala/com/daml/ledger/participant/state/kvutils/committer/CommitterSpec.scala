// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.protobuf.LedgerConfiguration
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.Conversions.{buildTimestamp, configurationStateKey}
import com.daml.ledger.participant.state.kvutils.Err
import com.daml.ledger.participant.state.kvutils.TestHelpers.{createCommitContext, theDefaultConfig}
import com.daml.ledger.participant.state.kvutils.committer.CommitterSpec._
import com.daml.ledger.participant.state.kvutils.store.events.PackageUpload.DamlPackageUploadRejectionEntry
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlConfigurationEntry,
  DamlPartyAllocationEntry,
}
import com.daml.ledger.participant.state.kvutils.store.{DamlLogEntry, DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.logging.entries.LoggingEntries
import com.daml.metrics.Metrics
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@nowarn("msg=deprecated")
class CommitterSpec
    extends AnyWordSpec
    with TableDrivenPropertyChecks
    with Matchers
    with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "preExecute" should {
    "set pre-execution results from context" in {
      val mockContext = mock[CommitContext]
      val expectedOutputs =
        Map(DamlStateKey.getDefaultInstance -> DamlStateValue.getDefaultInstance)
      when(mockContext.getOutputs).thenReturn(expectedOutputs)
      val expectedMinRecordTime = Timestamp.assertFromLong(100 * 1000 * 1000)
      val expectedMaxRecordTime = Timestamp.assertFromLong(200 * 1000 * 1000)
      when(mockContext.minimumRecordTime).thenReturn(Some(expectedMinRecordTime))
      when(mockContext.maximumRecordTime).thenReturn(Some(expectedMaxRecordTime))
      when(mockContext.outOfTimeBoundsLogEntry).thenReturn(Some(aRejectionLogEntry))
      val expectedReadSet = Set(
        DamlStateKey.newBuilder.setContractId("1").build,
        DamlStateKey.newBuilder.setContractId("2").build,
      )
      when(mockContext.getAccessedInputKeys).thenReturn(expectedReadSet)
      val instance = createCommitter()

      val actual = instance.preExecute(aDamlSubmission, mockContext)

      verify(mockContext, times(1)).getOutputs
      verify(mockContext, times(1)).getAccessedInputKeys
      actual.readSet shouldBe expectedReadSet
      actual.successfulLogEntry shouldBe aLogEntry
      actual.stateUpdates shouldBe expectedOutputs
      actual.minimumRecordTime shouldBe Some(expectedMinRecordTime)
      actual.maximumRecordTime shouldBe Some(expectedMaxRecordTime)
    }

    "set min/max record time to None in case they are not available from context" in {
      val mockContext = mock[CommitContext]
      when(mockContext.minimumRecordTime).thenReturn(None)
      when(mockContext.maximumRecordTime).thenReturn(None)
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
      val expectedMinRecordTime = Timestamp.assertFromLong(100 * 1000 * 1000)
      val expectedMaxRecordTime = Timestamp.assertFromLong(200 * 1000 * 1000)
      when(mockContext.minimumRecordTime).thenReturn(Some(expectedMinRecordTime))
      when(mockContext.maximumRecordTime).thenReturn(Some(expectedMaxRecordTime))
      val instance = createCommitter()
      val actual = instance.preExecute(aDamlSubmission, mockContext)

      actual.outOfTimeBoundsLogEntry.hasOutOfTimeBoundsEntry shouldBe true
      val actualOutOfTimeBoundsLogEntry = actual.outOfTimeBoundsLogEntry.getOutOfTimeBoundsEntry
      actualOutOfTimeBoundsLogEntry.getTooEarlyUntil shouldBe buildTimestamp(expectedMinRecordTime)
      actualOutOfTimeBoundsLogEntry.getTooLateFrom shouldBe buildTimestamp(expectedMaxRecordTime)
      actualOutOfTimeBoundsLogEntry.hasDuplicateUntil shouldBe false
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
      val aTimestamp = Timestamp.assertFromLong(1234 * 1000 * 1000)
      val combinations = Table(
        "min/max record time",
        Some(aTimestamp) -> Some(aTimestamp),
        Some(aTimestamp) -> None,
        None -> Some(aTimestamp),
      )

      forAll(combinations) { case (minRecordTimeMaybe, maxRecordTimeMaybe) =>
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

    "run init" in {
      val initialized = new AtomicBoolean(false)
      val mockContext = mock[CommitContext]
      when(mockContext.outOfTimeBoundsLogEntry).thenReturn(None)
      when(mockContext.getOutputs).thenReturn(Iterable.empty)
      when(mockContext.getAccessedInputKeys).thenReturn(Set.empty[DamlStateKey])
      when(mockContext.minimumRecordTime).thenReturn(None)
      when(mockContext.maximumRecordTime).thenReturn(None)
      val committer = new TestCommitter {
        override protected def init(ctx: CommitContext, submission: DamlSubmission)(implicit
            loggingContext: LoggingContext
        ): Int = {
          initialized.set(true)
          super.init(ctx, submission)
        }
      }

      committer.preExecute(aDamlSubmission, mockContext)

      initialized.get() shouldBe true
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

  "run" should {
    "run init" in {
      val initialized = new AtomicBoolean(false)
      val committer = new TestCommitter {
        override protected def init(ctx: CommitContext, submission: DamlSubmission)(implicit
            loggingContext: LoggingContext
        ): Int = {
          initialized.set(true)
          super.init(ctx, submission)
        }
      }

      committer.run(None, aDamlSubmission, Ref.ParticipantId.assertFromString("test"), Map.empty)

      initialized.get() shouldBe true
    }
  }

  "runSteps" should {
    "stop at first StepStop" in {
      val expectedLogEntry = aLogEntry
      val instance = new TestCommitter {
        override protected def steps: Iterable[(StepInfo, Step)] =
          Iterable(
            "first" -> stepReturning(StepContinue(1)),
            "second" -> stepReturning(StepStop(expectedLogEntry)),
            "third" -> stepReturning(StepStop(DamlLogEntry.getDefaultInstance)),
          )
      }

      instance.runSteps(mock[CommitContext], aDamlSubmission) shouldBe expectedLogEntry
    }

    "throw in case there was no StepStop yielded" in {
      val instance = new TestCommitter {
        override protected def steps: Iterable[(StepInfo, Step)] =
          Iterable(
            "first" -> stepReturning(StepContinue(1)),
            "second" -> stepReturning(StepContinue(2)),
          )
      }

      assertThrows[RuntimeException](instance.runSteps(mock[CommitContext], aDamlSubmission))
    }
  }

  "getCurrentConfiguration" should {
    "return configuration in case there is one available on the ledger" in {
      val inputState = Map(configurationStateKey -> Some(aConfigurationStateValue))
      val commitContext = createCommitContext(recordTime = None, inputState)

      val (Some(actualConfigurationEntry), actualConfiguration) =
        Committer.getCurrentConfiguration(theDefaultConfig, commitContext)

      actualConfigurationEntry should be(aConfigurationStateValue.getConfigurationEntry)
      actualConfiguration should be(aConfig)
    }

    "return default configuration in case there is no configuration on the ledger" in {
      val inputState = Map(configurationStateKey -> None)
      val commitContext = createCommitContext(recordTime = None, inputState)

      val (actualConfigurationEntry, actualConfiguration) =
        Committer.getCurrentConfiguration(theDefaultConfig, commitContext)

      actualConfigurationEntry should not be defined
      actualConfiguration should be(theDefaultConfig)
    }

    "throw in case configuration key is not declared in the input" in {
      val commitContext = createCommitContext(recordTime = None, Map.empty)

      assertThrows[Err.MissingInputState] {
        Committer.getCurrentConfiguration(theDefaultConfig, commitContext)
      }
    }

    "return no configuration entry in case decoding of configuration fails" in {
      // Create a configuration with an unsupported generation number.
      val invalidConfigurationEntry = DamlStateValue.newBuilder
        .setConfigurationEntry(
          DamlConfigurationEntry.newBuilder
            .setConfiguration(LedgerConfiguration.newBuilder.setGeneration(123456))
        )
        .build
      val commitContext = createCommitContext(
        recordTime = None,
        Map(configurationStateKey -> Some(invalidConfigurationEntry)),
      )

      val (actualConfigurationEntry, actualConfiguration) =
        Committer.getCurrentConfiguration(theDefaultConfig, commitContext)

      actualConfigurationEntry should not be defined
      actualConfiguration should be(theDefaultConfig)
    }
  }
}

object CommitterSpec {
  private val aRecordTime = Timestamp(100)
  private val aDamlSubmission = DamlSubmission.getDefaultInstance
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
        .setParticipantId("a participant")
    )
    .build
  private val aConfig: Configuration = Configuration(
    generation = 1,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofMinutes(1),
  )

  private def newMetrics() = new Metrics(new MetricRegistry)

  private def createCommitter(): Committer[Int] = new TestCommitter

  class TestCommitter extends Committer[Int] {
    override protected val committerName: String = "test"

    override protected def extraLoggingContext(result: Int): LoggingEntries =
      LoggingEntries.empty

    override protected def init(
        ctx: CommitContext,
        submission: DamlSubmission,
    )(implicit loggingContext: LoggingContext): Int = 0

    override protected def steps: Iterable[(StepInfo, Step)] =
      Iterable(
        "result" -> stepReturning(StepStop(aLogEntry))
      )

    override protected val metrics: Metrics = newMetrics()
  }

  private def aConfigurationStateValue: DamlStateValue =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setConfiguration(Configuration.encode(aConfig))
      )
      .build

  private type Step = CommitStep[Int]

  def stepReturning[PartialResult](result: StepResult[PartialResult]): CommitStep[PartialResult] =
    new CommitStep[PartialResult] {
      override def apply(
          context: CommitContext,
          input: PartialResult,
      )(implicit loggingContext: LoggingContext): StepResult[PartialResult] = result
    }
}
