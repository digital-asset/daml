// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Duration
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.store.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmissionBatch.CorrelatedSubmission
import com.daml.ledger.participant.state.kvutils.wire.{
  DamlConfigurationSubmission,
  DamlSubmission,
  DamlSubmissionBatch,
}
import com.daml.ledger.participant.state.kvutils.{
  DamlStateMap,
  Envelope,
  KeyValueCommitting,
  Raw,
  TestHelpers,
}
import com.daml.ledger.validator.TestHelper._
import com.daml.ledger.validator.ValidationFailed.ValidationError
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidatorSpec._
import com.daml.ledger.validator.preexecution.PreExecutionTestHelper.{
  TestReadSet,
  TestValue,
  TestWriteSet,
  createLedgerStateReader,
}
import com.daml.ledger.validator.reading.StateReader
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class PreExecutingSubmissionValidatorSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "validate" should {
    "generate correct output in case of success" in {
      val expectedReadSet = Set(allDamlStateKeyTypes.head)
      val actualInputState = Map(
        allDamlStateKeyTypes.head -> Some(DamlStateValue.getDefaultInstance)
      )
      val expectedMinRecordTime = Some(recordTime.subtract(Duration.ofSeconds(123)))
      val expectedMaxRecordTime = Some(recordTime)
      val expectedSuccessWriteSet = TestWriteSet("success")
      val expectedOutOfTimeBoundsWriteSet = TestWriteSet("failure")
      val expectedInvolvedParticipants = Set(TestHelpers.mkParticipantId(0))
      val instance = createInstance(
        expectedReadSet = expectedReadSet,
        expectedMinRecordTime = expectedMinRecordTime,
        expectedMaxRecordTime = expectedMaxRecordTime,
        expectedSuccessWriteSet = expectedSuccessWriteSet,
        expectedOutOfTimeBoundsWriteSet = expectedOutOfTimeBoundsWriteSet,
        expectedInvolvedParticipants = expectedInvolvedParticipants,
      )
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(expectedReadSet), aParticipantId, ledgerStateReader)
        .map { actual =>
          actual.minRecordTime shouldBe expectedMinRecordTime
          actual.maxRecordTime shouldBe expectedMaxRecordTime
          actual.successWriteSet shouldBe expectedSuccessWriteSet
          actual.outOfTimeBoundsWriteSet shouldBe expectedOutOfTimeBoundsWriteSet
          actual.readSet shouldBe TestReadSet(expectedReadSet)
          actual.involvedParticipants shouldBe expectedInvolvedParticipants
        }
    }

    "return a sorted read set" in {
      val expectedReadSet = allDamlStateKeyTypes.toSet
      val actualInputState =
        allDamlStateKeyTypes.map(key => key -> Some(DamlStateValue.getDefaultInstance)).toMap
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(expectedReadSet), aParticipantId, ledgerStateReader)
        .map(verifyReadSet(_, expectedReadSet))
    }

    "fail in case a batched submission is input" in {
      val instance = createInstance()
      val aBatchedSubmission = DamlSubmissionBatch.newBuilder
        .addSubmissions(
          CorrelatedSubmission.newBuilder
            .setCorrelationId("correlated submission")
            .setSubmission(anEnvelope().bytes)
        )
        .build()

      instance
        .validate(
          Envelope.enclose(aBatchedSubmission),
          aParticipantId,
          mock[StateReader[DamlStateKey, TestValue]],
        )
        .failed
        .map { case ValidationError(actualReason) =>
          actualReason should include("Batched submissions are not supported")
        }
    }

    "fail in case an invalid envelope is input" in {
      val instance = createInstance()

      instance
        .validate(anInvalidEnvelope, aParticipantId, mock[StateReader[DamlStateKey, TestValue]])
        .failed
        .map { case ValidationError(actualReason) =>
          actualReason should include("Cannot open envelope")
        }
    }

    "fail in case an unexpected message type is input in the envelope" in {
      val instance = createInstance()
      val anEnvelopedDamlLogEntry = Envelope.enclose(aLogEntry)

      instance
        .validate(
          anEnvelopedDamlLogEntry,
          aParticipantId,
          mock[StateReader[DamlStateKey, TestValue]],
        )
        .failed
        .map { case ValidationError(actualReason) =>
          actualReason should include("Unexpected message in envelope")
        }
    }
  }
}

object PreExecutingSubmissionValidatorSpec {

  import ArgumentMatchersSugar._
  import MockitoSugar._

  private val recordTime = Timestamp.now()

  private val metrics = new Metrics(new MetricRegistry)

  private def anEnvelope(expectedReadSet: Set[DamlStateKey] = Set.empty): Raw.Envelope = {
    val submission = DamlSubmission
      .newBuilder()
      .setConfigurationSubmission(DamlConfigurationSubmission.getDefaultInstance)
      .addAllInputDamlState(expectedReadSet.asJava)
      .build
    Envelope.enclose(submission)
  }

  private def createInstance(
      expectedReadSet: Set[DamlStateKey] = Set.empty,
      expectedMinRecordTime: Option[Timestamp] = None,
      expectedMaxRecordTime: Option[Timestamp] = None,
      expectedSuccessWriteSet: TestWriteSet = TestWriteSet(""),
      expectedOutOfTimeBoundsWriteSet: TestWriteSet = TestWriteSet(""),
      expectedInvolvedParticipants: Set[ParticipantId] = Set.empty,
  ): PreExecutingSubmissionValidator[TestValue, TestReadSet, TestWriteSet] = {
    val mockCommitter = mock[KeyValueCommitting]
    val result = PreExecutionResult(
      expectedReadSet,
      aLogEntry,
      Map.empty,
      aLogEntry,
      expectedMinRecordTime,
      expectedMaxRecordTime,
    )
    when(
      mockCommitter.preExecuteSubmission(
        any[Configuration],
        any[DamlSubmission],
        any[ParticipantId],
        any[DamlStateMap],
      )(any[LoggingContext])
    )
      .thenReturn(result)

    val mockCommitStrategy = mock[PreExecutingCommitStrategy[
      DamlStateKey,
      TestValue,
      TestReadSet,
      TestWriteSet,
    ]]
    when(
      mockCommitStrategy.generateReadSet(any[Map[DamlStateKey, TestValue]], any[Set[DamlStateKey]])
    )
      .thenAnswer[Map[DamlStateKey, TestValue], Set[DamlStateKey]] { (_, accessedKeys) =>
        TestReadSet(accessedKeys)
      }
    when(
      mockCommitStrategy.generateWriteSets(
        eqTo(aParticipantId),
        any[DamlLogEntryId],
        any[Map[DamlStateKey, TestValue]],
        same(result),
      )(any[ExecutionContext])
    )
      .thenReturn(
        Future.successful(
          PreExecutionCommitResult(
            expectedSuccessWriteSet,
            expectedOutOfTimeBoundsWriteSet,
            expectedInvolvedParticipants,
          )
        )
      )

    new PreExecutingSubmissionValidator(
      mockCommitter,
      mockCommitStrategy,
      metrics = metrics,
    )
  }

  private def verifyReadSet(
      output: PreExecutionOutput[TestReadSet, Any],
      expectedReadSet: Set[DamlStateKey],
  ): Assertion = {
    import org.scalatest.matchers.should.Matchers._
    output.readSet.keys should be(expectedReadSet)
  }
}
