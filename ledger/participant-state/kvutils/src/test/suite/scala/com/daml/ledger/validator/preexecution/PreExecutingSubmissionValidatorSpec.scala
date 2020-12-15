// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch.CorrelatedSubmission
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.{
  Bytes,
  DamlStateMap,
  Envelope,
  KeyValueCommitting,
  TestHelpers
}
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.ledger.validator.HasDamlStateValue
import com.daml.ledger.validator.TestHelper._
import com.daml.ledger.validator.ValidationFailed.ValidationError
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidatorSpec._
import com.daml.ledger.validator.reading.StateReader
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class PreExecutingSubmissionValidatorSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "validate" should {
    "generate correct output in case of success" in {
      val expectedReadSet = Set(allDamlStateKeyTypes.head)
      val actualInputState = Map(
        allDamlStateKeyTypes.head -> Some(DamlStateValue.getDefaultInstance)
      )
      val expectedMinRecordTime = Some(recordTime.toInstant.minusSeconds(123))
      val expectedMaxRecordTime = Some(recordTime.toInstant)
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

    "return a sorted read set with correct fingerprints" in {
      val expectedReadSet = allDamlStateKeyTypes.toSet
      val actualInputState =
        allDamlStateKeyTypes.map(key => key -> Some(DamlStateValue.getDefaultInstance)).toMap
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(expectedReadSet), aParticipantId, ledgerStateReader)
        .map(verifyReadSet(_, expectedReadSet))
    }

    "return a read set when the contract keys are inconsistent" in {
      val contractKeyStateKey = makeContractKeyStateKey("id")

      // At the time of pre-execution, the key points to contract A.
      val contractIdAStateKey = makeContractIdStateKey("contract ID A")
      val contractIdAStateValue = makeContractIdStateValue()

      // However, at the time of validation, it points to contract B.
      val contractKeyBStateValue = makeContractKeyStateValue("contract ID B")
      val contractIdBStateKey = makeContractIdStateKey("contract ID B")
      val contractIdBStateValue = makeContractIdStateValue()

      val preExecutedInputKeys = Set(contractKeyStateKey, contractIdAStateKey)
      val expectedReadSet = Set(contractKeyStateKey, contractIdBStateKey)
      val actualInputState = Map(
        contractKeyStateKey -> Some(contractKeyBStateValue),
        contractIdAStateKey -> Some(contractIdAStateValue),
        contractIdBStateKey -> Some(contractIdBStateValue),
      )
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(preExecutedInputKeys), aParticipantId, ledgerStateReader)
        .map(verifyReadSet(_, expectedReadSet))
    }

    "fail in case a batched submission is input" in {
      val instance = createInstance()
      val aBatchedSubmission = DamlSubmissionBatch.newBuilder
        .addSubmissions(
          CorrelatedSubmission.newBuilder
            .setCorrelationId("correlated submission")
            .setSubmission(anEnvelope()))
        .build()

      instance
        .validate(
          Envelope.enclose(aBatchedSubmission),
          aParticipantId,
          mock[StateReader[DamlStateKey, TestValue]],
        )
        .failed
        .map {
          case ValidationError(actualReason) =>
            actualReason should include("Batched submissions are not supported")
        }
    }

    "fail in case an invalid envelope is input" in {
      val instance = createInstance()

      instance
        .validate(anInvalidEnvelope, aParticipantId, mock[StateReader[DamlStateKey, TestValue]])
        .failed
        .map {
          case ValidationError(actualReason) =>
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
        .map {
          case ValidationError(actualReason) =>
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

  private final case class TestValue(value: Option[DamlStateValue])

  private object TestValue {
    implicit object `TestValue has DamlStateValue` extends HasDamlStateValue[TestValue] {
      override def damlStateValue(value: TestValue): Option[DamlStateValue] = value.value
    }
  }

  private final case class TestReadSet(keys: Set[DamlStateKey])

  private final case class TestWriteSet(value: String)

  private def anEnvelope(expectedReadSet: Set[DamlStateKey] = Set.empty): Bytes = {
    val submission = DamlSubmission
      .newBuilder()
      .setConfigurationSubmission(DamlConfigurationSubmission.getDefaultInstance)
      .addAllInputDamlState(expectedReadSet.asJava)
      .build
    Envelope.enclose(submission)
  }

  private def createInstance(
      expectedReadSet: Set[DamlStateKey] = Set.empty,
      expectedMinRecordTime: Option[Instant] = None,
      expectedMaxRecordTime: Option[Instant] = None,
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
      expectedMinRecordTime.map(Timestamp.assertFromInstant),
      expectedMaxRecordTime.map(Timestamp.assertFromInstant)
    )
    when(
      mockCommitter.preExecuteSubmission(
        any[Configuration],
        any[DamlSubmission],
        any[ParticipantId],
        any[DamlStateMap],
      ))
      .thenReturn(result)

    val mockCommitStrategy = mock[PreExecutingCommitStrategy[
      DamlStateKey,
      TestValue,
      TestReadSet,
      TestWriteSet,
    ]]
    when(
      mockCommitStrategy.generateReadSet(any[Map[DamlStateKey, TestValue]], any[Set[DamlStateKey]]))
      .thenAnswer[Map[DamlStateKey, TestValue], Set[DamlStateKey]] { (_, accessedKeys) =>
        TestReadSet(accessedKeys)
      }
    when(
      mockCommitStrategy.generateWriteSets(
        eqTo(aParticipantId),
        any[DamlLogEntryId],
        any[Map[DamlStateKey, TestValue]],
        same(result),
      )(any[ExecutionContext]))
      .thenReturn(
        Future.successful(
          PreExecutionCommitResult(
            expectedSuccessWriteSet,
            expectedOutOfTimeBoundsWriteSet,
            expectedInvolvedParticipants,
          )))

    new PreExecutingSubmissionValidator(mockCommitter, mockCommitStrategy, metrics)
  }

  private def createLedgerStateReader(
      inputState: Map[DamlStateKey, Option[DamlStateValue]]
  ): StateReader[DamlStateKey, TestValue] = {
    val wrappedInputState = inputState.mapValues(TestValue(_))
    new StateReader[DamlStateKey, TestValue] {
      override def read(
          keys: Iterable[DamlStateKey]
      )(implicit executionContext: ExecutionContext): Future[Seq[TestValue]] =
        Future.successful(keys.view.map(wrappedInputState).toVector)
    }
  }

  private def verifyReadSet(
      output: PreExecutionOutput[TestReadSet, Any],
      expectedReadSet: Set[DamlStateKey],
  ): Assertion = {
    import org.scalatest.matchers.should.Matchers._
    output.readSet.keys should be(expectedReadSet)
  }
}
