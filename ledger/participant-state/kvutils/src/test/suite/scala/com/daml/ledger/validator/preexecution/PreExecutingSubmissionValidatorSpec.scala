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
  Fingerprint,
  FingerprintPlaceholder,
  KeyValueCommitting,
  TestHelpers
}
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.ledger.validator.TestHelper.{aLogEntry, anInvalidEnvelope}
import com.daml.ledger.validator.ValidationFailed.ValidationError
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidatorSpec._
import com.daml.ledger.validator.{StateKeySerializationStrategy, TestHelper}
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.value.ValueOuterClass.Identifier
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class PreExecutingSubmissionValidatorSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "validate" should {
    "generate correct output in case of success" in {
      val expectedReadSet = Map(
        TestHelper.allDamlStateKeyTypes.head -> FingerprintPlaceholder
      )
      val actualInputState = Map(
        TestHelper.allDamlStateKeyTypes.head -> (
          (
            Some(DamlStateValue.getDefaultInstance),
            FingerprintPlaceholder))
      )
      val expectedMinRecordTime = Some(recordTime.toInstant.minusSeconds(123))
      val expectedMaxRecordTime = Some(recordTime.toInstant)
      val expectedSuccessWriteSet = ByteString.copyFromUtf8("success")
      val expectedOutOfTimeBoundsWriteSet = ByteString.copyFromUtf8("failure")
      val expectedInvolvedParticipants = Set(TestHelpers.mkParticipantId(0))
      val instance = createInstance(
        expectedReadSet = expectedReadSet,
        expectedMinRecordTime = expectedMinRecordTime,
        expectedMaxRecordTime = expectedMaxRecordTime,
        expectedSuccessWriteSet = expectedSuccessWriteSet,
        expectedOutOfTimeBoundsWriteSet = expectedOutOfTimeBoundsWriteSet,
        expectedInvolvedParticipants = expectedInvolvedParticipants
      )
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(expectedReadSet.keySet), aParticipantId, ledgerStateReader)
        .map { actual =>
          actual.minRecordTime shouldBe expectedMinRecordTime
          actual.maxRecordTime shouldBe expectedMaxRecordTime
          actual.successWriteSet shouldBe expectedSuccessWriteSet
          actual.outOfTimeBoundsWriteSet shouldBe expectedOutOfTimeBoundsWriteSet
          actual.readSet should have size expectedReadSet.size.toLong
          actual.involvedParticipants shouldBe expectedInvolvedParticipants
        }
    }

    "return a sorted read set with correct fingerprints" in {
      val expectedReadSet =
        TestHelper.allDamlStateKeyTypes.map(key => key -> key.toByteString).toMap
      val actualInputState =
        TestHelper.allDamlStateKeyTypes
          .map(key => key -> ((Some(DamlStateValue.getDefaultInstance), key.toByteString)))
          .toMap
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(expectedReadSet.keySet), aParticipantId, ledgerStateReader)
        .map(verifyReadSet(_, expectedReadSet))
    }

    "return a read set when the contract keys are inconsistent" in {
      val contractKeyStateKey = DamlStateKey.newBuilder
        .setContractKey(
          DamlContractKey.newBuilder.setTemplateId(Identifier.newBuilder.addName("id")))
        .build
      val contractKeyFingerprint = ByteString.copyFromUtf8("contract key fingerprint")

      // At the time of pre-execution, the key points to contract A.
      val contractIdAStateKey =
        DamlStateKey.newBuilder.setContractId("contract ID A").build
      val contractIdAStateValue =
        DamlStateValue.newBuilder.setContractState(DamlContractState.newBuilder).build
      val contractIdAFingerprint = ByteString.copyFromUtf8("contract ID A fingerprint")

      // However, at the time of validation, it points to contract B.
      val contractKeyBStateValue = DamlStateValue.newBuilder
        .setContractKeyState(DamlContractKeyState.newBuilder.setContractId("contract ID B"))
        .build
      val contractIdBStateKey =
        DamlStateKey.newBuilder.setContractId("contract ID B").build
      val contractIdBStateValue =
        DamlStateValue.newBuilder.setContractState(DamlContractState.newBuilder).build
      val contractIdBFingerprint = ByteString.copyFromUtf8("contract ID B fingerprint")

      val preExecutedInputKeys = Set(contractKeyStateKey, contractIdAStateKey)
      val expectedReadSet = Map(
        contractKeyStateKey -> contractKeyFingerprint,
        contractIdBStateKey -> contractIdBFingerprint,
      )
      val actualInputState = Map(
        contractKeyStateKey -> ((Some(contractKeyBStateValue), contractKeyFingerprint)),
        contractIdAStateKey -> ((Some(contractIdAStateValue), contractIdAFingerprint)),
        contractIdBStateKey -> ((Some(contractIdBStateValue), contractIdBFingerprint)),
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
          mock[DamlLedgerStateReaderWithFingerprints])
        .failed
        .map {
          case ValidationError(actualReason) =>
            actualReason should include("Batched submissions are not supported")
        }
    }

    "fail in case an invalid envelope is input" in {
      val instance = createInstance()

      instance
        .validate(anInvalidEnvelope, aParticipantId, mock[DamlLedgerStateReaderWithFingerprints])
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
          mock[DamlLedgerStateReaderWithFingerprints])
        .failed
        .map {
          case ValidationError(actualReason) =>
            actualReason should include("Unexpected message in envelope")
        }
    }
  }

  "generateReadSet" should {
    "generate a read set" in {
      val contractIdStateKey = DamlStateKey.newBuilder.setContractId("a contract ID").build
      val contractIdStateValue =
        DamlStateValue.newBuilder.setContractState(DamlContractState.newBuilder).build
      val fingerprint = ByteString.copyFromUtf8("fingerprint")
      val instance = createInstance()

      instance.generateReadSet(
        fetchedInputs = Map(contractIdStateKey -> ((Some(contractIdStateValue), fingerprint))),
        accessedKeys = Set(contractIdStateKey),
      ) should be(Seq(contractIdStateKey.toByteString -> fingerprint))
    }

    "throw in case an input key is declared in the read set but not fetched as input" in {
      val instance = createInstance()

      assertThrows[IllegalStateException](
        instance
          .generateReadSet(
            fetchedInputs = Map.empty,
            accessedKeys = TestHelper.allDamlStateKeyTypes.toSet
          )
      )
    }
  }
}

object PreExecutingSubmissionValidatorSpec {

  import org.mockito.Mockito.when
  import org.scalatest.mockito.MockitoSugar.mock

  private val recordTime = Timestamp.now()

  private val metrics = new Metrics(new MetricRegistry)

  private val keySerializationStrategy = StateKeySerializationStrategy.createDefault()

  private val aParticipantId = TestHelpers.mkParticipantId(1)

  private def anEnvelope(expectedReadSet: Set[DamlStateKey] = Set.empty): Bytes = {
    val submission = DamlSubmission
      .newBuilder()
      .setConfigurationSubmission(DamlConfigurationSubmission.getDefaultInstance)
      .addAllInputDamlState(expectedReadSet.asJava)
      .build
    Envelope.enclose(submission)
  }

  private def createInstance(
      expectedReadSet: Map[DamlStateKey, Fingerprint] = Map.empty,
      expectedMinRecordTime: Option[Instant] = None,
      expectedMaxRecordTime: Option[Instant] = None,
      expectedSuccessWriteSet: Bytes = ByteString.EMPTY,
      expectedOutOfTimeBoundsWriteSet: Bytes = ByteString.EMPTY,
      expectedInvolvedParticipants: Set[ParticipantId] = Set.empty,
  ): PreExecutingSubmissionValidator[Bytes] = {
    val mockCommitter = mock[KeyValueCommitting]
    when(
      mockCommitter.preExecuteSubmission(
        any[Configuration],
        any[DamlSubmission],
        any[ParticipantId],
        any[DamlStateMap],
      ))
      .thenReturn(
        PreExecutionResult(
          expectedReadSet.keySet,
          aLogEntry,
          Map.empty,
          aLogEntry,
          expectedMinRecordTime.map(Timestamp.assertFromInstant),
          expectedMaxRecordTime.map(Timestamp.assertFromInstant)
        ))
    val mockCommitStrategy = mock[PreExecutingCommitStrategy[Bytes]]
    when(
      mockCommitStrategy.generateWriteSets(
        any[ParticipantId],
        any[DamlLogEntryId],
        any[DamlStateMap],
        any[PreExecutionResult],
      )(any[ExecutionContext]))
      .thenReturn(
        Future.successful(
          PreExecutionCommitResult[Bytes](
            expectedSuccessWriteSet,
            expectedOutOfTimeBoundsWriteSet,
            expectedInvolvedParticipants)))
    new PreExecutingSubmissionValidator[Bytes](
      mockCommitter,
      metrics,
      keySerializationStrategy,
      mockCommitStrategy)
  }

  private def createLedgerStateReader(
      inputState: Map[DamlStateKey, (Option[DamlStateValue], Fingerprint)]
  ): DamlLedgerStateReaderWithFingerprints =
    (keys: Seq[DamlStateKey]) => Future.successful(keys.map(inputState))

  private def verifyReadSet(
      output: PreExecutionOutput[Bytes],
      expectedReadSet: Map[DamlStateKey, Fingerprint],
  ): Assertion = {
    import org.scalatest.Matchers._
    val expectedSortedReadSet = expectedReadSet
      .map {
        case (key, fingerprint) =>
          keySerializationStrategy.serializeStateKey(key) -> fingerprint
      }
      .toSeq
      .sortBy(_._1.asReadOnlyByteBuffer())
    output.readSet shouldBe expectedSortedReadSet
  }
}
