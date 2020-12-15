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
  DamlStateMapWithFingerprints,
  Envelope,
  Fingerprint,
  FingerprintPlaceholder,
  KeyValueCommitting,
  TestHelpers
}
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.ledger.validator.TestHelper._
import com.daml.ledger.validator.ValidationFailed.ValidationError
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidator.DamlLedgerStateReaderWithFingerprints
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidatorSpec._
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
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
      val expectedReadSet = Map(
        allDamlStateKeyTypes.head -> FingerprintPlaceholder
      )
      val actualInputState = Map(
        allDamlStateKeyTypes.head ->
          (Some(DamlStateValue.getDefaultInstance) -> FingerprintPlaceholder)
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
        expectedInvolvedParticipants = expectedInvolvedParticipants,
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
        allDamlStateKeyTypes.map(key => key -> key.toByteString).toMap
      val actualInputState =
        allDamlStateKeyTypes
          .map(key => key -> ((Some(DamlStateValue.getDefaultInstance), key.toByteString)))
          .toMap
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val ledgerStateReader = createLedgerStateReader(actualInputState)

      instance
        .validate(anEnvelope(expectedReadSet.keySet), aParticipantId, ledgerStateReader)
        .map(verifyReadSet(_, expectedReadSet))
    }

    "return a read set when the contract keys are inconsistent" in {
      val contractKeyStateKey = makeContractKeyStateKey("id")
      val contractKeyFingerprint = fingerprint("contract key")

      // At the time of pre-execution, the key points to contract A.
      val contractIdAStateKey = makeContractIdStateKey("contract ID A")
      val contractIdAStateValue = makeContractIdStateValue()
      val contractIdAFingerprint = fingerprint("contract ID A")

      // However, at the time of validation, it points to contract B.
      val contractKeyBStateValue = makeContractKeyStateValue("contract ID B")
      val contractIdBStateKey = makeContractIdStateKey("contract ID B")
      val contractIdBStateValue = makeContractIdStateValue()
      val contractIdBFingerprint = fingerprint("contract ID B")

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
}

object PreExecutingSubmissionValidatorSpec {

  import ArgumentMatchersSugar._
  import MockitoSugar._

  private val recordTime = Timestamp.now()

  private val metrics = new Metrics(new MetricRegistry)

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
  ): PreExecutingSubmissionValidator[ReadSet, Bytes] = {
    val mockCommitter = mock[KeyValueCommitting]
    val result = PreExecutionResult(
      expectedReadSet.keySet,
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
      (Option[DamlStateValue], Fingerprint),
      ReadSet,
      Bytes,
    ]]
    when(
      mockCommitStrategy.generateReadSet(any[DamlStateMapWithFingerprints], any[Set[DamlStateKey]]))
      .thenAnswer[DamlStateMapWithFingerprints, Set[DamlStateKey]] {
        (fetchedInputs, accessedKeys) =>
          accessedKeys.map { key =>
            val (_, fingerprint) = fetchedInputs(key)
            key.toByteString -> fingerprint
          }.toSeq
      }
    when(
      mockCommitStrategy.generateWriteSets(
        eqTo(aParticipantId),
        any[DamlLogEntryId],
        any[Map[DamlStateKey, (Option[DamlStateValue], Fingerprint)]],
        same(result),
      )(any[ExecutionContext]))
      .thenReturn(
        Future.successful(
          PreExecutionCommitResult[Bytes](
            expectedSuccessWriteSet,
            expectedOutOfTimeBoundsWriteSet,
            expectedInvolvedParticipants)))

    new PreExecutingSubmissionValidator[ReadSet, Bytes](mockCommitter, metrics, mockCommitStrategy)
  }

  private def createLedgerStateReader(
      inputState: Map[DamlStateKey, (Option[DamlStateValue], Fingerprint)]
  ): DamlLedgerStateReaderWithFingerprints =
    new DamlLedgerStateReaderWithFingerprints {
      override def read(keys: Seq[DamlStateKey])(implicit executionContext: ExecutionContext)
        : Future[Seq[(Option[DamlStateValue], Fingerprint)]] =
        Future.successful(keys.map(inputState))
    }

  private def verifyReadSet(
      output: PreExecutionOutput[ReadSet, Bytes],
      expectedReadSet: Map[DamlStateKey, Fingerprint],
  ): Assertion = {
    import org.scalatest.matchers.should.Matchers._
    val actualReadSet = output.readSet.map {
      case (key, value) =>
        DamlStateKey.parseFrom(key) -> value
    }.toMap
    actualReadSet should be(expectedReadSet)
  }
}
