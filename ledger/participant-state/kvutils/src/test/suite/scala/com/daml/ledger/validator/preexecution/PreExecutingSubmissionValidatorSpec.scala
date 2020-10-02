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
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class PreExecutingSubmissionValidatorSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  "validate" should {
    "generate correct output in case of success" in {
      val expectedReadSet = Map(
        TestHelper.allDamlStateKeyTypes.head -> FingerprintPlaceholder
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
      val ledgerStateReader = createLedgerStateReader(expectedReadSet)

      instance
        .validate(
          anEnvelope(expectedReadSet.keySet),
          aCorrelationId,
          aParticipantId,
          ledgerStateReader)
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
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val ledgerStateReader = createLedgerStateReader(expectedReadSet)

      instance
        .validate(
          anEnvelope(expectedReadSet.keySet),
          aCorrelationId,
          aParticipantId,
          ledgerStateReader)
        .map { actual =>
          val expectedSortedReadSet = expectedReadSet
            .map {
              case (key, fingerprint) =>
                keySerializationStrategy.serializeStateKey(key) -> fingerprint
            }
            .toSeq
            .sortBy(_._1.asReadOnlyByteBuffer())
          actual.readSet shouldBe expectedSortedReadSet
        }
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
          aCorrelationId,
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
        .validate(
          anInvalidEnvelope,
          aCorrelationId,
          aParticipantId,
          mock[DamlLedgerStateReaderWithFingerprints])
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
          aCorrelationId,
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

  import MockitoSugar._

  private val recordTime = Timestamp.now()

  private val metrics = new Metrics(new MetricRegistry)

  private val keySerializationStrategy = StateKeySerializationStrategy.createDefault()

  private val aCorrelationId = "correlation ID"

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
      expectedReadSet: Map[DamlStateKey, Fingerprint],
  ): DamlLedgerStateReaderWithFingerprints =
    (keys: Seq[DamlStateKey]) =>
      Future.successful {
        keys.map {
          case key if expectedReadSet.isDefinedAt(key) =>
            Some(DamlStateValue.getDefaultInstance) -> expectedReadSet(key)
          case _ =>
            None -> FingerprintPlaceholder
        }
    }
}
