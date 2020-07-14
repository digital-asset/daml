package com.daml.ledger.validator.preexecution

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.{Envelope, _}
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidator.DamlInputStateWithFingerprints
import com.daml.ledger.validator.{StateKeySerializationStrategy, TestHelper}
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import com.google.protobuf.{ByteString, Empty}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}
import TestHelper.aLogEntry
import TestHelper.anInvalidEnvelope
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch.CorrelatedSubmission
import com.daml.ledger.validator.ValidationFailed.ValidationError

import scala.concurrent.Future

class PreExecutingSubmissionValidatorSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  "validate" should {
    "generate correct output in case of success" in {
      val expectedReadSet = Map(
        TestHelper.allDamlStateKeyTypes.head -> FingerprintPlaceholder
      )
      val expectedSuccessWriteSet = ByteString.copyFromUtf8("success")
      val expectedOutOfTimeBoundsWriteSet = ByteString.copyFromUtf8("failure")
      val expectedInvolvedParticipants = Set(TestHelpers.mkParticipantId(0))
      val instance = createInstance(
        expectedReadSet = expectedReadSet,
        expectedSuccessWriteSet = expectedSuccessWriteSet,
        expectedOutOfTimeBoundsWriteSet = expectedOutOfTimeBoundsWriteSet,
        expectedInvolvedParticipants = expectedInvolvedParticipants
      )
      val mockLedgerStateReader = createMockLedgerStateReader()

      instance
        .validate(anEnvelope(), aCorrelationId, aParticipantId, mockLedgerStateReader)
        .map { actual =>
          actual.maxRecordTime shouldBe Some(recordTime.toInstant)
          actual.successWriteSet shouldBe expectedSuccessWriteSet
          actual.outOfTimeBoundsWriteSet shouldBe expectedOutOfTimeBoundsWriteSet
          actual.readSet should have size expectedReadSet.size.toLong
          actual.involvedParticipants shouldBe expectedInvolvedParticipants
        }
    }

    "return a sorted read set" in {
      val expectedReadSet = TestHelper.allDamlStateKeyTypes.map(_ -> FingerprintPlaceholder).toMap
      val instance = createInstance(expectedReadSet = expectedReadSet)
      val mockLedgerStateReader = createMockLedgerStateReader()

      instance.validate(anEnvelope(), aCorrelationId, aParticipantId, mockLedgerStateReader).map {
        actual =>
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
      val mockLedgerStateReader = createMockLedgerStateReader()

      instance
        .validate(
          Envelope.enclose(aBatchedSubmission),
          aCorrelationId,
          aParticipantId,
          mockLedgerStateReader)
        .failed
        .map {
          case ValidationError(actualReason) =>
            actualReason should include("Batched submissions are not supported")
        }
    }

    "fail in case an invalid envelope is input" in {
      val instance = createInstance()
      val mockLedgerStateReader = createMockLedgerStateReader()

      instance
        .validate(anInvalidEnvelope, aCorrelationId, aParticipantId, mockLedgerStateReader)
        .failed
        .map {
          case ValidationError(actualReason) =>
            actualReason should include("Cannot open envelope")
        }
    }
  }

  private val recordTime: Timestamp = Timestamp.now()

  private val metrics = new Metrics(new MetricRegistry)

  private val keySerializationStrategy = StateKeySerializationStrategy.createDefault()

  private val aCorrelationId = "correlation ID"

  private val aParticipantId = TestHelpers.mkParticipantId(1)

  private def anEnvelope(): Bytes = {
    val submission = DamlSubmission
      .newBuilder()
      .setConfigurationSubmission(DamlConfigurationSubmission.getDefaultInstance)
      .addInputDamlState(DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance))
      .build
    Envelope.enclose(submission)
  }

  private def createInstance(
      expectedReadSet: Map[DamlStateKey, Fingerprint] = Map.empty,
      expectedSuccessWriteSet: Bytes = ByteString.EMPTY,
      expectedOutOfTimeBoundsWriteSet: Bytes = ByteString.EMPTY,
      expectedInvolvedParticipants: Set[ParticipantId] = Set.empty)
    : PreExecutingSubmissionValidator[Bytes] = {
    val mockCommitter = mock[KeyValueCommitting]
    when(
      mockCommitter.preExecuteSubmission(
        any[Configuration],
        any[DamlSubmission],
        any[ParticipantId],
        any[DamlInputStateWithFingerprints]))
      .thenReturn(
        PreExecutionResult(
          expectedReadSet,
          aLogEntry,
          Map.empty,
          aLogEntry,
          None,
          Some(recordTime)))
    val mockCommitStrategy = mock[PreExecutingCommitStrategy[Bytes]]
    when(
      mockCommitStrategy.generateWriteSets(
        any[ParticipantId],
        any[DamlLogEntryId],
        any[DamlStateMap],
        any[PreExecutionResult]))
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

  private def createMockLedgerStateReader(): DamlLedgerStateReaderWithFingerprints =
    (keys: Seq[DamlStateKey]) =>
      Future.successful {
        keys.map { _ =>
          (None, FingerprintPlaceholder)
        }
    }
}
