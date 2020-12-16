// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import java.time.Clock

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch.CorrelatedSubmission
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.export.{
  NoOpLedgerDataExporter,
  SubmissionAggregator
}
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.ArgumentMatchers.{anyExecutionContext, seqOf}
import com.daml.ledger.validator.TestHelper.{aParticipantId, anInvalidEnvelope, makePartySubmission}
import com.daml.ledger.validator.batch.BatchedSubmissionValidatorSpec._
import com.daml.ledger.validator.reading.DamlLedgerStateReader
import com.daml.ledger.validator.{CommitStrategy, ValidationFailed}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.JavaConverters._
import scala.concurrent.Future

class BatchedSubmissionValidatorSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AkkaBeforeAndAfterAll
    with MockitoSugar
    with ArgumentMatchersSugar {

  private val engine = Engine.DevEngine()
  private val metrics = new Metrics(new MetricRegistry)

  private def newBatchedSubmissionValidator[CommitResult](
      params: BatchedSubmissionValidatorParameters,
      metrics: Metrics = this.metrics,
  ): BatchedSubmissionValidator[CommitResult] =
    new BatchedSubmissionValidator[CommitResult](
      params,
      new KeyValueCommitting(engine, metrics),
      new ConflictDetection(metrics),
      metrics,
      NoOpLedgerDataExporter,
    )

  "validateAndCommit" should {

    "return validation failure for invalid envelope" in {
      val validator = newBatchedSubmissionValidator[Unit](
        BatchedSubmissionValidatorParameters.reasonableDefault,
      )

      validator
        .validateAndCommit(
          anInvalidEnvelope,
          aCorrelationId,
          newRecordTime().toInstant,
          aParticipantId,
          mock[DamlLedgerStateReader],
          mock[CommitStrategy[Unit]]
        )
        .failed
        .map { result =>
          result shouldBe a[ValidationFailed]
        }
    }

    "return validation failure for invalid message type in envelope" in {
      val validator = newBatchedSubmissionValidator[Unit](
        BatchedSubmissionValidatorParameters.reasonableDefault,
      )
      val notASubmission = Envelope.enclose(DamlStateValue.getDefaultInstance)

      validator
        .validateAndCommit(
          notASubmission,
          aCorrelationId,
          newRecordTime().toInstant,
          aParticipantId,
          mock[DamlLedgerStateReader],
          mock[CommitStrategy[Unit]]
        )
        .failed
        .map { result =>
          result shouldBe a[ValidationFailed]
        }
    }

    "return validation failure for invalid envelope in batch" in {
      val validator = newBatchedSubmissionValidator[Unit](
        BatchedSubmissionValidatorParameters.reasonableDefault,
      )
      val batchSubmission = DamlSubmissionBatch.newBuilder
        .addSubmissions(
          CorrelatedSubmission.newBuilder
            .setCorrelationId(aCorrelationId)
            .setSubmission(anInvalidEnvelope))
        .build

      validator
        .validateAndCommit(
          Envelope.enclose(batchSubmission),
          aCorrelationId,
          newRecordTime().toInstant,
          aParticipantId,
          mock[DamlLedgerStateReader],
          mock[CommitStrategy[Unit]]
        )
        .failed
        .map { result =>
          result shouldBe a[ValidationFailed]
        }
    }

    "validate a non-batched submission" in {
      val mockLedgerStateReader = mock[DamlLedgerStateReader]
      val mockCommit = mock[CommitStrategy[Unit]]
      val partySubmission = makePartySubmission("foo")
      // Expect two keys, i.e., to retrieve the party and submission dedup values.
      when(mockLedgerStateReader.read(seqOf(size = 2))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(None, None)))
      val logEntryCaptor = ArgumentCaptor.forClass(classOf[DamlLogEntry])
      val outputStateCaptor = ArgumentCaptor.forClass(classOf[Map[DamlStateKey, DamlStateValue]])
      when(
        mockCommit.commit(
          any[ParticipantId],
          any[String],
          any[DamlLogEntryId],
          logEntryCaptor.capture(),
          any[Map[DamlStateKey, Option[DamlStateValue]]],
          outputStateCaptor.capture(),
          any[Option[SubmissionAggregator.WriteSetBuilder]],
        ))
        .thenReturn(Future.unit)
      val validator = newBatchedSubmissionValidator[Unit](
        BatchedSubmissionValidatorParameters.reasonableDefault,
      )

      validator
        .validateAndCommit(
          Envelope.enclose(partySubmission),
          aCorrelationId,
          newRecordTime().toInstant,
          aParticipantId,
          mockLedgerStateReader,
          mockCommit
        )
        .map { _ =>
          // Verify that the log entry is committed.
          logEntryCaptor.getAllValues should have size 1
          val logEntry = logEntryCaptor.getValue.asInstanceOf[DamlLogEntry]
          logEntry.getPartyAllocationEntry should be(partySubmission.getPartyAllocationEntry)

          // Verify that output state contains the expected values.
          outputStateCaptor.getAllValues should have size 1
          val outputState =
            outputStateCaptor.getValue.asInstanceOf[Map[DamlStateKey, DamlStateValue]]
          outputState should have size 2
          outputState.keySet should be(partySubmission.getInputDamlStateList.asScala.toSet)
        }
    }

    "validate and commit a batch" in {
      val (submissions, _, batchSubmissionBytes) = createBatchSubmissionOf(1000)
      val mockLedgerStateReader = mock[DamlLedgerStateReader]
      // Expect two keys, i.e., to retrieve the party and submission dedup values.
      when(mockLedgerStateReader.read(seqOf(size = 2))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(None, None)))
      val logEntryCaptor = ArgumentCaptor.forClass(classOf[DamlLogEntry])
      val outputStateCaptor = ArgumentCaptor.forClass(classOf[Map[DamlStateKey, DamlStateValue]])
      val mockCommit = mock[CommitStrategy[Unit]]
      when(
        mockCommit.commit(
          any[ParticipantId],
          any[String],
          any[DamlLogEntryId],
          logEntryCaptor.capture(),
          any[Map[DamlStateKey, Option[DamlStateValue]]],
          outputStateCaptor.capture(),
          any[Option[SubmissionAggregator.WriteSetBuilder]],
        ))
        .thenReturn(Future.unit)
      val validator =
        newBatchedSubmissionValidator[Unit](BatchedSubmissionValidatorParameters.reasonableDefault)

      validator
        .validateAndCommit(
          batchSubmissionBytes,
          "batch-correlationId",
          newRecordTime().toInstant,
          aParticipantId,
          mockLedgerStateReader,
          mockCommit
        )
        .map { _ =>
          // We expected two state fetches and two commits.
          verify(mockLedgerStateReader, times(1000))
            .read(any[Seq[DamlStateKey]])(anyExecutionContext)
          verify(mockCommit, times(1000)).commit(
            any[ParticipantId],
            any[String],
            any[DamlLogEntryId],
            any[DamlLogEntry],
            any[DamlInputState],
            any[DamlOutputState],
            any[Option[SubmissionAggregator.WriteSetBuilder]],
          )
          // Verify that the log entries have been committed in the right order.
          val logEntries = logEntryCaptor.getAllValues.asScala.map(_.asInstanceOf[DamlLogEntry])
          logEntries.map(_.getPartyAllocationEntry) should be(
            submissions.map(_.getPartyAllocationEntry))
          // Verify that output state contains all the expected values.
          val outputState = outputStateCaptor.getAllValues.asScala
            .map(_.asInstanceOf[Map[DamlStateKey, DamlStateValue]])
            .fold(Map.empty) { case (a, b) => a ++ b }
          outputState should have size (2 * 1000.toLong) // party + submission dedup for each
          outputState.keySet should be(submissions.flatMap(_.getInputDamlStateList.asScala).toSet)
        }
    }

    "not commit the duplicate submission" in {
      val submission = makePartySubmission("duplicate-test")
      val batchSubmission = DamlSubmissionBatch.newBuilder
        .addSubmissions(
          CorrelatedSubmission.newBuilder
            .setCorrelationId(aCorrelationId)
            .setSubmission(Envelope.enclose(submission)))
        .addSubmissions(CorrelatedSubmission.newBuilder
          .setCorrelationId("anotherCorrelationId")
          .setSubmission(Envelope.enclose(submission)))
        .build()
      val mockLedgerStateReader = mock[DamlLedgerStateReader]
      // Expect two keys, i.e., to retrieve the party and submission dedup values.
      when(mockLedgerStateReader.read(seqOf(size = 2))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(None, None)))
      val mockCommit = mock[CommitStrategy[Unit]]
      when(
        mockCommit.commit(
          any[ParticipantId],
          any[String],
          any[DamlLogEntryId],
          any[DamlLogEntry],
          any[Map[DamlStateKey, Option[DamlStateValue]]],
          any[Map[DamlStateKey, DamlStateValue]],
          any[Option[SubmissionAggregator.WriteSetBuilder]],
        ))
        .thenReturn(Future.unit)
      val validator = newBatchedSubmissionValidator[Unit](
        BatchedSubmissionValidatorParameters.reasonableDefault,
      )

      validator
        .validateAndCommit(
          Envelope.enclose(batchSubmission),
          "batch-correlationId",
          newRecordTime().toInstant,
          aParticipantId,
          mockLedgerStateReader,
          mockCommit
        )
        .map { _ =>
          // We must have 1 commit only (for the first submission).
          verify(mockCommit, times(1)).commit(
            any[ParticipantId],
            any[String],
            any[DamlLogEntryId],
            any[DamlLogEntry],
            any[DamlInputState],
            any[DamlOutputState],
            any[Option[SubmissionAggregator.WriteSetBuilder]],
          )
          succeed
        }
    }

    "collect size/count metrics for a batch" in {
      val metrics = new Metrics(new MetricRegistry)
      val validatorMetrics = metrics.daml.kvutils.submission.validator
      val (submissions, batchSubmission, batchSubmissionBytes) = createBatchSubmissionOf(2)
      val mockLedgerStateReader = mock[DamlLedgerStateReader]
      // Expect two keys, i.e., to retrieve the party and submission dedup values.
      when(mockLedgerStateReader.read(seqOf(size = 2))(anyExecutionContext))
        .thenReturn(Future.successful(Seq(None, None)))
      val mockCommit = mock[CommitStrategy[Unit]]
      when(
        mockCommit.commit(
          any[ParticipantId],
          any[String],
          any[DamlLogEntryId],
          any[DamlLogEntry],
          any[Map[DamlStateKey, Option[DamlStateValue]]],
          any[Map[DamlStateKey, DamlStateValue]],
          any[Option[SubmissionAggregator.WriteSetBuilder]],
        ))
        .thenReturn(Future.unit)
      val validator = newBatchedSubmissionValidator[Unit](
        BatchedSubmissionValidatorParameters.reasonableDefault,
        metrics = metrics,
      )

      validator
        .validateAndCommit(
          batchSubmissionBytes,
          "batch-correlationId",
          newRecordTime().toInstant,
          aParticipantId,
          mockLedgerStateReader,
          mockCommit
        )
        .map { _ =>
          validatorMetrics.batchSizes.getSnapshot.getValues should equal(Array(2))
          val Array(actualBatchSubmissionSize) =
            validatorMetrics.receivedBatchSubmissionBytes.getSnapshot.getValues
          actualBatchSubmissionSize should equal(batchSubmission.getSerializedSize)
          val expectedSubmissionSizes = submissions.map(_.getSerializedSize)
          validatorMetrics.receivedSubmissionBytes.getSnapshot.getValues.toSet should contain allElementsOf
            expectedSubmissionSizes
        }
    }
  }
}

object BatchedSubmissionValidatorSpec {

  type DamlInputState = Map[DamlStateKey, Option[DamlStateValue]]
  type DamlOutputState = Map[DamlStateKey, DamlStateValue]

  private lazy val aCorrelationId: String = "aCorrelationId"

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def createBatchSubmissionOf(
      nSubmissions: Int): (Seq[DamlSubmission], DamlSubmissionBatch, ByteString) = {
    val submissions = (1 to nSubmissions).map { n =>
      makePartySubmission(s"party-$n")
    }
    val batchBuilder =
      DamlSubmissionBatch.newBuilder
    submissions.zipWithIndex.foreach {
      case (submission, index) =>
        batchBuilder
          .addSubmissionsBuilder()
          .setCorrelationId(s"test-correlationId-$index")
          .setSubmission(Envelope.enclose(submission))
    }
    val batchSubmission = batchBuilder.build
    (submissions, batchSubmission, Envelope.enclose(batchSubmission))
  }

}
