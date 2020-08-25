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
import com.daml.ledger.validator.TestHelper.{aParticipantId, anInvalidEnvelope, makePartySubmission}
import com.daml.ledger.validator.{CommitStrategy, DamlLedgerStateReader, ValidationFailed}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertion, AsyncWordSpec, Inside, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class BatchedSubmissionValidatorSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AkkaBeforeAndAfterAll
    with MockitoSugar {

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
      when(mockLedgerStateReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 2)))
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

    "validate a batch" in {
      validateBatchSubmission(nSubmissions = 1000, commitParallelism = 2)
    }

    "validate a batch with 1 committer for each submission" in {
      validateBatchSubmission(nSubmissions = 4, commitParallelism = 4)
    }

    "serially commit a batch in case commitParallelism is set to 1" in {
      val nSubmissions = 100
      val (submissions, _, batchSubmissionBytes) = createBatchSubmissionOf(nSubmissions)
      val mockLedgerStateReader = mock[DamlLedgerStateReader]
      // Expect two keys, i.e., to retrieve the party and submission dedup values.
      when(mockLedgerStateReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 2)))
        .thenReturn(Future.successful(Seq(None, None)))
      val logEntryCaptor = ArgumentCaptor.forClass(classOf[DamlLogEntry])
      val mockCommit = mock[CommitStrategy[Unit]]
      when(
        mockCommit.commit(
          any[ParticipantId],
          any[String],
          any[DamlLogEntryId],
          logEntryCaptor.capture(),
          any[Map[DamlStateKey, Option[DamlStateValue]]],
          any[Map[DamlStateKey, DamlStateValue]],
          any[Option[SubmissionAggregator.WriteSetBuilder]],
        ))
        .thenReturn(Future.unit)
      val validatorConfig =
        BatchedSubmissionValidatorParameters.reasonableDefault.copy(commitParallelism = 1)
      val validator = newBatchedSubmissionValidator[Unit](validatorConfig)

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
          verify(mockCommit, times(nSubmissions)).commit(
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
      when(mockLedgerStateReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 2)))
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
      when(mockLedgerStateReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 2)))
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

  type DamlInputState = Map[DamlStateKey, Option[DamlStateValue]]
  type DamlOutputState = Map[DamlStateKey, DamlStateValue]

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private lazy val aCorrelationId: String = "aCorrelationId"

  private def validateBatchSubmission(
      nSubmissions: Int,
      commitParallelism: Int): Future[Assertion] = {
    val (submissions, _, batchSubmissionBytes) = createBatchSubmissionOf(nSubmissions)
    val mockLedgerStateReader = mock[DamlLedgerStateReader]
    // Expect two keys, i.e., to retrieve the party and submission dedup values.
    when(mockLedgerStateReader.readState(argThat((keys: Seq[DamlStateKey]) => keys.size == 2)))
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
    val validatorConfig =
      BatchedSubmissionValidatorParameters.reasonableDefault.copy(
        commitParallelism = commitParallelism)
    val validator = newBatchedSubmissionValidator[Unit](validatorConfig)

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
        verify(mockLedgerStateReader, times(nSubmissions)).readState(any[Seq[DamlStateKey]]())
        verify(mockCommit, times(nSubmissions)).commit(
          any[ParticipantId],
          any[String],
          any[DamlLogEntryId],
          any[DamlLogEntry],
          any[DamlInputState],
          any[DamlOutputState],
          any[Option[SubmissionAggregator.WriteSetBuilder]],
        )
        // Verify we have all the expected log entries.
        val logEntries = logEntryCaptor.getAllValues.asScala.map(_.asInstanceOf[DamlLogEntry])
        logEntries.map(_.getPartyAllocationEntry) should contain allElementsOf
          submissions.map(_.getPartyAllocationEntry)
        // Verify that output state contains all the expected values.
        val outputState = outputStateCaptor.getAllValues.asScala
          .map(_.asInstanceOf[Map[DamlStateKey, DamlStateValue]])
          .fold(Map.empty) { case (a, b) => a ++ b }
        outputState should have size (2 * nSubmissions.toLong) // party + submission dedup for each
        outputState.keySet should be(submissions.flatMap(_.getInputDamlStateList.asScala).toSet)
      }
  }

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
