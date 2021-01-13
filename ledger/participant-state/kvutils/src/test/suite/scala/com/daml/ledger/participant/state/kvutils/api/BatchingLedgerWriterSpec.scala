// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.stream.Materializer
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.logging.LoggingContext
import com.google.protobuf.ByteString
import org.mockito.captor.{ArgCaptor, Captor}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}

class BatchingLedgerWriterSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Eventually
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  import BatchingLedgerWriterSpec._

  private val someCommitMetadata = mock[CommitMetadata]
  when(someCommitMetadata.estimatedInterpretationCost).thenReturn(Some(123L))

  "BatchingLedgerWriter" should {

    "report unhealthy when queue is dead" in {
      val handle = mock[RunningBatchingQueueHandle]
      when(handle.state).thenReturn(RunningBatchingQueueState.Closing)
      val queue = mock[BatchingQueue]
      when(queue.run(any[BatchingQueue.CommitBatchFunction])(any[Materializer])).thenReturn(handle)
      val writer = mock[LedgerWriter]
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit ctx =>
          new BatchingLedgerWriter(queue, writer)
        }
      batchingWriter.currentHealth() shouldBe HealthStatus.unhealthy
      Future.successful(succeed)
    }

    "construct batch correctly" in {
      val batchCaptor = ArgCaptor[Raw.Value]
      val mockWriter = createMockWriter(captor = Some(batchCaptor))
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit loggingContext =>
          new BatchingLedgerWriter(immediateBatchingQueue(), mockWriter)
        }
      val expectedBatch = createExpectedBatch(aCorrelationId -> aSubmission)
      for {
        submissionResult <- batchingWriter.commit(aCorrelationId, aSubmission, someCommitMetadata)
      } yield {
        verify(mockWriter).commit(
          any[String],
          eqTo(expectedBatch),
          argThat((metadata: CommitMetadata) => metadata.estimatedInterpretationCost.isEmpty),
        )
        submissionResult should be(SubmissionResult.Acknowledged)
      }
    }

    "continue even when commit fails" in {
      val mockWriter =
        createMockWriter(captor = None)
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit loggingContext =>
          new BatchingLedgerWriter(immediateBatchingQueue(), mockWriter)
        }
      for {
        result1 <- batchingWriter.commit("test1", aSubmission, someCommitMetadata)
        result2 <- batchingWriter.commit("test2", aSubmission, someCommitMetadata)
        result3 <- batchingWriter.commit("test3", aSubmission, someCommitMetadata)
      } yield {
        verify(mockWriter, times(3)).commit(any[String], any[Raw.Value], any[CommitMetadata])
        all(Seq(result1, result2, result3)) should be(SubmissionResult.Acknowledged)
        batchingWriter.currentHealth() should be(HealthStatus.healthy)
      }
    }

  }

}

object BatchingLedgerWriterSpec extends MockitoSugar with ArgumentMatchersSugar {
  private val aCorrelationId = "aCorrelationId"
  private val aSubmission = Raw.Value(ByteString.copyFromUtf8("a submission"))

  def immediateBatchingQueue()(implicit executionContext: ExecutionContext): BatchingQueue =
    new BatchingQueue {
      override def run(
          commitBatch: Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit]
      )(implicit materializer: Materializer): RunningBatchingQueueHandle =
        new RunningBatchingQueueHandle {
          override def state: RunningBatchingQueueState = RunningBatchingQueueState.Alive

          override def offer(
              submission: DamlSubmissionBatch.CorrelatedSubmission
          ): Future[SubmissionResult] =
            commitBatch(Seq(submission))
              .map { _ =>
                SubmissionResult.Acknowledged
              }

          override def stop(): Future[Unit] = Future.unit
        }
    }

  private def createMockWriter(captor: Option[Captor[Raw.Value]]): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(writer.commit(any[String], captor.map(_.capture).getOrElse(any), any[CommitMetadata]))
      .thenReturn(Future.successful(SubmissionResult.Acknowledged))
    when(writer.participantId).thenReturn(v1.ParticipantId.assertFromString("test-participant"))
    when(writer.currentHealth()).thenReturn(HealthStatus.healthy)
    writer
  }

  private def createExpectedBatch(correlatedSubmissions: (String, Raw.Value)*): Raw.Value =
    Envelope
      .enclose(
        DamlSubmissionBatch.newBuilder
          .addAllSubmissions(
            correlatedSubmissions.map { case (correlationId, submission) =>
              DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                .setCorrelationId(correlationId)
                .setSubmission(submission.bytes)
                .build
            }.asJava
          )
          .build
      )
}
