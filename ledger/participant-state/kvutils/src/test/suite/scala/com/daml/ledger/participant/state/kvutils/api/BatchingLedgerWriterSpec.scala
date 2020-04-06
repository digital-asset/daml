// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.{Envelope, MockitoHelpers}
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.participant.state.{kvutils, v1}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.logging.LoggingContext
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar._
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class BatchingLedgerWriterSpec
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Eventually
    with Matchers {

  import BatchingLedgerWriterSpec._

  "BatchingLedgerWriter" should {

    "report unhealthy when queue is dead" in {
      val handle = mock[RunningBatchingQueueHandle]
      when(handle.alive).thenReturn(false)
      val queue = mock[BatchingQueue]
      when(queue.run(any[BatchingQueue.CommitBatchFunction]())(any[Materializer]()))
        .thenReturn(handle)
      val writer = mock[LedgerWriter]
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit ctx =>
          new BatchingLedgerWriter(queue, writer)
        }
      batchingWriter.currentHealth shouldBe HealthStatus.unhealthy
      Future.successful(succeed)
    }

    "construct batch correctly" in {
      val batchCaptor = MockitoHelpers.captor[kvutils.Bytes]
      val mockWriter = createMockWriter(captor = Some(batchCaptor))
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit logCtx =>
          new BatchingLedgerWriter(immediateBatchingQueue, mockWriter)
        }
      val expected = createExpectedBatch(aCorrelationId -> aSubmission)
      for {
        submissionResult <- batchingWriter.commit(aCorrelationId, aSubmission)
      } yield {
        verify(mockWriter).commit(anyString(), ArgumentMatchers.eq(expected))
        submissionResult should be(SubmissionResult.Acknowledged)
      }
    }

    "continue even when commit fails" in {
      val mockWriter =
        createMockWriter(captor = None, submissionResult = SubmissionResult.Overloaded)
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit logCtx =>
          new BatchingLedgerWriter(immediateBatchingQueue, mockWriter)
        }
      for {
        result1 <- batchingWriter.commit("test1", aSubmission)
        result2 <- batchingWriter.commit("test2", aSubmission)
        result3 <- batchingWriter.commit("test3", aSubmission)
      } yield {
        verify(mockWriter, times(3))
          .commit(anyString(), any[kvutils.Bytes])
        all(Seq(result1, result2, result3)) should be(SubmissionResult.Acknowledged)
        batchingWriter.currentHealth should be(HealthStatus.healthy)
      }
    }

  }

}

object BatchingLedgerWriterSpec {
  val aCorrelationId = "aCorrelationId"
  val aSubmission = ByteString.copyFromUtf8("a submission")

  def immediateBatchingQueue()(implicit executionContext: ExecutionContext): BatchingQueue =
    new BatchingQueue {
      override def run(commitBatch: Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit])(
          implicit materializer: Materializer): RunningBatchingQueueHandle =
        new RunningBatchingQueueHandle {
          override def alive: Boolean = true
          override def offer(
              submission: DamlSubmissionBatch.CorrelatedSubmission): Future[SubmissionResult] =
            commitBatch(Seq(submission))
              .map { _ =>
                SubmissionResult.Acknowledged
              }

          override def close(): Unit = ()
        }
    }

  private def createMockWriter(
      captor: Option[ArgumentCaptor[kvutils.Bytes]] = None,
      submissionResult: SubmissionResult = SubmissionResult.Acknowledged): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(writer.commit(anyString(), captor.map(_.capture()).getOrElse(any[kvutils.Bytes]())))
      .thenReturn(Future.successful(SubmissionResult.Acknowledged))
    when(writer.participantId).thenReturn(v1.ParticipantId.assertFromString("test-participant"))
    when(writer.currentHealth).thenReturn(HealthStatus.healthy)
    writer
  }

  private def createExpectedBatch(correlatedSubmissions: (String, kvutils.Bytes)*): kvutils.Bytes =
    Envelope
      .enclose(
        DamlSubmissionBatch.newBuilder
          .addAllSubmissions(
            correlatedSubmissions.map {
              case (correlationId, submission) =>
                DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                  .setCorrelationId(correlationId)
                  .setSubmission(submission)
                  .build
            }.asJava
          )
          .build)
}
