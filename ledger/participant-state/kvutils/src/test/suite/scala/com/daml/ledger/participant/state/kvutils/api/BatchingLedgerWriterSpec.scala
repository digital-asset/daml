// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class BatchingLedgerWriterSpec
    extends AsyncWordSpec
    with MockitoSugar
    with AkkaBeforeAndAfterAll
    with Eventually
    with Matchers {

  "DefaultBatchingQueue" should {

    "report dead when queue is closed" in {
      val queue = DefaultBatchingQueue(10, 5, 1.millis, 1)
        .run { batch =>
          throw new RuntimeException("kill the queue")
        }
      val subm1 = DamlSubmissionBatch.CorrelatedSubmission.newBuilder
        .setCorrelationId("1")
        .setSubmission(ByteString.copyFromUtf8("helloworld"))
        .build

      queue.alive should be(true)
      for {
        res <- queue.offer(subm1)
      } yield {
        res should be(SubmissionResult.Acknowledged)
        queue.alive should be(false)
      }
    }

    "not commit empty batches" in {
      val mockCommit =
        mock[Function[Seq[DamlSubmissionBatch.CorrelatedSubmission], Future[Unit]]]
      val queue = DefaultBatchingQueue(1, 1024, 1.millis, 1)
      queue.run(mockCommit)
      Thread.sleep(5.millis.toMillis);
      verifyZeroInteractions(mockCommit)
      succeed
    }

    // Test that we commit a batch when we have a submission and the maxWaitDuration has been reached.
    "commit batch after maxWaitDuration" in {
      val maxWait = 5.millis
      var batches = Seq.empty[Seq[DamlSubmissionBatch.CorrelatedSubmission]]
      val queue =
        DefaultBatchingQueue(10, 1024, maxWait, 1)
          .run { batch =>
            {
              batches = batch +: batches
              Future.successful(())
            }
          }

      val subm1 = DamlSubmissionBatch.CorrelatedSubmission.newBuilder.setCorrelationId("1").build
      val subm2 = DamlSubmissionBatch.CorrelatedSubmission.newBuilder.setCorrelationId("2").build

      for {
        res1 <- queue.offer(subm1)
        _ = eventually {
          batches.size should be(1)
        }
        res2 <- queue.offer(subm2)
        _ <- eventually {
          batches.size should be(2)
        }
      } yield {
        res1 should be(SubmissionResult.Acknowledged)
        res2 should be(SubmissionResult.Acknowledged)
        batches should be(Seq(Seq(subm2), Seq(subm1)))
        queue.alive should be(true)
      }
    }

    "commit batch after max batch size exceeded" in {
      var batches = Seq.empty[Seq[DamlSubmissionBatch.CorrelatedSubmission]]
      val queue =
        DefaultBatchingQueue(10, 15, 50.millis, 1)
          .run { batch =>
            {
              batches = batch +: batches
              Future.successful(())
            }
          }

      val subm1 = DamlSubmissionBatch.CorrelatedSubmission.newBuilder
        .setCorrelationId("1")
        .setSubmission(ByteString.copyFromUtf8("helloworld"))
        .build
      val subm2 = DamlSubmissionBatch.CorrelatedSubmission.newBuilder
        .setCorrelationId("2")
        .setSubmission(ByteString.copyFromUtf8("helloworld"))
        .build

      for {
        res1 <- queue.offer(subm1)
        _ = {
          batches.size should be(0)
        }
        res2 <- queue.offer(subm2)
      } yield {
        // First batch emitted.
        batches.size should be(1)

        // Wait for second batch.
        eventually {
          batches.size should be(2)
        }

        res1 should be(SubmissionResult.Acknowledged)
        res2 should be(SubmissionResult.Acknowledged)
        batches should be(Seq(Seq(subm2), Seq(subm1)))
        queue.alive should be(true)
      }
    }
  }

  def immediateBatchingQueue: BatchingQueue =
    new BatchingQueue {
      override def run(commitBatch: Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit])(
          implicit materializer: Materializer): BatchingQueueHandle =
        new BatchingQueueHandle {
          override def alive: Boolean = true
          override def offer(
              submission: DamlSubmissionBatch.CorrelatedSubmission): Future[SubmissionResult] =
            commitBatch(Seq(submission))
              .map { _ =>
                SubmissionResult.Acknowledged
              }
        }
    }

  "BatchingLedgerWriter" should {

    "construct batch correctly" in {
      val batchCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val mockWriter = createWriter(Some(batchCaptor))
      val batchingWriter =
        LoggingContext.newLoggingContext { implicit logCtx =>
          new BatchingLedgerWriter(immediateBatchingQueue, mockWriter)
        }
      val (corId1, subm1) = "test1" -> Array[Byte](1, 2, 3)
      val expected = createExpectedBatch(corId1, subm1)
      for {
        res1 <- batchingWriter.commit(corId1, subm1)
      } yield {
        verify(mockWriter).commit(anyString(), ArgumentMatchers.eq(expected))
        res1 should be(SubmissionResult.Acknowledged)
      }
    }

    "drop batch when commit fails" in {
      val mockWriter = createWriter(None, SubmissionResult.Overloaded)
      val batchingWriter = createBatchingWriter(mockWriter, 5, 1.millis)
      for {
        res1 <- batchingWriter.commit("test1", Array[Byte](1, 2, 3, 4, 5))
        res2 <- batchingWriter.commit("test2", Array[Byte](6, 7, 8, 9, 10))
        res3 <- batchingWriter.commit("test3", Array[Byte](11, 12, 13, 14, 15))
      } yield {
        verify(mockWriter, times(3))
          .commit(anyString(), any[Array[Byte]])
        all(Seq(res1, res2, res3)) should be(SubmissionResult.Acknowledged)
        batchingWriter.currentHealth should be(HealthStatus.healthy)
      }
    }
  }

  private def createWriter(
      captor: Option[ArgumentCaptor[Array[Byte]]] = None,
      result: SubmissionResult = SubmissionResult.Acknowledged): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(writer.commit(anyString(), captor.map(_.capture()).getOrElse(any[Array[Byte]]())))
      .thenReturn(Future.successful(SubmissionResult.Acknowledged))
    when(writer.participantId).thenReturn(v1.ParticipantId.assertFromString("test-participant"))
    writer
  }

  private def createBatchingWriter(
      writer: LedgerWriter,
      maxBatchSizeBytes: Long,
      maxWaitDuration: FiniteDuration) =
    LoggingContext.newLoggingContext { implicit logCtx =>
      new BatchingLedgerWriter(
        DefaultBatchingQueue(
          maxQueueSize = 128,
          maxBatchSizeBytes = maxBatchSizeBytes,
          maxWaitDuration = maxWaitDuration,
          maxConcurrentCommits = 1
        ),
        writer = writer,
      )
    }

  private def createExpectedBatch(corId: String, subm: Array[Byte]) =
    Envelope
      .enclose(
        DamlSubmissionBatch.newBuilder
          .addSubmissions(
            DamlSubmissionBatch.CorrelatedSubmission.newBuilder
              .setCorrelationId(corId)
              .setSubmission(ByteString.copyFrom(subm)))
          .build)
      .toByteArray
}
