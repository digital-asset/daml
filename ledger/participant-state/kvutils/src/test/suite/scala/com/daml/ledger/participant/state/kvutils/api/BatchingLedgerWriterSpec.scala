// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SubmissionResult
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext
import com.google.protobuf.ByteString
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{timeout, times, _}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class BatchingLedgerWriterSpec
    extends AsyncWordSpec
    with MockitoSugar
    with AkkaBeforeAndAfterAll
    with Matchers {

  private def createWriter(
      captor: Option[ArgumentCaptor[Array[Byte]]] = None,
      result: SubmissionResult = SubmissionResult.Acknowledged): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(
      writer.commit(
        ArgumentMatchers.anyString(),
        captor.map(_.capture()).getOrElse(ArgumentMatchers.any[Array[Byte]]())))
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
        writer = writer,
        maxQueueSize = 128,
        maxBatchSizeBytes = maxBatchSizeBytes,
        maxWaitDuration = maxWaitDuration,
        maxParallelism = 1
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

  "BatchingLedgerWriter" should {

    // Test that even when maxWaitDuration is reached we don't commit an empty batch.
    "not commit empty batches" in {
      val mockWriter = mock[LedgerWriter]
      val batchingWriter = createBatchingWriter(mockWriter, 1024, 10.millis)
      Thread.sleep(2 * batchingWriter.maxWaitDuration.toMillis);
      verifyZeroInteractions(mockWriter)
      succeed
    }

    // Test that we commit a batch when we have a submission and the maxWaitDuration has been reached.
    "commit batch after maxWaitDuration" in {
      val batchCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val mockWriter = createWriter(Some(batchCaptor))
      val batchingWriter = createBatchingWriter(
        mockWriter,
        1024,
        /* Fingers crossed that this is long enough to not make this test flaky */
        50.millis)

      val (corId1, subm1) = "test1" -> Array[Byte](1, 2, 3)
      val (corId2, subm2) = "test2" -> Array[Byte](4, 5, 6)

      for {
        res1 <- batchingWriter.commit(corId1, subm1)

        // Wait until the first batch's duration expires
        _ <- Future { Thread.sleep(batchingWriter.maxWaitDuration.toMillis * 2); }

        // Commit another submission, which we expect to land in a separate batch.
        res2 <- batchingWriter.commit(corId2, subm2)

      } yield {
        def verifyCommit(expected: Array[Byte]) = {
          val commitTimeout = batchingWriter.maxWaitDuration.toMillis * 5
          verify(mockWriter, timeout(commitTimeout))
            .commit(ArgumentMatchers.anyString(), ArgumentMatchers.eq(expected))
        }
        verifyCommit(createExpectedBatch(corId1, subm1))
        verifyCommit(createExpectedBatch(corId2, subm2))

        res1 should be(SubmissionResult.Acknowledged)
        res2 should be(SubmissionResult.Acknowledged)
      }
    }

    "commit batch when maxBatchSizeBytes reached" in {
      val batchCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val mockWriter = createWriter(Some(batchCaptor))
      val batchingWriter = createBatchingWriter(mockWriter, 10, 1.seconds)
      val expectedBatch =
        Envelope
          .enclose(
            DamlSubmissionBatch.newBuilder
              .addSubmissions(DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                .setCorrelationId("test1")
                .setSubmission(ByteString.copyFrom(Array[Byte](1, 2, 3, 4, 5))))
              .addSubmissions(DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                .setCorrelationId("test2")
                .setSubmission(ByteString.copyFrom(Array[Byte](6, 7, 8, 9, 10))))
              .build)
          .toByteArray

      for {
        res1 <- batchingWriter.commit("test1", Array[Byte](1, 2, 3, 4, 5))
        res2 <- batchingWriter.commit("test2", Array[Byte](6, 7, 8, 9, 10))
        res3 <- batchingWriter.commit("test3", Array[Byte](11, 12, 13, 14, 15))
      } yield {
        // We're only expecting the first batch to have been emitted.
        verify(mockWriter, times(1))
          .commit(ArgumentMatchers.anyString(), ArgumentMatchers.eq(expectedBatch))

        all(Seq(res1, res2, res3)) should be(SubmissionResult.Acknowledged)
      }
    }
  }

  // Test that the batching writer stays up and keeps going even when underlying writer fails.
  "drop batch when commit fails" in {
    val mockWriter = createWriter(None, SubmissionResult.Overloaded)
    val batchingWriter = createBatchingWriter(mockWriter, 5, 1.millis)
    for {
      res1 <- batchingWriter.commit("test1", Array[Byte](1, 2, 3, 4, 5))
      res2 <- batchingWriter.commit("test2", Array[Byte](6, 7, 8, 9, 10))
      res3 <- batchingWriter.commit("test3", Array[Byte](11, 12, 13, 14, 15))
    } yield {
      verify(mockWriter).commit(ArgumentMatchers.eq("test1"), ArgumentMatchers.any[Array[Byte]])
      verify(mockWriter).commit(ArgumentMatchers.eq("test2"), ArgumentMatchers.any[Array[Byte]])
      verify(mockWriter).commit(ArgumentMatchers.eq("test3"), ArgumentMatchers.any[Array[Byte]])

      all(Seq(res1, res2, res3)) should be(SubmissionResult.Acknowledged)
    }
  }
}
