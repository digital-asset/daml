// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmissionBatch.CorrelatedSubmission
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.participant.state.v2.SubmissionResult.SynchronousError
import com.google.protobuf.ByteString
import com.google.rpc.code.Code
import org.mockito.ArgumentMatchers.any
import org.mockito.{Mockito, MockitoSugar}
import org.scalatest.Inside.inside
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class BatchingQueueSpec
    extends AsyncWordSpec
    with MockitoSugar
    with AkkaBeforeAndAfterAll
    with Eventually
    with Matchers {

  import BatchingQueueSpec._

  "DefaultBatchingQueue" should {

    "report dead when queue dies" in {
      val correlatedSubmission = createCorrelatedSubmission("1")
      val queue = DefaultBatchingQueue(
        maxQueueSize = 10,
        maxBatchSizeBytes =
          correlatedSubmission.getSerializedSize / 2L, // To force emitting the batch right away.
        maxWaitDuration = 1.millis,
        maxConcurrentCommits = 1,
      ).run { _ =>
        throw new RuntimeException("kill the queue")
      }

      queue.state should be(RunningBatchingQueueState.Alive)
      for {
        res <- queue.offer(correlatedSubmission)
      } yield {
        res should be(SubmissionResult.Acknowledged)
        eventually(Timeout(1.second)) {
          queue.state should be(RunningBatchingQueueState.Failed)
        }
      }
    }

    "report dead and return error when queue is closed" in {
      val correlatedSubmission = createCorrelatedSubmission("1")
      val queue = DefaultBatchingQueue(
        maxQueueSize = 10,
        maxBatchSizeBytes =
          correlatedSubmission.getSerializedSize / 2L, // To force emitting the batch right away.
        maxWaitDuration = 1.millis,
        maxConcurrentCommits = 1,
      ).run { _ =>
        Future.unit
      }
      queue.state should be(RunningBatchingQueueState.Alive)
      val wait = queue.stop()
      wait.map { _ =>
        queue.state should be(RunningBatchingQueueState.Complete)
      }
    }

    "not commit empty batches" in {
      val mockCommit =
        mock[Function[Seq[CorrelatedSubmission], Future[Unit]]]
      val maxWaitDuration = 1.millis
      val queue = DefaultBatchingQueue(
        maxQueueSize = 1,
        maxBatchSizeBytes = 1024L,
        maxWaitDuration = maxWaitDuration,
        maxConcurrentCommits = 1,
      )
      queue.run(mockCommit)
      verify(mockCommit, Mockito.timeout(10 * maxWaitDuration.toMillis).times(0))
        .apply(any[Seq[CorrelatedSubmission]]())
      succeed
    }

    "commit batch after maxWaitDuration" in {
      val maxWait = 5.millis
      val batches = mutable.ListBuffer.empty[Seq[CorrelatedSubmission]]
      val queue =
        DefaultBatchingQueue(
          maxQueueSize = 10,
          maxBatchSizeBytes = 1024,
          maxWaitDuration = maxWait,
          maxConcurrentCommits = 1,
        )
          .run { batch =>
            batches += batch
            Future.unit
          }

      val correlatedSubmission1 =
        CorrelatedSubmission.newBuilder.setCorrelationId("1").build
      val correlatedSubmission2 =
        CorrelatedSubmission.newBuilder.setCorrelationId("2").build

      for {
        res1 <- queue.offer(correlatedSubmission1)
        _ = eventually(Timeout(1.second)) {
          batches.size should be(1)
        }
        res2 <- queue.offer(correlatedSubmission2)
        _ <- eventually(Timeout(1.second)) {
          batches.size should be(2)
        }
      } yield {
        res1 should be(SubmissionResult.Acknowledged)
        res2 should be(SubmissionResult.Acknowledged)
        batches should contain.only(Seq(correlatedSubmission1), Seq(correlatedSubmission2))
        queue.state should be(RunningBatchingQueueState.Alive)
      }
    }

    "return overloaded when queue is overrun" in {
      val correlatedSubmission = createCorrelatedSubmission("1")
      val queue =
        DefaultBatchingQueue(
          maxQueueSize = 1,
          maxBatchSizeBytes = 1L,
          maxWaitDuration = 1.millis,
          maxConcurrentCommits = 1,
        ).run(_ => Future.never)

      for {
        res1 <- queue.offer(correlatedSubmission)
        res2 <- queue.offer(correlatedSubmission)
        res3 <- queue.offer(correlatedSubmission)
        res4 <- queue.offer(correlatedSubmission)
      } yield {
        // First one is sent right away, room in queue for one.
        res1 should be(SubmissionResult.Acknowledged)
        // Second gets passed down without queueing and gets blocked by first one.
        res2 should be(SubmissionResult.Acknowledged)
        // Third one gets queued.
        res3 should be(SubmissionResult.Acknowledged)
        // Fourth will be dropped.
        inside(res4) { case SynchronousError(status) =>
          status.code shouldBe Code.RESOURCE_EXHAUSTED.value
        }
      }
    }

    "commit batch after maxBatchSizeBytes exceeded" in {
      val correlatedSubmission1 = createCorrelatedSubmission("1")
      val correlatedSubmission2 = createCorrelatedSubmission("2")
      val batches = mutable.Buffer.empty[Seq[CorrelatedSubmission]]

      val maxWaitDuration = 500.millis

      // Queue that can fit a single submission plus tiny bit more
      val queue =
        DefaultBatchingQueue(
          maxQueueSize = 10,
          maxBatchSizeBytes = correlatedSubmission1.getSerializedSize + 1L,
          maxWaitDuration = maxWaitDuration,
          maxConcurrentCommits = 1,
        ).run { batch =>
          {
            batches += batch
            Future.unit
          }
        }

      for {
        res1 <- queue.offer(correlatedSubmission1)
        _ = res1 should be(SubmissionResult.Acknowledged)
        // The batch is not yet full, so should not be emitted yet.
        _ = {
          batches.size should be(0)
        }
        res2 <- queue.offer(correlatedSubmission2)
        _ = res2 should be(SubmissionResult.Acknowledged)
      } yield {
        // The batch is now full, so it will be emitted immediately, without the second submission.
        eventually(Timeout(maxWaitDuration / 2)) {
          batches should be(Seq(Seq(correlatedSubmission1)))
        }
        // After the wait timeout, the second batch will be emitted with the second submission.
        eventually(Timeout(maxWaitDuration * 2)) {
          batches should be(Seq(Seq(correlatedSubmission1), Seq(correlatedSubmission2)))
        }

        queue.state should be(RunningBatchingQueueState.Alive)
      }
    }
  }

}

object BatchingQueueSpec {

  def createCorrelatedSubmission(correlationId: String): CorrelatedSubmission =
    CorrelatedSubmission.newBuilder
      .setCorrelationId(correlationId)
      .setSubmission(ByteString.copyFromUtf8("helloworld"))
      .build
}
