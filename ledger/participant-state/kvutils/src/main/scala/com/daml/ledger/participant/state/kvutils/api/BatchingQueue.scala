// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.concurrent.atomic.AtomicReference

import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmissionBatch
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import scala.concurrent.Future
import scala.concurrent.duration._

object BatchingQueue {
  type CommitBatchFunction =
    Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit]
}

/** Trait for instantiating a batching queue. */
trait BatchingQueue {

  /** Instantiate a running batching queue for enqueueing submissions into a batch.
    * The batch is committed using the provided callback `commitBatch`.
    */
  def run(commitBatch: BatchingQueue.CommitBatchFunction)(implicit
      materializer: Materializer
  ): RunningBatchingQueueHandle
}

/** Handle for a running batching queue. */
trait RunningBatchingQueueHandle {

  /** Returns `true` if the queue is alive, and `false` if it is in any other state. */
  def isAlive: Boolean = state == RunningBatchingQueueState.Alive

  /** Returns the current state of the queue. */
  def state: RunningBatchingQueueState

  /** Enqueue a submission into a batch. */
  def offer(submission: DamlSubmissionBatch.CorrelatedSubmission): Future[SubmissionResult]

  /** Stop accepting new submissions. The future will complete when all submissions are handled. */
  def stop(): Future[Unit]
}

sealed trait RunningBatchingQueueState

object RunningBatchingQueueState {

  /** The queue is alive and can handle new submissions. */
  object Alive extends RunningBatchingQueueState

  /** The queue has been closed, but is still handling submissions. */
  object Closing extends RunningBatchingQueueState

  /** The queue has finished handling submissions. */
  object Complete extends RunningBatchingQueueState

  /** The queue encountered an exception when handling a submission. */
  object Failed extends RunningBatchingQueueState

}

/** A default batching queue implementation for the batching ledger writer backed by an
  * akka-streams [[Source.queue]].
  *
  * @param maxQueueSize         The maximum number of submissions to queue for batching. On overflow new submissions are dropped.
  * @param maxBatchSizeBytes    Maximum size for the batch. A batch is emitted if adding a submission would exceed this limit.
  * @param maxWaitDuration      The maximum time to wait before a batch is forcefully emitted.
  * @param maxConcurrentCommits The maximum number of concurrent calls to make to [[LedgerWriter.commit]].
  */
case class DefaultBatchingQueue(
    maxQueueSize: Int,
    maxBatchSizeBytes: Long,
    maxWaitDuration: FiniteDuration,
    maxConcurrentCommits: Int,
) extends BatchingQueue {
  private val queue: Source[Seq[DamlSubmissionBatch.CorrelatedSubmission], SourceQueueWithComplete[
    DamlSubmissionBatch.CorrelatedSubmission
  ]] =
    Source
      .queue(maxQueueSize, OverflowStrategy.dropNew)
      .groupedWeightedWithin(maxBatchSizeBytes, maxWaitDuration)(
        (cs: DamlSubmissionBatch.CorrelatedSubmission) => cs.getSubmission.size.toLong
      )

  def run(
      commitBatch: Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit]
  )(implicit materializer: Materializer): RunningBatchingQueueHandle = {
    val materializedQueue = queue
      .mapAsync(maxConcurrentCommits)(commitBatch)
      .to(Sink.ignore)
      .run()

    val queueState = new AtomicReference[RunningBatchingQueueState](RunningBatchingQueueState.Alive)
    materializedQueue
      .watchCompletion()
      .onComplete { _ =>
        queueState.compareAndSet(RunningBatchingQueueState.Alive, RunningBatchingQueueState.Failed)
      }(materializer.executionContext)

    new RunningBatchingQueueHandle {
      override def state: RunningBatchingQueueState = queueState.get()

      override def offer(
          submission: DamlSubmissionBatch.CorrelatedSubmission
      ): Future[SubmissionResult] = {
        materializedQueue
          .offer(submission)
          .map {
            case QueueOfferResult.Enqueued => SubmissionResult.Acknowledged
            case QueueOfferResult.Dropped =>
              SubmissionResult.SynchronousError(
                Status(
                  Code.RESOURCE_EXHAUSTED.value
                )
              )
            case f: QueueOfferResult.Failure =>
              SubmissionResult.SynchronousError(
                Status(
                  Code.INTERNAL.value,
                  f.toString,
                )
              )
            case QueueOfferResult.QueueClosed =>
              SubmissionResult.SynchronousError(
                Status(
                  Code.INTERNAL.value,
                  "DefaultBatchingQueue.queue is closed",
                )
              )
          }(materializer.executionContext)
      }

      override def stop(): Future[Unit] = {
        if (
          queueState.compareAndSet(
            RunningBatchingQueueState.Alive,
            RunningBatchingQueueState.Closing,
          )
        ) {
          materializedQueue.complete()
        }
        materializedQueue
          .watchCompletion()
          .map { _ =>
            queueState.compareAndSet(
              RunningBatchingQueueState.Closing,
              RunningBatchingQueueState.Complete,
            )
            ()
          }(DirectExecutionContext)
      }
    }
  }
}
