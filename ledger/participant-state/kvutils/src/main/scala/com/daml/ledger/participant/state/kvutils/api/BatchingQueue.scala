// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.v1.SubmissionResult

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
  def run(commitBatch: BatchingQueue.CommitBatchFunction)(
      implicit materializer: Materializer): RunningBatchingQueueHandle
}

/** Handle for a running batching queue. */
trait RunningBatchingQueueHandle extends Closeable {

  /** Returns true if the handle is still alive and can handle new submissions.
    * If this returns false the queue is unrecoverable. */
  def alive: Boolean

  /** Enqueue a submission into a batch. */
  def offer(submission: DamlSubmissionBatch.CorrelatedSubmission): Future[SubmissionResult]
}

/** A default batching queue implementation for the batching ledger writer backed by an
  * akka-streams [[Source.queue]].
  *
  * @param maxQueueSize The maximum number of submissions to queue for batching. On overflow new submissions are dropped.
  * @param maxBatchSizeBytes Maximum size for the batch. A batch is emitted if adding a submission would exceed this limit.
  * @param maxWaitDuration The maximum time to wait before a batch is forcefully emitted.
  * @param maxConcurrentCommits The maximum number of concurrent calls to make to [[LedgerWriter.commit]].
  */
case class DefaultBatchingQueue(
    maxQueueSize: Int,
    maxBatchSizeBytes: Long,
    maxWaitDuration: FiniteDuration,
    maxConcurrentCommits: Int
) extends BatchingQueue {
  private val queue: Source[
    Seq[DamlSubmissionBatch.CorrelatedSubmission],
    SourceQueueWithComplete[DamlSubmissionBatch.CorrelatedSubmission]] =
    Source
      .queue(maxQueueSize, OverflowStrategy.dropNew)
      .groupedWeightedWithin(maxBatchSizeBytes, maxWaitDuration)(
        (cs: DamlSubmissionBatch.CorrelatedSubmission) => cs.getSubmission.size.toLong)

  @SuppressWarnings(Array("org.wartremover.warts.Any")) /* Keep.left */
  def run(commitBatch: Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit])(
      implicit materializer: Materializer): RunningBatchingQueueHandle = {
    val materializedQueue = queue
      .mapAsync(maxConcurrentCommits)(commitBatch)
      .to(Sink.ignore)
      .run()

    val queueAlive = new AtomicBoolean(true)
    materializedQueue.watchCompletion.foreach { _ =>
      queueAlive.set(false)
    }(materializer.executionContext)

    new RunningBatchingQueueHandle {
      override def alive: Boolean = queueAlive.get()

      override def offer(
          submission: DamlSubmissionBatch.CorrelatedSubmission): Future[SubmissionResult] = {
        materializedQueue
          .offer(submission)
          .map {
            case QueueOfferResult.Enqueued => SubmissionResult.Acknowledged
            case QueueOfferResult.Dropped => SubmissionResult.Overloaded
            case f: QueueOfferResult.Failure => SubmissionResult.InternalError(f.toString)
            case QueueOfferResult.QueueClosed =>
              SubmissionResult.InternalError("DefaultBatchingQueue.queue is closed")
          }(materializer.executionContext)
      }

      override def close(): Unit = {
        materializedQueue.complete()
      }
    }
  }
}
