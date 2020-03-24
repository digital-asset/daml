// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait BatchingQueue {
  def run(commitBatch: Seq[DamlSubmissionBatch.CorrelatedSubmission] => Future[Unit])(
      implicit materializer: Materializer): BatchingQueueHandle
}

trait BatchingQueueHandle {
  def alive: Boolean
  def offer(submission: DamlSubmissionBatch.CorrelatedSubmission): Future[SubmissionResult]
}

/** Default batching queue implementation for the batching ledger writer.
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
      implicit materializer: Materializer): BatchingQueueHandle = {
    val commitSink: Sink[Seq[DamlSubmissionBatch.CorrelatedSubmission], _] =
      Sink.foreachAsync(maxConcurrentCommits)(commitBatch)

    val materializedQueue = queue
      .log("DefaultBatchingQueue")
      .toMat(commitSink)(Keep.left)
      .run()

    val queueAlive = new AtomicBoolean(true)
    materializedQueue.watchCompletion.foreach { _ =>
      queueAlive.set(false)
    }(materializer.executionContext)

    new BatchingQueueHandle {
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
              SubmissionResult.InternalError("BatchingLedgerWriter.queue is closed")
          }(materializer.executionContext)
      }
    }
  }
}

/** A batching ledger writer that collects submissions into a batch and commits
  * the batch once a set time and byte limit has been reached.
  *
  * @param queue The batching queue implementation
  * @param writer The underlying ledger writer to use to commit the batch
  */
class BatchingLedgerWriter(val queue: BatchingQueue, val writer: LedgerWriter)(
    implicit val materializer: Materializer,
    implicit val logCtx: LoggingContext)
    extends LedgerWriter {

  implicit val executionContext: ExecutionContext = materializer.executionContext
  private val logger = ContextualizedLogger.get(getClass)
  private val queueHandle = queue.run(commitBatch)

  private def commitBatch(
      submissions: Seq[DamlSubmissionBatch.CorrelatedSubmission]): Future[Unit] = {
    assert(submissions.nonEmpty) // Empty batches should never happen

    // Pick a correlation id for the batch.
    val correlationId = UUID.randomUUID().toString

    newLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      // Log the correlation ids of the submissions so we can correlate the batch to the submissions.
      val childCorrelationIds = submissions.map(_.getCorrelationId).mkString(", ")
      logger.trace(s"Committing batch $correlationId with submissions: $childCorrelationIds")
      val batch = DamlSubmissionBatch.newBuilder
        .addAllSubmissions(submissions.asJava)
        .build
      val envelope = Envelope.enclose(batch)

      try {
        writer
          .commit(correlationId, envelope)
          .map {
            case SubmissionResult.Acknowledged => ()
            case err =>
              logger.error(s"Batch dropped as commit failed: $err")
          }
      } catch {
        case e: Throwable =>
          logger.error(s"writer.commit threw an exception: $e!")
          throw e
      }
    }
  }

  override def commit(correlationId: String, envelope: kvutils.Bytes): Future[SubmissionResult] =
    queueHandle
      .offer(
        DamlSubmissionBatch.CorrelatedSubmission.newBuilder
          .setCorrelationId(correlationId)
          .setSubmission(envelope)
          .build)

  override def participantId: ParticipantId = writer.participantId

  override def currentHealth(): HealthStatus =
    if (queueHandle.alive)
      writer.currentHealth()
    else
      HealthStatus.unhealthy

}
