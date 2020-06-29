// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.io.Closeable
import java.util.UUID

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.api.health.HealthStatus
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}

/** A batching ledger writer that collects submissions into a batch and commits
  * the batch once a set time or byte limit has been reached.
  * Use `apply()` from the companion object to construct an instance from a [[BatchingLedgerWriterConfig]] and a
  * [[LedgerWriter]] delegate instance.
  *
  * @param queue batching queue implementation
  * @param writer underlying ledger writer that will commit batches
  */
class BatchingLedgerWriter(val queue: BatchingQueue, val writer: LedgerWriter)(
    implicit val materializer: Materializer,
    implicit val logCtx: LoggingContext)
    extends LedgerWriter
    with Closeable {

  implicit val executionContext: ExecutionContext = materializer.executionContext
  private val logger = ContextualizedLogger.get(getClass)
  private val queueHandle = queue.run(commitBatch)

  override def commit(
      correlationId: String,
      envelope: kvutils.Bytes,
      metadata: CommitMetadata,
    ): Future[SubmissionResult] = {
    val correlatedSubmissionBuilder = DamlSubmissionBatch.CorrelatedSubmission.newBuilder
      .setCorrelationId(correlationId)
      .setSubmission(envelope)
    metadata.estimatedInterpretationCost.foreach(correlatedSubmissionBuilder.setEstimatedInterpretationCost)
    queueHandle
      .offer(correlatedSubmissionBuilder.build)
  }

  override def participantId: ParticipantId = writer.participantId

  override def currentHealth(): HealthStatus =
    if (queueHandle.alive)
      writer.currentHealth()
    else
      HealthStatus.unhealthy

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
      // We assume parallelization of interpretation hence return max.
      val totalEstimatedInterpretationCost = submissions.map(_.getEstimatedInterpretationCost).max
      writer
        .commit(correlationId, envelope, SimpleCommitMetadata(estimatedInterpretationCost =
          Some(totalEstimatedInterpretationCost)))
        .map {
          case SubmissionResult.Acknowledged => ()
          case err =>
            logger.error(s"Batch dropped as commit failed: $err")
        }
    }
  }

  override def close(): Unit = queueHandle.close()
}

object BatchingLedgerWriter {
  def apply(batchingLedgerWriterConfig: BatchingLedgerWriterConfig, delegate: LedgerWriter)(
      implicit materializer: Materializer,
      loggingContext: LoggingContext): BatchingLedgerWriter = {
    val batchingQueue = batchingQueueFrom(batchingLedgerWriterConfig)
    new BatchingLedgerWriter(batchingQueue, delegate)
  }

  private def batchingQueueFrom(
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig): BatchingQueue =
    if (batchingLedgerWriterConfig.enableBatching) {
      DefaultBatchingQueue(
        maxQueueSize = batchingLedgerWriterConfig.maxBatchQueueSize,
        maxBatchSizeBytes = batchingLedgerWriterConfig.maxBatchSizeBytes,
        maxWaitDuration = batchingLedgerWriterConfig.maxBatchWaitDuration,
        maxConcurrentCommits = batchingLedgerWriterConfig.maxBatchConcurrentCommits
      )
    } else {
      batchingQueueForSerialValidation(batchingLedgerWriterConfig.maxBatchQueueSize)
    }

  private def batchingQueueForSerialValidation(maxBatchQueueSize: Int): DefaultBatchingQueue =
    DefaultBatchingQueue(
      maxQueueSize = maxBatchQueueSize,
      maxBatchSizeBytes = 1,
      maxWaitDuration = Duration(1, MILLISECONDS),
      maxConcurrentCommits = 1
    )
}
