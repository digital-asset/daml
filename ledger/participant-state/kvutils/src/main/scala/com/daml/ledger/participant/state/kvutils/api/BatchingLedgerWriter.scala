// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.io.Closeable
import java.util.UUID

import akka.stream.Materializer
import com.daml.ledger.api.health.{HealthStatus, Unhealthy}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

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
    implicit val loggingContext: LoggingContext,
) extends LedgerWriter
    with Closeable {

  implicit val executionContext: ExecutionContext = materializer.executionContext
  private val logger = ContextualizedLogger.get(getClass)
  private val queueHandle = queue.run(commitBatch)

  /** Buffers the submission for the next batch.
    * Note that the [[CommitMetadata]] written to the delegate along with a batched submission will
    * be Empty as output keys cannot be determined due to potential conflicts among input/output
    * keys in the batch.
    */
  override def commit(
      correlationId: String,
      envelope: Raw.Envelope,
      metadata: CommitMetadata,
  )(implicit telemetryContext: TelemetryContext): Future[SubmissionResult] =
    queueHandle.offer(
      DamlSubmissionBatch.CorrelatedSubmission.newBuilder
        .setCorrelationId(correlationId)
        .setSubmission(envelope.bytes)
        .build
    )

  override def participantId: Ref.ParticipantId = writer.participantId

  override def currentHealth(): HealthStatus =
    if (queueHandle.isAlive)
      writer.currentHealth()
    else
      Unhealthy

  private def commitBatch(
      submissions: Seq[DamlSubmissionBatch.CorrelatedSubmission]
  ): Future[Unit] = {
    assert(submissions.nonEmpty) // Empty batches should never happen

    // Pick a correlation id for the batch.
    val correlationId = UUID.randomUUID().toString

    newLoggingContextWith("correlationId" -> correlationId) { implicit loggingContext =>
      // Log the correlation ids of the submissions so we can correlate the batch to the submissions.
      val childCorrelationIds = submissions.map(_.getCorrelationId).mkString(", ")
      logger.trace(s"Committing batch $correlationId with submissions: $childCorrelationIds")
      val batch = DamlSubmissionBatch.newBuilder
        .addAllSubmissions(submissions.asJava)
        .build
      val envelope = Envelope.enclose(batch)
      writer
        // Use the NoOpTelemetryContext, as support for batching will be removed soon.
        .commit(correlationId, envelope, CommitMetadata.Empty)(NoOpTelemetryContext)
        .map {
          case SubmissionResult.Acknowledged => ()
          case error =>
            logger.error(s"Batch dropped as commit failed: $error")
        }
    }
  }

  override def close(): Unit = {
    // Do not wait for the queue to complete; just fire and forget.
    queueHandle.stop()
    ()
  }
}

object BatchingLedgerWriter {
  def apply(batchingLedgerWriterConfig: BatchingLedgerWriterConfig, delegate: LedgerWriter)(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): BatchingLedgerWriter = {
    val batchingQueue = batchingQueueFrom(batchingLedgerWriterConfig)
    new BatchingLedgerWriter(batchingQueue, delegate)
  }

  private def batchingQueueFrom(
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig
  ): BatchingQueue =
    if (batchingLedgerWriterConfig.enableBatching) {
      DefaultBatchingQueue(
        maxQueueSize = batchingLedgerWriterConfig.maxBatchQueueSize,
        maxBatchSizeBytes = batchingLedgerWriterConfig.maxBatchSizeBytes,
        maxWaitDuration = batchingLedgerWriterConfig.maxBatchWaitDuration,
        maxConcurrentCommits = batchingLedgerWriterConfig.maxBatchConcurrentCommits,
      )
    } else {
      batchingQueueForSerialValidation(batchingLedgerWriterConfig.maxBatchQueueSize)
    }

  private def batchingQueueForSerialValidation(maxBatchQueueSize: Int): DefaultBatchingQueue =
    DefaultBatchingQueue(
      maxQueueSize = maxBatchQueueSize,
      maxBatchSizeBytes = 1,
      maxWaitDuration = Duration(1, MILLISECONDS),
      maxConcurrentCommits = 1,
    )
}
