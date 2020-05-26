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
import scala.concurrent.{ExecutionContext, Future}

/** A batching ledger writer that collects submissions into a batch and commits
  * the batch once a set time and byte limit has been reached.
  *
  * @param queue The batching queue implementation
  * @param writer The underlying ledger writer to use to commit the batch
  */
class BatchingLedgerWriter(val queue: BatchingQueue, val writer: LedgerWriter)(
    implicit val materializer: Materializer,
    implicit val logCtx: LoggingContext)
    extends LedgerWriter
    with Closeable {

  implicit val executionContext: ExecutionContext = materializer.executionContext
  private val logger = ContextualizedLogger.get(getClass)
  private val queueHandle = queue.run(commitBatch)

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
      writer
        .commit(correlationId, envelope)
        .map {
          case SubmissionResult.Acknowledged => ()
          case err =>
            logger.error(s"Batch dropped as commit failed: $err")
        }
    }
  }

  override def close(): Unit = queueHandle.close()
}
