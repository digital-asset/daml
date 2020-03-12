package com.daml.ledger.participant.state.kvutils.api

import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmissionBatch
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class BatchingLedgerWriter(
    val writer: LedgerWriter,
    val maxQueueSize: Int,
    val maxBatchSizeBytes: Long,
    val maxWaitDuration: FiniteDuration,
    val maxParallelism: Int)(
    implicit val materializer: Materializer,
    implicit val logCtx: LoggingContext)
    extends LedgerWriter {

  implicit val executionContext: ExecutionContext = materializer.executionContext
  private val logger = ContextualizedLogger.get(getClass)

  /** Flag to mark whether the submission queue has been completed or not. Used for health status. */
  private var queueAlive = true

  @SuppressWarnings(Array("org.wartremover.warts.Any")) /* Keep.left */
  private val queue: SourceQueueWithComplete[DamlSubmissionBatch.CorrelatedSubmission] = {
    // Sink to commit batches coming from the queue. We use foreachAsync to commit them in parallel.
    // TODO(JM): Verify that foreachAsync actually works correctly and that we use sensible executionContext!
    val commitSink: Sink[Seq[DamlSubmissionBatch.CorrelatedSubmission], _] =
      Sink.foreachAsync(maxParallelism)(commitBatch)

    val queue = Source
      .queue(maxQueueSize, OverflowStrategy.dropNew)
      .groupedWeightedWithin(maxBatchSizeBytes, maxWaitDuration)(
        (cs: DamlSubmissionBatch.CorrelatedSubmission) => cs.getSubmission.size.toLong)
      .toMat(commitSink)(Keep.left)
      .run

    // Watch for completion of the queue to mark the writer as unhealthy, so that upper layers can
    // handle restarting.
    // TODO(JM): Is there a neater way to accomplish this?
    queue.watchCompletion.foreach { _ =>
      queueAlive = false
    }

    queue
  }

  private def commitBatch(
      submissions: Seq[DamlSubmissionBatch.CorrelatedSubmission]): Future[Unit] = {
    assert(submissions.nonEmpty) // Empty batches should never happen

    // Use the first submission's correlation id for the batch. This is potentially confusing
    // so we might want to revisit this.
    val correlationId = submissions.head.getCorrelationId

    val batch = DamlSubmissionBatch.newBuilder
      .addAllSubmissions(submissions.asJava)
      .build
    val envelope = Envelope.enclose(batch)
    writer
      .commit(correlationId, envelope.toByteArray)
      .map {
        case SubmissionResult.Acknowledged => ()
        case other =>
          // TODO(JM): What are our options here? We cannot signal the applications directly about the outcome
          // as we've already acknowledged towards them. We can do a RestartSink, but it feels like that should
          // be the underlying LedgerWriter's decision. Dropping seems reasonable.
          logger.warn(s"commitBatch($correlationId): Batch dropped as commit failed: $other")
      }
  }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    queue
      .offer(
        DamlSubmissionBatch.CorrelatedSubmission.newBuilder
          .setCorrelationId(correlationId)
          .setSubmission(ByteString.copyFrom(envelope))
          .build)
      .map {
        case QueueOfferResult.Enqueued => SubmissionResult.Acknowledged
        case QueueOfferResult.Dropped => SubmissionResult.Overloaded
        case f: QueueOfferResult.Failure => SubmissionResult.InternalError(f.toString)
        case QueueOfferResult.QueueClosed =>
          SubmissionResult.InternalError("BatchingLedgerWriter.queue is closed")
      }

  override def participantId: ParticipantId = writer.participantId

  override def currentHealth(): HealthStatus =
    if (queueAlive)
      writer.currentHealth()
    else
      HealthStatus.unhealthy

}
