// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Time
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.telemetry.TelemetryContext
import com.google.common.primitives.Longs

case class ReadWriteServiceBridge(
    participantId: ParticipantId,
    ledgerId: LedgerId,
    maxDedupSeconds: Int,
    submissionBufferSize: Int,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadService
    with WriteService
    with AutoCloseable {
  import ReadWriteServiceBridge._

  private[this] val logger = ContextualizedLogger.get(getClass)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    submit(
      Submission.Transaction(
        submitterInfo = submitterInfo,
        transactionMeta = transactionMeta,
        transaction = transaction,
        estimatedInterpretationCost = estimatedInterpretationCost,
      )
    )

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.Config(
        maxRecordTime = maxRecordTime,
        submissionId = submissionId,
        config = config,
      )
    )

  override def currentHealth(): HealthStatus = HealthStatus.healthy

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    submit(
      Submission.AllocateParty(
        hint = hint,
        displayName = displayName,
        submissionId = submissionId,
      )
    )

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.UploadPackages(
        submissionId = submissionId,
        archives = archives,
        sourceDescription = sourceDescription,
      )
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: SubmissionId,
  ): CompletionStage[PruningResult] =
    CompletableFuture.completedFuture(
      PruningResult.ParticipantPruned
    )

  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(
      LedgerInitialConditions(
        ledgerId = ledgerId,
        config = Configuration(
          generation = 1L,
          timeModel = TimeModel.reasonableDefault,
          maxDeduplicationTime = java.time.Duration.ofSeconds(maxDedupSeconds.toLong),
        ),
        initialRecordTime = Timestamp.now(),
      )
    )

  var stateUpdatesWasCalledAlready = false
  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    // TODO for PoC purposes:
    //   no beginAfter supported
    //   neither multiple subscriptions
    //   neither bootstrapping the bridge from indexer persistence
    assert(
      beginAfter.isEmpty,
      "Re-subscribing not supported. Only supported to subscribe once, and from inception.",
    )
    synchronized {
      if (stateUpdatesWasCalledAlready)
        throw new IllegalStateException("not allowed to call this twice")
      else stateUpdatesWasCalledAlready = true
    }
    logger.info("Indexer subscribed to state updates.")
    queueSource
  }

  val (queue: BoundedSourceQueue[Submission], queueSource: Source[(Offset, Update), NotUsed]) =
    Source
      .queue[Submission](submissionBufferSize)
      .zipWithIndex
      .map { case (submission, index) =>
        (toOffset(index), successMapper(submission, index, participantId))
      }
      .preMaterialize()

  logger.info(
    s"BridgeLedgerFactory initialized. Configuration: [maxDedupSeconds: $maxDedupSeconds, submissionBufferSize: $submissionBufferSize]"
  )

  private def submit(submission: Submission): CompletionStage[SubmissionResult] =
    toSubmissionResult(queue.offer(submission))

  override def close(): Unit = {
    logger.info("Shutting down BridgeLedgerFactory.")
    queue.complete()
  }
}

object ReadWriteServiceBridge {
  trait Submission
  object Submission {
    case class Transaction(
        submitterInfo: SubmitterInfo,
        transactionMeta: TransactionMeta,
        transaction: SubmittedTransaction,
        estimatedInterpretationCost: Long,
    ) extends Submission
    case class Config(
        maxRecordTime: Time.Timestamp,
        submissionId: SubmissionId,
        config: Configuration,
    ) extends Submission
    case class AllocateParty(
        hint: Option[Party],
        displayName: Option[String],
        submissionId: SubmissionId,
    ) extends Submission
    case class UploadPackages(
        submissionId: SubmissionId,
        archives: List[Archive],
        sourceDescription: Option[String],
    ) extends Submission
  }

  private[this] val logger = ContextualizedLogger.get(getClass)

  def successMapper(s: Submission, index: Long, participantId: ParticipantId): Update = s match {
    case s: Submission.AllocateParty =>
      val party = s.hint.getOrElse(UUID.randomUUID().toString)
      Update.PartyAddedToParticipant(
        party = party.asInstanceOf[Party],
        displayName = s.displayName.getOrElse(party),
        participantId = participantId,
        recordTime = Time.Timestamp.now(),
        submissionId = Some(s.submissionId),
      )

    case s: Submission.Config =>
      Update.ConfigurationChanged(
        recordTime = Time.Timestamp.now(),
        submissionId = s.submissionId,
        participantId = participantId,
        newConfiguration = s.config,
      )

    case s: Submission.UploadPackages =>
      Update.PublicPackageUpload(
        archives = s.archives,
        sourceDescription = s.sourceDescription,
        recordTime = Time.Timestamp.now(),
        submissionId = Some(s.submissionId),
      )

    case s: Submission.Transaction =>
      Update.TransactionAccepted(
        optSubmitterInfo = Some(s.submitterInfo),
        transactionMeta = s.transactionMeta,
        transaction = s.transaction.asInstanceOf[CommittedTransaction],
        transactionId = index.toString.asInstanceOf[TransactionId],
        recordTime = Time.Timestamp.now(),
        divulgedContracts = Nil,
        blindingInfo = None,
      )
  }

  def toOffset(index: Long): Offset = Offset.fromByteArray(Longs.toByteArray(index))

  def toSubmissionResult(
      queueOfferResult: QueueOfferResult
  )(implicit loggingContext: LoggingContext): CompletableFuture[SubmissionResult] =
    CompletableFuture.completedFuture(
      queueOfferResult match {
        case QueueOfferResult.Enqueued =>
          SubmissionResult.Acknowledged

        case QueueOfferResult.Dropped =>
          logger.warn(
            "Buffer overflow: new submission is not added, signalized `Overloaded` for caller."
          )
          SubmissionResult.Overloaded

        case QueueOfferResult.Failure(throwable) =>
          logger.error("Error enqueueing new submission.", throwable)
          SubmissionResult.InternalError(throwable.getMessage)

        case QueueOfferResult.QueueClosed =>
          logger.error("Error enqueueing new submission: queue is closed.")
          SubmissionResult.InternalError("Service is shutting down.")
      }
    )
}
