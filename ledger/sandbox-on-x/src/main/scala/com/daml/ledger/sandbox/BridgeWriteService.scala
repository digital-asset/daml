// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2._
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.{CommittedTransaction, SubmittedTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.telemetry.TelemetryContext
import com.google.common.primitives.Longs
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}

class BridgeWriteService(
    feedSink: Sink[(Offset, Update), NotUsed],
    participantId: Ref.ParticipantId,
    submissionBufferSize: Int,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends WriteService
    with AutoCloseable {
  import BridgeWriteService._

  private[this] val logger = ContextualizedLogger.get(getClass)

  override def isApiDeduplicationEnabled: Boolean = true

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
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
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.Config(
        maxRecordTime = maxRecordTime,
        submissionId = submissionId,
        config = config,
      )
    )

  override def currentHealth(): HealthStatus = Healthy

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.AllocateParty(
        hint = hint,
        displayName = displayName,
        submissionId = submissionId,
      )
    )

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
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
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    CompletableFuture.completedFuture(
      PruningResult.ParticipantPruned
    )

  private val queue: BoundedSourceQueue[Submission] = {
    val (queue, queueSource) =
      Source
        .queue[Submission](submissionBufferSize)
        .zipWithIndex
        .map { case (submission, index) =>
          (toOffset(index), successMapper(submission, index, participantId))
        }
        .preMaterialize()

    queueSource.runWith(feedSink)
    logger.info(
      s"Write service initialized. Configuration: [submissionBufferSize: $submissionBufferSize]"
    )
    queue
  }

  private def submit(submission: Submission): CompletionStage[SubmissionResult] =
    toSubmissionResult(queue.offer(submission))

  override def close(): Unit = {
    logger.info("Shutting down BridgeLedgerFactory.")
    queue.complete()
  }
}

object BridgeWriteService {
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
        submissionId: Ref.SubmissionId,
        config: Configuration,
    ) extends Submission
    case class AllocateParty(
        hint: Option[Ref.Party],
        displayName: Option[String],
        submissionId: Ref.SubmissionId,
    ) extends Submission

    case class UploadPackages(
        submissionId: Ref.SubmissionId,
        archives: List[Archive],
        sourceDescription: Option[String],
    ) extends Submission
  }

  private[this] val logger = ContextualizedLogger.get(getClass)

  def successMapper(submission: Submission, index: Long, participantId: Ref.ParticipantId): Update =
    submission match {
      case s: Submission.AllocateParty =>
        val party = s.hint.getOrElse(UUID.randomUUID().toString)
        Update.PartyAddedToParticipant(
          party = Ref.Party.assertFromString(party),
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
          optCompletionInfo = Some(s.submitterInfo.toCompletionInfo),
          transactionMeta = s.transactionMeta,
          transaction = s.transaction.asInstanceOf[CommittedTransaction],
          transactionId = Ref.TransactionId.assertFromString(index.toString),
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
        case QueueOfferResult.Enqueued => SubmissionResult.Acknowledged
        case QueueOfferResult.Dropped =>
          logger.warn(
            "Buffer overflow: new submission is not added, signalized `Overloaded` for caller."
          )
          SubmissionResult.SynchronousError(
            Status(
              Code.RESOURCE_EXHAUSTED.value
            )
          )
        case QueueOfferResult.Failure(throwable) =>
          logger.error("Error enqueueing new submission.", throwable)
          SubmissionResult.SynchronousError(
            Status(
              Code.INTERNAL.value,
              throwable.getMessage,
            )
          )
        case QueueOfferResult.QueueClosed =>
          logger.error("Error enqueueing new submission: queue is closed.")
          SubmissionResult.SynchronousError(
            Status(
              Code.INTERNAL.value,
              "Queue is closed",
            )
          )
      }
    )
}
