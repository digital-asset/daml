// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.BridgeMetrics
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig}
import com.daml.ledger.participant.state.v2._
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.telemetry.TelemetryContext
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.ExecutionContext

class BridgeWriteService(
    feedSink: Sink[(Offset, Update), NotUsed],
    submissionBufferSize: Int,
    ledgerBridge: LedgerBridge,
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
        // TODO SoX: Ugh
        .via(ledgerBridge.flow)
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
  private[this] val logger = ContextualizedLogger.get(getClass)

  def owner(
      feedSink: Sink[(Offset, Update), NotUsed],
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      indexService: IndexService,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[WriteService] =
    for {
      initialLedgerEnd <- ResourceOwner.forFuture(() => indexService.currentLedgerEnd())
      // TODO SoX: Consider extracting building this guy in the runner
      ledgerBridge =
        if (config.extra.conflictCheckingEnabled)
          new ConflictCheckingLedgerBridge(
            participantId = participantConfig.participantId,
            indexService = indexService,
            initialLedgerEnd =
              Offset.fromHexString(Ref.HexString.assertFromString(initialLedgerEnd.value)),
            bridgeMetrics = new BridgeMetrics(metrics),
          )
        else new PassThroughLedgerBridge(participantId = participantConfig.participantId)
      writeService <- ResourceOwner.forCloseable(() =>
        new BridgeWriteService(
          feedSink = feedSink,
          submissionBufferSize = config.extra.submissionBufferSize,
          ledgerBridge = ledgerBridge,
        )
      )
    } yield writeService

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
