// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.daml_lf_dev.DamlLf
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.errors.KVErrors
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueSubmission}
import com.daml.ledger.participant.state.v2._
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.kv.archives.ArchiveConversions
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.telemetry.TelemetryContext

import scala.jdk.FutureConverters.FutureOps

class KeyValueParticipantStateWriter(
    writer: LedgerWriter,
    metrics: Metrics,
) extends WriteService {

  private val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  private val keyValueSubmission = new KeyValueSubmission(metrics)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    val submission =
      keyValueSubmission.transactionToSubmission(
        submitterInfo,
        transactionMeta,
        transaction,
      )
    val metadata = CommitMetadata(submission, Some(estimatedInterpretationCost))
    val submissionId = submitterInfo.submissionId.getOrElse {
      withEnrichedLoggingContext(
        "commandId" -> submitterInfo.commandId,
        "applicationId" -> submitterInfo.applicationId,
      ) { implicit loggingContext =>
        logger.warn("Submission id should not be empty")
      }
      ""
    }

    commit(
      correlationId = submissionId,
      submission = submission,
      metadata = Some(metadata),
    )
  }

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    ArchiveConversions.parsePackageIdsAndRawArchives(archives) match {
      case Left(_) =>
        CompletableFuture.completedFuture(
          SubmissionResult.SynchronousError(
            KVErrors.Internal.SubmissionFailed
              .Reject("Could not parse a package ID")(
                new DamlContextualizedErrorLogger(logger, loggingContext, None)
              )
              .asStatus
          )
        )
      case Right(packageIdsToRawArchives) =>
        val submission = keyValueSubmission
          .archivesToSubmission(
            submissionId,
            packageIdsToRawArchives,
            sourceDescription.getOrElse(""),
            writer.participantId,
          )
        commit(submissionId, submission)
    }
  }

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    val submission =
      keyValueSubmission
        .configurationToSubmission(maxRecordTime, submissionId, writer.participantId, config)
    commit(submissionId, submission)
  }

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    val party = hint.getOrElse(generateRandomParty())
    val submission =
      keyValueSubmission.partyToSubmission(
        submissionId,
        Some(party),
        displayName,
        writer.participantId,
      )
    commit(submissionId, submission)
  }

  override def currentHealth(): HealthStatus = writer.currentHealth()

  private def generateRandomParty(): Ref.Party =
    Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString.take(8)}")

  private def commit(
      correlationId: String,
      submission: DamlSubmission,
      metadata: Option[CommitMetadata] = None,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    writer
      .commit(
        correlationId,
        Envelope.enclose(submission),
        metadata.getOrElse(CommitMetadata(submission, None)),
      )
      .asJava

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    // kvutils has no participant local state to prune, so return success to let participant pruning proceed elsewhere.
    CompletableFuture.completedFuture(PruningResult.ParticipantPruned)
}
