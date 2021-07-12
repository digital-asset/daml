// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Instant
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v2.AdaptedV1WriteService.{adaptLedgerConfiguration, adaptPruningResult, adaptSubmissionResult}
import com.daml.lf.data.Time
import com.daml.telemetry.TelemetryContext
import com.google.rpc.status.Status
import AdaptedV1WriteService._
import com.google.rpc.code.Code

class AdaptedV1WriteService(delegate: v1.WriteService) extends WriteService {
  override def submitTransaction(submitterInfo: SubmitterInfo, transactionMeta: TransactionMeta, transaction: SubmittedTransaction, estimatedInterpretationCost: Long)(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .submitTransaction(adaptSubmitterInfo(submitterInfo), adaptTransactionMeta(transactionMeta), transaction, estimatedInterpretationCost)
      .thenApply(adaptSubmissionResult)

  /**
   * @return an UNIMPLEMENTED gRPC error as v1.WriteService doesn't support this functionality.
   */
  override def rejectSubmission(submitterInfo: SubmitterInfo, submissionTime: Time.Timestamp, reason: Status)(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    CompletableFuture.supplyAsync(() =>
      SubmissionResult.SynchronousError(Status.of(Code.UNIMPLEMENTED.index, "WriteService.rejectSubmission not implemented for v1 adaptor", NoErrorDetails))
    )

  override def allocateParty(hint: Option[Party], displayName: Option[String], submissionId: SubmissionId)(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .allocateParty(hint, displayName, submissionId)
      .thenApply(adaptSubmissionResult)

  override def submitConfiguration(maxRecordTime: Time.Timestamp, submissionId: SubmissionId, config: Configuration)(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .submitConfiguration(maxRecordTime, submissionId, adaptLedgerConfiguration(config))
      .thenApply(adaptSubmissionResult)

  override def prune(pruneUpToInclusive: Offset, submissionId: SubmissionId): CompletionStage[PruningResult] =
    delegate
      .prune(v1.Offset(pruneUpToInclusive.bytes), submissionId)
      .thenApply(adaptPruningResult)

  override def uploadPackages(submissionId: SubmissionId, archives: List[DamlLf.Archive], sourceDescription: Option[String])(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .uploadPackages(submissionId, archives, sourceDescription)
      .thenApply(adaptSubmissionResult)

  override def currentHealth(): HealthStatus = delegate.currentHealth()
}

private[v2] object AdaptedV1WriteService {
  private val NoErrorDetails = Seq.empty[com.google.protobuf.any.Any]

  def adaptSubmitterInfo(submitterInfo: SubmitterInfo): v1.SubmitterInfo = {
    val deduplicateUntil = submitterInfo.deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(duration) => Instant.now().plus(duration)
    }
    v1.SubmitterInfo(
      actAs = submitterInfo.actAs,
      applicationId = submitterInfo.applicationId,
      commandId = submitterInfo.commandId,
      deduplicateUntil = deduplicateUntil,
    )
  }

  def adaptTransactionMeta(transactionMeta: TransactionMeta): v1.TransactionMeta =
    v1.TransactionMeta(
      ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
      workflowId = transactionMeta.workflowId,
      submissionTime = transactionMeta.submissionTime,
      submissionSeed = transactionMeta.submissionSeed,
      optUsedPackages = transactionMeta.optUsedPackages,
      optNodeSeeds = transactionMeta.optNodeSeeds,
      optByKeyNodes = transactionMeta.optByKeyNodes,
    )

  def adaptPruningResult(input: v1.PruningResult): PruningResult = input match {
    case v1.PruningResult.ParticipantPruned => PruningResult.ParticipantPruned
    case v1.PruningResult.NotPruned(grpcStatus) => PruningResult.NotPruned(grpcStatus)
  }

  // FIXME(miklos-da): Convert synchronous rejection from v1.
  def adaptSubmissionResult(input: v1.SubmissionResult): SubmissionResult =
    input match {
      case v1.SubmissionResult.Acknowledged =>
        SubmissionResult.Acknowledged
      case v1.SubmissionResult.Overloaded =>
        SubmissionResult.SynchronousError(
          Status.of(Code.RESOURCE_EXHAUSTED.index, "Overloaded", NoErrorDetails)
        )
      case v1.SubmissionResult.NotSupported =>
        SubmissionResult.SynchronousError(
          Status.of(Code.UNIMPLEMENTED.index, "Not supported", NoErrorDetails)
        )
      case v1.SubmissionResult.InternalError(reason) =>
        SubmissionResult.SynchronousError(
          Status.of(Code.INTERNAL.index, reason, NoErrorDetails)
        )
      case v1.SubmissionResult.SynchronousReject(failure) =>
        SubmissionResult.SynchronousReject(failure)
    }

  def adaptLedgerConfiguration(input: Configuration): v1.Configuration =
    v1.Configuration(
      generation = input.generation,
      timeModel = adaptTimeModel(input.timeModel),
      maxDeduplicationTime = input.maxDeduplicationTime,
    )

  private def adaptTimeModel(timeModel: TimeModel): v1.TimeModel =
    v1.TimeModel(
      avgTransactionLatency = timeModel.avgTransactionLatency,
      minSkew = timeModel.minSkew,
      maxSkew = timeModel.maxSkew,
    )
      .get
}
