// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Instant
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1
import com.daml.lf.data.Time
import com.daml.telemetry.TelemetryContext
import com.google.rpc.code.Code
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status
import io.grpc.{Metadata, StatusRuntimeException}

import scala.jdk.CollectionConverters._

class AdaptedV1WriteService(delegate: v1.WriteService) extends WriteService {
  import AdaptedV1WriteService._

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .submitTransaction(
        adaptSubmitterInfo(submitterInfo),
        adaptTransactionMeta(transactionMeta),
        transaction,
        estimatedInterpretationCost,
      )
      .thenApply(adaptSubmissionResult)

  /** @return an UNIMPLEMENTED gRPC error as v1.WriteService doesn't support this functionality.
    */
  override def rejectSubmission(
      submitterInfo: SubmitterInfo,
      submissionTime: Time.Timestamp,
      reason: Status,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    CompletableFuture.supplyAsync(() =>
      SubmissionResult.SynchronousError(
        Status.of(
          Code.UNIMPLEMENTED.index,
          "WriteService.rejectSubmission not implemented for v1 adaptor",
          NoErrorDetails,
        )
      )
    )

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .allocateParty(hint, displayName, submissionId)
      .thenApply(adaptSubmissionResult)

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    delegate
      .submitConfiguration(maxRecordTime, submissionId, adaptLedgerConfiguration(config))
      .thenApply(adaptSubmissionResult)

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: SubmissionId,
  ): CompletionStage[PruningResult] =
    delegate
      .prune(v1.Offset(pruneUpToInclusive.bytes), submissionId)
      .thenApply(adaptPruningResult)

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
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

  def adaptPruningResult(pruningResult: v1.PruningResult): PruningResult = pruningResult match {
    case v1.PruningResult.ParticipantPruned => PruningResult.ParticipantPruned
    case v1.PruningResult.NotPruned(grpcStatus) => PruningResult.NotPruned(grpcStatus)
  }

  def adaptSubmissionResult(submissionResult: v1.SubmissionResult): SubmissionResult =
    submissionResult match {
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
        val status = failure.getStatus
        val rpcStatus =
          Status.of(status.getCode.value(), status.getDescription, errorDetailsForFailure(failure))
        SubmissionResult.SynchronousError(rpcStatus)
    }

  private def errorDetailsForFailure(
      failure: StatusRuntimeException
  ): Seq[com.google.protobuf.any.Any] = {
    val trailers = failure.getTrailers
    val metadata = trailers
      .keys()
      .asScala
      .map { key =>
        key -> trailers.get[String](Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER))
      }
      .toMap
    val errorInfo = ErrorInfo.of(failure.getLocalizedMessage, "Synchronous rejection", metadata)
    Seq(com.google.protobuf.any.Any.pack(errorInfo))
  }

  def adaptLedgerConfiguration(config: Configuration): v1.Configuration =
    v1.Configuration(
      generation = config.generation,
      timeModel = adaptTimeModel(config.timeModel),
      maxDeduplicationTime = config.maxDeduplicationTime,
    )

  private def adaptTimeModel(timeModel: TimeModel): v1.TimeModel =
    v1.TimeModel(
      avgTransactionLatency = timeModel.avgTransactionLatency,
      minSkew = timeModel.minSkew,
      maxSkew = timeModel.maxSkew,
    ).get
}
