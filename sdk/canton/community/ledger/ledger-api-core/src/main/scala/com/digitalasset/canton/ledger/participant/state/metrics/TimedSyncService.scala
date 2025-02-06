// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.metrics

import com.daml.error.ContextualizedErrorLogger
import com.daml.metrics.Timed
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{Offset, ProcessedDisclosedContract}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.ledger.participant.state.SyncService.{
  ConnectedSynchronizerRequest,
  ConnectedSynchronizerResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{GlobalKey, SubmittedTransaction}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString

import java.util.concurrent.CompletionStage
import scala.concurrent.Future

final class TimedSyncService(delegate: SyncService, metrics: LedgerApiServerMetrics)
    extends SyncService {

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      optSynchronizerId: Option[SynchronizerId],
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.services.write.submitTransaction,
      metrics.services.write.submitTransactionRunning,
      delegate.submitTransaction(
        submitterInfo,
        optSynchronizerId,
        transactionMeta,
        transaction,
        estimatedInterpretationCost,
        globalKeyMapping,
        processedDisclosedContracts,
      ),
    )

  def submitReassignment(
      submitter: Ref.Party,
      applicationId: Ref.ApplicationId,
      commandId: Ref.CommandId,
      submissionId: Option[Ref.SubmissionId],
      workflowId: Option[Ref.WorkflowId],
      reassignmentCommand: ReassignmentCommand,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.services.write.submitReassignment,
      metrics.services.write.submitReassignmentRunning,
      delegate.submitReassignment(
        submitter,
        applicationId,
        commandId,
        submissionId,
        workflowId,
        reassignmentCommand,
      ),
    )

  override def uploadDar(
      dar: ByteString,
      submissionId: Ref.SubmissionId,
  )(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    Timed.future(
      metrics.services.write.uploadPackages,
      delegate.uploadDar(dar, submissionId),
    )

  override def allocateParty(
      hint: Ref.Party,
      submissionId: Ref.SubmissionId,
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.services.write.allocateParty,
      delegate.allocateParty(hint, submissionId),
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    Timed.completionStage(
      metrics.services.write.prune,
      delegate.prune(pruneUpToInclusive, submissionId, pruneAllDivulgedContracts),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  override def getConnectedSynchronizers(
      request: ConnectedSynchronizerRequest
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ConnectedSynchronizerResponse] =
    Timed.future(
      metrics.services.read.getConnectedSynchronizers,
      delegate.getConnectedSynchronizers(request),
    )

  override def getProtocolVersionForSynchronizer(
      synchronizerId: Traced[SynchronizerId]
  ): Option[ProtocolVersion] =
    delegate.getProtocolVersionForSynchronizer(synchronizerId)

  override def incompleteReassignmentOffsets(validAt: Offset, stakeholders: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Vector[Offset]] =
    Timed.future(
      metrics.services.read.getConnectedSynchronizers,
      delegate.incompleteReassignmentOffsets(validAt, stakeholders),
    )

  override def registerInternalStateService(internalStateService: InternalStateService): Unit =
    delegate.registerInternalStateService(internalStateService)

  override def internalStateService: Option[InternalStateService] =
    delegate.internalStateService

  override def unregisterInternalStateService(): Unit =
    delegate.unregisterInternalStateService()

  override def getPackageMetadataSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata =
    delegate.getPackageMetadataSnapshot

  override def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    Timed.future(
      metrics.services.read.listLfPackages,
      delegate.listLfPackages(),
    )

  override def getLfArchive(
      packageId: PackageId
  )(implicit traceContext: TraceContext): Future[Option[Archive]] =
    Timed.future(
      metrics.services.read.getLfArchive,
      delegate.getLfArchive(packageId),
    )

  override def validateDar(dar: ByteString, darName: String)(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    Timed.future(
      metrics.services.read.validateDar,
      delegate.validateDar(dar, darName),
    )
}
