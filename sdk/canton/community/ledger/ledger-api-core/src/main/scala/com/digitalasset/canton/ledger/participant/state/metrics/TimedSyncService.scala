// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.metrics

import cats.data.EitherT
import com.daml.metrics.Timed
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.error.{TransactionError, TransactionRoutingError}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.api.{
  EnrichedVettedPackage,
  ListVettedPackagesOpts,
  UpdateVettedPackagesOpts,
  UploadDarVettingChange,
}
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.ledger.participant.state.SyncService.{
  ConnectedSynchronizerRequest,
  ConnectedSynchronizerResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.protocol.{LfContractId, LfFatContractInst, LfSubmittedTransaction}
import com.digitalasset.canton.topology.{
  ExternalPartyOnboardingDetails,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfKeyResolver, LfPartyId}
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.SubmittedTransaction
import com.google.protobuf.ByteString

import java.util.concurrent.CompletionStage
import scala.concurrent.Future

final class TimedSyncService(delegate: SyncService, metrics: LedgerApiServerMetrics)
    extends SyncService {

  override def submitTransaction(
      transaction: SubmittedTransaction,
      synchronizerRank: SynchronizerRank,
      routingSynchronizerState: RoutingSynchronizerState,
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      // Currently, the estimated interpretation cost is not used
      _estimatedInterpretationCost: Long,
      keyResolver: LfKeyResolver,
      processedDisclosedContracts: ImmArray[LfFatContractInst],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.services.write.submitTransaction,
      metrics.services.write.submitTransactionRunning,
      delegate.submitTransaction(
        transaction,
        synchronizerRank,
        routingSynchronizerState,
        submitterInfo,
        transactionMeta,
        _estimatedInterpretationCost,
        keyResolver,
        processedDisclosedContracts,
      ),
    )

  def submitReassignment(
      submitter: Ref.Party,
      userId: Ref.UserId,
      commandId: Ref.CommandId,
      submissionId: Option[Ref.SubmissionId],
      workflowId: Option[Ref.WorkflowId],
      reassignmentCommands: Seq[ReassignmentCommand],
  )(implicit
      traceContext: TraceContext
  ): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.services.write.submitReassignment,
      metrics.services.write.submitReassignmentRunning,
      delegate.submitReassignment(
        submitter,
        userId,
        commandId,
        submissionId,
        workflowId,
        reassignmentCommands,
      ),
    )

  override def uploadDar(
      dar: Seq[ByteString],
      submissionId: Ref.SubmissionId,
      vettingChange: UploadDarVettingChange,
  )(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    Timed.future(
      metrics.services.write.uploadPackages,
      delegate.uploadDar(dar, submissionId, vettingChange),
    )

  override def allocateParty(
      hint: Ref.Party,
      submissionId: Ref.SubmissionId,
      synchronizerIdO: Option[SynchronizerId],
      externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SubmissionResult] =
    Timed.future(
      metrics.services.write.allocateParty,
      delegate.allocateParty(hint, submissionId, synchronizerIdO, externalPartyOnboardingDetails),
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
  ): CompletionStage[PruningResult] =
    Timed.completionStage(
      metrics.services.write.prune,
      delegate.prune(pruneUpToInclusive, submissionId),
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

  override def incompleteReassignmentOffsets(validAt: Offset, stakeholders: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Vector[Offset]] =
    Timed.future(
      metrics.services.read.getConnectedSynchronizers,
      delegate.incompleteReassignmentOffsets(validAt, stakeholders),
    )

  override def registerInternalIndexService(internalIndexService: InternalIndexService): Unit =
    delegate.registerInternalIndexService(internalIndexService)

  override def internalIndexService: Option[InternalIndexService] =
    delegate.internalIndexService

  override def unregisterInternalIndexService(): Unit =
    delegate.unregisterInternalIndexService()

  override def getPackageMetadataSnapshot(implicit
      errorLoggingContext: ErrorLoggingContext
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

  override def updateVettedPackages(
      opts: UpdateVettedPackagesOpts
  )(implicit
      traceContext: TraceContext
  ): Future[(Seq[EnrichedVettedPackage], Seq[EnrichedVettedPackage])] =
    Timed.future(
      metrics.services.write.updateVettedPackages,
      delegate.updateVettedPackages(opts),
    )

  override def listVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit
      traceContext: TraceContext
  ): Future[Option[(Seq[EnrichedVettedPackage], PositiveInt)]] =
    Timed.future(
      metrics.services.read.listVettedPackages,
      delegate.listVettedPackages(opts),
    )

  // TODO(#25385): Time the operation
  override def packageMapFor(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      vettingValidityTimestamp: CantonTimestamp,
      prescribedSynchronizer: Option[SynchronizerId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PhysicalSynchronizerId, Map[LfPartyId, Set[PackageId]]]] =
    delegate.packageMapFor(
      submitters,
      informees,
      vettingValidityTimestamp,
      prescribedSynchronizer,
      routingSynchronizerState,
    )

  // TODO(#25385): Time the operation
  override def computeHighestRankedSynchronizerFromAdmissible(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]],
      disclosedContractIds: List[LfContractId],
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, PhysicalSynchronizerId] =
    delegate.computeHighestRankedSynchronizerFromAdmissible(
      submitterInfo,
      transaction,
      transactionMeta,
      admissibleSynchronizers,
      disclosedContractIds,
      routingSynchronizerState,
    )

  // TODO(#25385): Time the operation
  override def selectRoutingSynchronizer(
      submitterInfo: SubmitterInfo,
      transaction: LfSubmittedTransaction,
      transactionMeta: TransactionMeta,
      disclosedContractIds: List[LfContractId],
      optSynchronizerId: Option[SynchronizerId],
      transactionUsedForExternalSigning: Boolean,
      routingSynchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionError, SynchronizerRank] =
    delegate.selectRoutingSynchronizer(
      submitterInfo,
      transaction,
      transactionMeta,
      disclosedContractIds,
      optSynchronizerId,
      transactionUsedForExternalSigning,
      routingSynchronizerState,
    )

  override def getRoutingSynchronizerState(implicit
      traceContext: TraceContext
  ): RoutingSynchronizerState =
    delegate.getRoutingSynchronizerState

  override def protocolVersionForSynchronizerId(
      synchronizerId: SynchronizerId
  ): Option[ProtocolVersion] = delegate.protocolVersionForSynchronizerId(synchronizerId)
}
