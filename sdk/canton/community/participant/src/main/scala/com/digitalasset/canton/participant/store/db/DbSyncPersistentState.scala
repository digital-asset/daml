// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
  SendTrackerStore,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.transaction.HostingParticipant
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  SynchronizerOutboxQueue,
  SynchronizerTopologyManager,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ReassignmentTag

import scala.concurrent.ExecutionContext

class DbLogicalSyncPersistentState(
    override val synchronizerIdx: IndexedSynchronizer,
    storage: DbStorage,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    contractStore: ContractStore,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends LogicalSyncPersistentState {

  private val timeouts = parameters.processingTimeouts

  override val enableAdditionalConsistencyChecks: Boolean =
    parameters.enableAdditionalConsistencyChecks

  override val activeContractStore: DbActiveContractStore =
    new DbActiveContractStore(
      storage,
      synchronizerIdx,
      Option.when(enableAdditionalConsistencyChecks)(
        parameters.activationFrequencyForWarnAboutConsistencyChecks
      ),
      parameters.stores.journalPruning.toInternal,
      indexedStringStore,
      timeouts,
      loggerFactory,
    )

  override val acsCommitmentStore = new DbAcsCommitmentStore(
    storage,
    synchronizerIdx,
    acsCounterParticipantConfigStore,
    timeouts,
    loggerFactory,
  )

  override val acsInspection: AcsInspection =
    new AcsInspection(
      lsid,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )

  override val reassignmentStore: DbReassignmentStore = new DbReassignmentStore(
    storage,
    ReassignmentTag.Target(synchronizerIdx),
    indexedStringStore,
    futureSupervisor,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    parameters.batchingConfig,
    timeouts,
    loggerFactory,
  )

  override def close(): Unit =
    LifeCycle.close(activeContractStore, acsCommitmentStore)(logger)
}

class DbPhysicalSyncPersistentState(
    participantId: ParticipantId,
    override val physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    clock: Clock,
    storage: DbStorage,
    crypto: SynchronizerCrypto,
    parameters: ParticipantNodeParameters,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    logicalSyncPersistentState: LogicalSyncPersistentState,
    packageMetadataView: Eval[PackageMetadataView],
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends PhysicalSyncPersistentState
    with AutoCloseable
    with NoTracing {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  private val timeouts = parameters.processingTimeouts
  private val batching = parameters.batchingConfig

  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    physicalSynchronizerIdx,
    timeouts,
    loggerFactory,
  )
  val requestJournalStore: DbRequestJournalStore = new DbRequestJournalStore(
    physicalSynchronizerIdx,
    storage,
    insertBatchAggregatorConfig = batching.aggregator,
    replaceBatchAggregatorConfig = batching.aggregator,
    timeouts,
    loggerFactory,
  )

  val parameterStore: DbSynchronizerParameterStore =
    new DbSynchronizerParameterStore(
      physicalSynchronizerIdx.synchronizerId,
      storage,
      timeouts,
      loggerFactory,
    )

  val sendTrackerStore: SendTrackerStore = SendTrackerStore()

  val submissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage,
      physicalSynchronizerIdx,
      parameters.stores.journalPruning.toInternal,
      timeouts,
      loggerFactory,
    )

  override val topologyStore =
    new DbTopologyStore(
      storage,
      SynchronizerStore(physicalSynchronizerIdx.synchronizerId),
      staticSynchronizerParameters.protocolVersion,
      timeouts,
      loggerFactory,
    )

  override val synchronizerOutboxQueue = new SynchronizerOutboxQueue(loggerFactory)

  override val topologyManager: SynchronizerTopologyManager = new SynchronizerTopologyManager(
    participantId.uid,
    clock = clock,
    crypto = crypto,
    staticSynchronizerParameters = staticSynchronizerParameters,
    store = topologyStore,
    outboxQueue = synchronizerOutboxQueue,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    timeouts = timeouts,
    futureSupervisor = futureSupervisor,
    loggerFactory = loggerFactory,
  ) with ParticipantTopologyValidation {

    override def validatePackageVetting(
        currentlyVettedPackages: Set[LfPackageId],
        nextPackageIds: Set[LfPackageId],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      validatePackageVetting(
        currentlyVettedPackages,
        nextPackageIds,
        Some(packageMetadataView.value),
        packageDependencyResolver,
        acsInspections =
          () => Map(logicalSyncPersistentState.lsid -> logicalSyncPersistentState.acsInspection),
        forceFlags,
        parameters.disableUpgradeValidation,
      )

    override def checkCannotDisablePartyWithActiveContracts(
        partyId: PartyId,
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkCannotDisablePartyWithActiveContracts(
        partyId,
        forceFlags,
        acsInspections =
          () => Map(logicalSyncPersistentState.lsid -> logicalSyncPersistentState.acsInspection),
      )

    override def checkInsufficientSignatoryAssigningParticipantsForParty(
        partyId: PartyId,
        currentThreshold: PositiveInt,
        nextThreshold: Option[PositiveInt],
        nextConfirmingParticipants: Seq[HostingParticipant],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkInsufficientSignatoryAssigningParticipantsForParty(
        partyId,
        currentThreshold,
        nextThreshold,
        nextConfirmingParticipants,
        forceFlags,
        () => Map(logicalSyncPersistentState.lsid -> logicalSyncPersistentState.reassignmentStore),
        () => ledgerApiStore.value.ledgerEnd,
      )

    override def checkInsufficientParticipantPermissionForSignatoryParty(
        partyId: PartyId,
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkInsufficientParticipantPermissionForSignatoryParty(
        partyId,
        forceFlags,
        acsInspections =
          () => Map(logicalSyncPersistentState.lsid -> logicalSyncPersistentState.acsInspection),
      )
  }

  override def close(): Unit =
    LifeCycle.close(
      topologyStore,
      topologyManager,
      sequencedEventStore,
      requestJournalStore,
      parameterStore,
      sendTrackerStore,
      submissionTrackerStore,
    )(logger)

  override def isMemory: Boolean = false
}
