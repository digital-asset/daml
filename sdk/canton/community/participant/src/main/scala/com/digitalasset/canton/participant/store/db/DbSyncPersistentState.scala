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
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
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

class DbSyncPersistentState(
    participantId: ParticipantId,
    override val indexedSynchronizer: IndexedSynchronizer,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    clock: Clock,
    storage: DbStorage,
    crypto: SynchronizerCrypto,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncPersistentState
    with AutoCloseable
    with NoTracing {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  private val timeouts = parameters.processingTimeouts
  private val batching = parameters.batchingConfig

  override def enableAdditionalConsistencyChecks: Boolean =
    parameters.enableAdditionalConsistencyChecks

  val reassignmentStore: DbReassignmentStore = new DbReassignmentStore(
    storage,
    ReassignmentTag.Target(indexedSynchronizer),
    indexedStringStore,
    ReassignmentTag.Target(staticSynchronizerParameters.protocolVersion),
    pureCryptoApi,
    futureSupervisor,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    parameters.batchingConfig,
    timeouts,
    loggerFactory,
  )
  val activeContractStore: DbActiveContractStore =
    new DbActiveContractStore(
      storage,
      indexedSynchronizer,
      enableAdditionalConsistencyChecks,
      parameters.stores.journalPruning.toInternal,
      indexedStringStore,
      timeouts,
      loggerFactory,
    )
  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    indexedSynchronizer,
    staticSynchronizerParameters.protocolVersion,
    timeouts,
    loggerFactory,
  )
  val requestJournalStore: DbRequestJournalStore = new DbRequestJournalStore(
    indexedSynchronizer,
    storage,
    insertBatchAggregatorConfig = batching.aggregator,
    replaceBatchAggregatorConfig = batching.aggregator,
    timeouts,
    loggerFactory,
  )
  val acsCommitmentStore = new DbAcsCommitmentStore(
    storage,
    indexedSynchronizer,
    acsCounterParticipantConfigStore,
    staticSynchronizerParameters.protocolVersion,
    timeouts,
    loggerFactory,
  )

  val parameterStore: DbSynchronizerParameterStore =
    new DbSynchronizerParameterStore(
      indexedSynchronizer.synchronizerId,
      storage,
      timeouts,
      loggerFactory,
    )
  // TODO(i5660): Use the db-based send tracker store
  val sendTrackerStore = new InMemorySendTrackerStore()

  val submissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage,
      indexedSynchronizer,
      parameters.stores.journalPruning.toInternal,
      timeouts,
      loggerFactory,
    )

  override val topologyStore =
    new DbTopologyStore(
      storage,
      SynchronizerStore(indexedSynchronizer.synchronizerId),
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
        packageDependencyResolver,
        acsInspections = () => Map(indexedSynchronizer.synchronizerId -> acsInspection),
        forceFlags,
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
        acsInspections = () => Map(indexedSynchronizer.synchronizerId -> acsInspection),
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
        () => Map(indexedSynchronizer.synchronizerId -> reassignmentStore),
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
        acsInspections = () => Map(indexedSynchronizer.synchronizerId -> acsInspection),
      )
  }

  override def close(): Unit =
    LifeCycle.close(
      topologyStore,
      topologyManager,
      reassignmentStore,
      activeContractStore,
      sequencedEventStore,
      requestJournalStore,
      acsCommitmentStore,
      parameterStore,
      sendTrackerStore,
      submissionTrackerStore,
    )(logger)

  override def isMemory: Boolean = false

  override def acsInspection: AcsInspection =
    new AcsInspection(
      indexedSynchronizer.synchronizerId,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )
}
