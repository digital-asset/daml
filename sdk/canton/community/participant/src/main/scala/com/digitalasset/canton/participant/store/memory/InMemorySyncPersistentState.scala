// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.HostingParticipant
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  SynchronizerOutboxQueue,
  SynchronizerTopologyManager,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

class InMemoryLogicalSyncPersistentState(
    override val synchronizerIdx: IndexedSynchronizer,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LogicalSyncPersistentState {

  override val activeContractStore =
    new InMemoryActiveContractStore(
      indexedStringStore,
      loggerFactory,
    )

  val acsCommitmentStore =
    new InMemoryAcsCommitmentStore(
      synchronizerIdx.synchronizerId,
      acsCounterParticipantConfigStore,
      loggerFactory,
    )

  override val acsInspection: AcsInspection =
    new AcsInspection(
      synchronizerIdx.synchronizerId,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )

  override val reassignmentStore =
    new InMemoryReassignmentStore(Target(synchronizerIdx.item), loggerFactory)

  override def close(): Unit = ()
}

class InMemoryPhysicalSyncPersistentState(
    participantId: ParticipantId,
    clock: Clock,
    crypto: SynchronizerCrypto,
    override val physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    parameters: ParticipantNodeParameters,
    packageMetadataView: PackageMetadataView,
    ledgerApiStore: Eval[LedgerApiStore],
    logicalSyncPersistentState: LogicalSyncPersistentState,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends PhysicalSyncPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val parameterStore = new InMemorySynchronizerParameterStore()
  val sendTrackerStore = new InMemorySendTrackerStore()
  val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)

  override val topologyStore =
    new InMemoryTopologyStore(
      SynchronizerStore(physicalSynchronizerIdx.synchronizerId),
      staticSynchronizerParameters.protocolVersion,
      loggerFactory,
      timeouts,
    )

  override val synchronizerOutboxQueue = new SynchronizerOutboxQueue(loggerFactory)
  override val topologyManager: SynchronizerTopologyManager = new SynchronizerTopologyManager(
    participantId.uid,
    clock,
    crypto,
    staticSynchronizerParameters,
    topologyStore,
    synchronizerOutboxQueue,
    disableOptionalTopologyChecks = parameters.disableOptionalTopologyChecks,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    timeouts,
    futureSupervisor,
    loggerFactory,
  ) with ParticipantTopologyValidation {

    override def validatePackageVetting(
        currentlyVettedPackages: Set[LfPackageId],
        nextPackageIds: Set[LfPackageId],
        dryRunSnapshot: Option[PackageMetadata],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      validatePackageVetting(
        currentlyVettedPackages,
        nextPackageIds,
        packageMetadataView,
        dryRunSnapshot,
        acsInspections =
          () => Map(logicalSyncPersistentState.lsid -> logicalSyncPersistentState.acsInspection),
        forceFlags,
        disableUpgradeValidation = parameters.disableUpgradeValidation,
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

  override def isMemory: Boolean = true

  override def close(): Unit = ()

}
