// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.db.{
  DbLogicalSyncPersistentState,
  DbPhysicalSyncPersistentState,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryLogicalSyncPersistentState,
  InMemoryPhysicalSyncPersistentState,
  PackageMetadataView,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.{
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  SynchronizerOutboxQueue,
  SynchronizerTopologyManager,
}

import scala.concurrent.ExecutionContext

/** The participant-relevant state and components of a synchronizer that is independent of the
  * connectivity to the synchronizer.
  */
class SyncPersistentState(
    val logical: LogicalSyncPersistentState,
    val physical: PhysicalSyncPersistentState,
    override protected val loggerFactory: NamedLoggerFactory,
) extends LogicalSyncPersistentState
    with PhysicalSyncPersistentState
    with NamedLogging
    with AutoCloseable {

  override def synchronizerIdx: IndexedSynchronizer = logical.synchronizerIdx
  override def enableAdditionalConsistencyChecks: Boolean =
    logical.enableAdditionalConsistencyChecks
  override def activeContractStore: ActiveContractStore = logical.activeContractStore
  override def acsInspection: AcsInspection = logical.acsInspection
  override def acsCommitmentStore: AcsCommitmentStore = logical.acsCommitmentStore
  override def reassignmentStore: ReassignmentStore = logical.reassignmentStore

  override def pureCryptoApi: CryptoPureApi = physical.pureCryptoApi

  override def physicalSynchronizerIdx: IndexedPhysicalSynchronizer =
    physical.physicalSynchronizerIdx
  override def staticSynchronizerParameters: StaticSynchronizerParameters =
    physical.staticSynchronizerParameters
  override def sequencedEventStore: SequencedEventStore = physical.sequencedEventStore
  override def sendTrackerStore: SendTrackerStore = physical.sendTrackerStore
  override def requestJournalStore: RequestJournalStore = physical.requestJournalStore
  override def parameterStore: SynchronizerParameterStore = physical.parameterStore
  override def submissionTrackerStore: SubmissionTrackerStore = physical.submissionTrackerStore
  override def isMemory: Boolean = physical.isMemory
  override def topologyStore: TopologyStore[SynchronizerStore] = physical.topologyStore
  override def topologyManager: SynchronizerTopologyManager = physical.topologyManager
  override def synchronizerOutboxQueue: SynchronizerOutboxQueue = physical.synchronizerOutboxQueue

  override def close(): Unit =
    LifeCycle.close(
      logical,
      physical,
    )(logger)
}

/** Stores that are logical, meaning they can span across multiple physical synchronizers, and
  * therefore are not tied to a specific protocol version.
  */
trait LogicalSyncPersistentState extends NamedLogging with AutoCloseable {

  def synchronizerIdx: IndexedSynchronizer
  lazy val lsid: SynchronizerId = synchronizerIdx.synchronizerId

  def enableAdditionalConsistencyChecks: Boolean

  def activeContractStore: ActiveContractStore
  def acsInspection: AcsInspection
  def acsCommitmentStore: AcsCommitmentStore
  def reassignmentStore: ReassignmentStore

}

/** Stores that tied to a specific physical synchronizer. */
trait PhysicalSyncPersistentState extends NamedLogging with AutoCloseable {

  /** The crypto operations used on the synchronizer */
  def pureCryptoApi: CryptoPureApi
  def physicalSynchronizerIdx: IndexedPhysicalSynchronizer
  def staticSynchronizerParameters: StaticSynchronizerParameters

  def sequencedEventStore: SequencedEventStore
  def sendTrackerStore: SendTrackerStore
  def requestJournalStore: RequestJournalStore

  def parameterStore: SynchronizerParameterStore
  def submissionTrackerStore: SubmissionTrackerStore
  def isMemory: Boolean

  def topologyStore: TopologyStore[SynchronizerStore]
  def topologyManager: SynchronizerTopologyManager
  def synchronizerOutboxQueue: SynchronizerOutboxQueue

  lazy val psid: PhysicalSynchronizerId = physicalSynchronizerIdx.synchronizerId
}

object LogicalSyncPersistentState {
  def create(
      storage: Storage,
      synchronizerIdx: IndexedSynchronizer,
      parameters: ParticipantNodeParameters,
      indexedStringStore: IndexedStringStore,
      acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
      ledgerApiStore: Eval[LedgerApiStore],
      contractStore: Eval[ContractStore],
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext): LogicalSyncPersistentState =
    storage match {
      case _: MemoryStorage =>
        new InMemoryLogicalSyncPersistentState(
          synchronizerIdx,
          parameters.enableAdditionalConsistencyChecks,
          indexedStringStore,
          contractStore.value,
          acsCounterParticipantConfigStore,
          ledgerApiStore,
          loggerFactory,
        )
      case db: DbStorage =>
        new DbLogicalSyncPersistentState(
          synchronizerIdx,
          db,
          parameters,
          indexedStringStore,
          acsCounterParticipantConfigStore,
          contractStore.value,
          ledgerApiStore,
          loggerFactory,
          futureSupervisor,
        )
    }

}

object PhysicalSyncPersistentState {
  def create(
      participantId: ParticipantId,
      storage: Storage,
      physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
      indexedTopologyStoreId: IndexedTopologyStoreId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      clock: Clock,
      crypto: SynchronizerCrypto,
      parameters: ParticipantNodeParameters,
      packageMetadataView: PackageMetadataView,
      ledgerApiStore: Eval[LedgerApiStore],
      logicalSyncPersistentState: LogicalSyncPersistentState,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext): PhysicalSyncPersistentState =
    storage match {
      case _: MemoryStorage =>
        new InMemoryPhysicalSyncPersistentState(
          participantId,
          clock,
          crypto,
          physicalSynchronizerIdx,
          staticSynchronizerParameters,
          parameters,
          packageMetadataView,
          ledgerApiStore,
          logicalSyncPersistentState,
          loggerFactory,
          parameters.processingTimeouts,
          futureSupervisor,
        )
      case db: DbStorage =>
        new DbPhysicalSyncPersistentState(
          participantId,
          physicalSynchronizerIdx,
          indexedTopologyStoreId,
          staticSynchronizerParameters,
          clock,
          db,
          crypto,
          parameters,
          packageMetadataView,
          ledgerApiStore,
          logicalSyncPersistentState,
          loggerFactory,
          futureSupervisor,
        )
    }

}
