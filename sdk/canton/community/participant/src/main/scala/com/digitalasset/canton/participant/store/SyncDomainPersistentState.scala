// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.db.DbSyncDomainPersistentState
import com.digitalasset.canton.participant.store.memory.InMemorySyncDomainPersistentState
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.{
  ParticipantId,
  SynchronizerOutboxQueue,
  SynchronizerTopologyManager,
}

import scala.concurrent.ExecutionContext

/** The participant's relevant state and components that are bound to a particular synchronizer independent of the connectivity to the synchronizer */
trait SyncDomainPersistentState extends NamedLogging with AutoCloseable {

  protected[participant] def loggerFactory: NamedLoggerFactory

  /** The crypto operations used on the domain */
  def pureCryptoApi: CryptoPureApi
  def indexedSynchronizer: IndexedSynchronizer
  def staticSynchronizerParameters: StaticSynchronizerParameters
  def enableAdditionalConsistencyChecks: Boolean
  def reassignmentStore: ReassignmentStore
  def activeContractStore: ActiveContractStore
  def sequencedEventStore: SequencedEventStore
  def sendTrackerStore: SendTrackerStore
  def requestJournalStore: RequestJournalStore
  def acsCommitmentStore: AcsCommitmentStore
  def parameterStore: SynchronizerParameterStore
  def submissionTrackerStore: SubmissionTrackerStore
  def isMemory: Boolean

  def topologyStore: TopologyStore[SynchronizerStore]
  def topologyManager: SynchronizerTopologyManager
  def synchronizerOutboxQueue: SynchronizerOutboxQueue
  def acsInspection: AcsInspection
}

object SyncDomainPersistentState {

  def create(
      participantId: ParticipantId,
      storage: Storage,
      synchronizerIdx: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      clock: Clock,
      crypto: Crypto,
      parameters: ParticipantNodeParameters,
      indexedStringStore: IndexedStringStore,
      acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
      packageDependencyResolver: PackageDependencyResolver,
      ledgerApiStore: Eval[LedgerApiStore],
      contractStore: Eval[ContractStore],
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit ec: ExecutionContext): SyncDomainPersistentState = {
    val domainLoggerFactory =
      loggerFactory.append("synchronizerId", synchronizerIdx.synchronizerId.toString)
    storage match {
      case _: MemoryStorage =>
        new InMemorySyncDomainPersistentState(
          participantId,
          clock,
          crypto,
          synchronizerIdx,
          staticSynchronizerParameters,
          parameters.enableAdditionalConsistencyChecks,
          indexedStringStore,
          contractStore.value,
          acsCounterParticipantConfigStore,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          packageDependencyResolver,
          ledgerApiStore,
          domainLoggerFactory,
          parameters.processingTimeouts,
          futureSupervisor,
        )
      case db: DbStorage =>
        new DbSyncDomainPersistentState(
          participantId,
          synchronizerIdx,
          staticSynchronizerParameters,
          clock,
          db,
          crypto,
          parameters,
          indexedStringStore,
          contractStore.value,
          acsCounterParticipantConfigStore,
          packageDependencyResolver,
          ledgerApiStore,
          domainLoggerFactory,
          futureSupervisor,
        )
    }
  }

}
