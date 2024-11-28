// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.{
  DomainOutboxQueue,
  DomainTopologyManager,
  ForceFlags,
  ParticipantId,
  PartyId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

class InMemorySyncDomainPersistentState(
    participantId: ParticipantId,
    clock: Clock,
    crypto: Crypto,
    override val indexedDomain: IndexedDomain,
    val staticDomainParameters: StaticDomainParameters,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    exitOnFatalFailures: Boolean,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  val activeContractStore =
    new InMemoryActiveContractStore(
      indexedStringStore,
      loggerFactory,
    )
  val reassignmentStore =
    new InMemoryReassignmentStore(Target(indexedDomain.item), loggerFactory)
  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val acsCommitmentStore =
    new InMemoryAcsCommitmentStore(
      indexedDomain.domainId,
      acsCounterParticipantConfigStore,
      loggerFactory,
    )
  val parameterStore = new InMemoryDomainParameterStore()
  val sendTrackerStore = new InMemorySendTrackerStore()
  val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)

  override val topologyStore =
    new InMemoryTopologyStore(
      DomainStore(indexedDomain.domainId),
      loggerFactory,
      timeouts,
    )

  override val domainOutboxQueue = new DomainOutboxQueue(loggerFactory)
  override val topologyManager: DomainTopologyManager = new DomainTopologyManager(
    participantId.uid,
    clock,
    crypto,
    staticDomainParameters,
    topologyStore,
    domainOutboxQueue,
    exitOnFatalFailures = exitOnFatalFailures,
    timeouts,
    futureSupervisor,
    loggerFactory,
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
        acsInspections = () => Map(indexedDomain.domainId -> acsInspection),
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
        acsInspections = () => Map(indexedDomain.domainId -> acsInspection),
      )
  }

  override def isMemory: Boolean = true

  override def close(): Unit = ()

  override def acsInspection: AcsInspection =
    new AcsInspection(indexedDomain.domainId, activeContractStore, contractStore, ledgerApiStore)
}
