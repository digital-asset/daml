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
import com.digitalasset.canton.participant.store.{AcsInspection, SyncDomainPersistentState}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.TargetDomainId
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
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class InMemorySyncDomainPersistentState(
    participantId: ParticipantId,
    clock: Clock,
    crypto: Crypto,
    override val domainId: IndexedDomain,
    val protocolVersion: ProtocolVersion,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    exitOnFatalFailures: Boolean,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  val contractStore = new InMemoryContractStore(loggerFactory)
  val activeContractStore =
    new InMemoryActiveContractStore(indexedStringStore, protocolVersion, loggerFactory)
  val transferStore = new InMemoryTransferStore(TargetDomainId(domainId.item), loggerFactory)
  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val acsCommitmentStore =
    new InMemoryAcsCommitmentStore(loggerFactory)
  val parameterStore = new InMemoryDomainParameterStore()
  val sendTrackerStore = new InMemorySendTrackerStore()
  val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)

  override val topologyStore =
    new InMemoryTopologyStore(
      DomainStore(domainId.item),
      loggerFactory,
      timeouts,
    )

  override val domainOutboxQueue = new DomainOutboxQueue(loggerFactory)
  override val topologyManager = new DomainTopologyManager(
    participantId.uid,
    clock,
    crypto,
    topologyStore,
    domainOutboxQueue,
    protocolVersion = protocolVersion,
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
        acsInspections = () => Map(domainId.domainId -> acsInspection),
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
        acsInspections = () => Map(domainId.domainId -> acsInspection),
      )
  }

  override def isMemory: Boolean = true

  override def close(): Unit = ()

  override def acsInspection: AcsInspection =
    new AcsInspection(domainId.domainId, activeContractStore, contractStore, ledgerApiStore)
}
