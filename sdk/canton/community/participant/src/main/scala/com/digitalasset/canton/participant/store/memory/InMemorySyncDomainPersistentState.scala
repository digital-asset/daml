// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.topology.ParticipantPackageVettingValidation
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.store.memory.{
  InMemorySendTrackerStore,
  InMemorySequencedEventStore,
  InMemorySequencerCounterTrackerStore,
}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.{
  DomainOutboxQueue,
  DomainTopologyManager,
  ForceFlags,
  ParticipantId,
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
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  val eventLog = new InMemorySingleDimensionEventLog(DomainEventLogId(domainId), loggerFactory)
  val contractStore = new InMemoryContractStore(loggerFactory)
  val activeContractStore =
    new InMemoryActiveContractStore(indexedStringStore, protocolVersion, loggerFactory)
  val transferStore = new InMemoryTransferStore(TargetDomainId(domainId.item), loggerFactory)
  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val acsCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
  val parameterStore = new InMemoryDomainParameterStore()
  val sequencerCounterTrackerStore =
    new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
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
  ) with ParticipantPackageVettingValidation {

    override def validatePackages(
        currentlyVettedPackages: Set[LfPackageId],
        nextPackageIds: Set[LfPackageId],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      validate(
        currentlyVettedPackages,
        nextPackageIds,
        packageDependencyResolver,
        contractStores = () => Map(domainId.domainId -> (activeContractStore, contractStore)),
        forceFlags,
      )
  }

  override def isMemory: Boolean = true

  override def close(): Unit = ()
}
