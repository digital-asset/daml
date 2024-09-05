// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{AcsInspection, SyncDomainPersistentState}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.{
  DomainOutboxQueue,
  DomainTopologyManager,
  ForceFlags,
  ParticipantId,
  PartyId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion

import scala.concurrent.ExecutionContext

class DbSyncDomainPersistentState(
    participantId: ParticipantId,
    override val domainId: IndexedDomain,
    val protocolVersion: ProtocolVersion,
    clock: Clock,
    storage: DbStorage,
    crypto: Crypto,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState
    with AutoCloseable
    with NoTracing {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  private val timeouts = parameters.processingTimeouts
  private val batching = parameters.batchingConfig
  private val caching = parameters.cachingConfigs

  override def enableAdditionalConsistencyChecks: Boolean =
    parameters.enableAdditionalConsistencyChecks

  val contractStore: DbContractStore =
    new DbContractStore(
      storage,
      domainId,
      protocolVersion,
      batching.maxItemsInSqlClause,
      caching.contractStore,
      dbQueryBatcherConfig = batching.aggregator,
      insertBatchAggregatorConfig = batching.aggregator,
      timeouts,
      loggerFactory,
    )
  val reassignmentStore: DbReassignmentStore = new DbReassignmentStore(
    storage,
    TargetDomainId(domainId.item),
    TargetProtocolVersion(protocolVersion),
    pureCryptoApi,
    futureSupervisor,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    timeouts,
    loggerFactory,
  )
  val activeContractStore: DbActiveContractStore =
    new DbActiveContractStore(
      storage,
      domainId,
      enableAdditionalConsistencyChecks,
      batching.maxItemsInSqlClause,
      parameters.stores.journalPruning.toInternal,
      indexedStringStore,
      protocolVersion,
      timeouts,
      loggerFactory,
    )
  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    domainId,
    protocolVersion,
    timeouts,
    loggerFactory,
  )
  val requestJournalStore: DbRequestJournalStore = new DbRequestJournalStore(
    domainId,
    storage,
    batching.maxItemsInSqlClause,
    insertBatchAggregatorConfig = batching.aggregator,
    replaceBatchAggregatorConfig = batching.aggregator,
    timeouts,
    loggerFactory,
  )
  val acsCommitmentStore = new DbAcsCommitmentStore(
    storage,
    domainId,
    protocolVersion,
    timeouts,
    futureSupervisor,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    loggerFactory,
  )

  val parameterStore: DbDomainParameterStore =
    new DbDomainParameterStore(domainId.item, storage, timeouts, loggerFactory)
  // TODO(i5660): Use the db-based send tracker store
  val sendTrackerStore = new InMemorySendTrackerStore()

  val submissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage,
      domainId,
      parameters.stores.journalPruning.toInternal,
      timeouts,
      loggerFactory,
    )

  override val topologyStore =
    new DbTopologyStore(
      storage,
      DomainStore(domainId.item),
      timeouts,
      loggerFactory,
    )

  override val domainOutboxQueue = new DomainOutboxQueue(loggerFactory)

  override val topologyManager = new DomainTopologyManager(
    participantId.uid,
    clock = clock,
    crypto = crypto,
    store = topologyStore,
    outboxQueue = domainOutboxQueue,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    protocolVersion = protocolVersion,
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

  override def close(): Unit =
    Lifecycle.close(
      topologyStore,
      topologyManager,
      contractStore,
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
    new AcsInspection(domainId.domainId, activeContractStore, contractStore, ledgerApiStore)
}
