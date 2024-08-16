// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.topology.ParticipantPackageVettingValidation
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbSequencedEventStore, DbSequencerCounterTrackerStore}
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
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}

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

  val eventLog: DbSingleDimensionEventLog[DomainEventLogId] = new DbSingleDimensionEventLog(
    DomainEventLogId(domainId),
    storage,
    indexedStringStore,
    ReleaseProtocolVersion.latest,
    timeouts,
    loggerFactory,
  )

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
  val transferStore: DbTransferStore = new DbTransferStore(
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
  val sequencerCounterTrackerStore =
    new DbSequencerCounterTrackerStore(domainId, storage, timeouts, loggerFactory)
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
  ) with ParticipantPackageVettingValidation {

    override def validatePackages(
        currentlyVettedPackages: Set[LfPackageId],
        nextPackageIds: Set[LfPackageId],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkPackageDependencies(
        currentlyVettedPackages,
        nextPackageIds,
        packageDependencyResolver,
        forceFlags,
      )
  }

  override def close(): Unit =
    Lifecycle.close(
      topologyStore,
      topologyManager,
      eventLog,
      contractStore,
      transferStore,
      activeContractStore,
      sequencedEventStore,
      requestJournalStore,
      acsCommitmentStore,
      parameterStore,
      sequencerCounterTrackerStore,
      sendTrackerStore,
      submissionTrackerStore,
    )(logger)

  override def isMemory: Boolean = false
}
