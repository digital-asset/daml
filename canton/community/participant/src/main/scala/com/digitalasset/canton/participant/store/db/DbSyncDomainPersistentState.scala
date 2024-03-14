// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{
  DbSequencedEventStore,
  DbSequencerCounterTrackerStore,
  SequencerClientDiscriminator,
}
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.db.DbTopologyStoreX
import com.digitalasset.canton.topology.{DomainOutboxQueue, DomainTopologyManagerX}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.version.Transfer.TargetProtocolVersion
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}

import scala.concurrent.ExecutionContext

class DbSyncDomainPersistentState(
    override val domainId: IndexedDomain,
    val protocolVersion: ProtocolVersion,
    clock: Clock,
    storage: DbStorage,
    crypto: Crypto,
    parameters: ParticipantStoreConfig,
    val caching: CachingConfigs,
    val batching: BatchingConfig,
    val timeouts: ProcessingTimeout,
    override val enableAdditionalConsistencyChecks: Boolean,
    enableTopologyTransactionValidation: Boolean,
    indexedStringStore: IndexedStringStore,
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState
    with AutoCloseable
    with NoTracing {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

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
    timeouts,
    loggerFactory,
  )
  val activeContractStore: DbActiveContractStore =
    new DbActiveContractStore(
      storage,
      domainId,
      enableAdditionalConsistencyChecks,
      batching.maxItemsInSqlClause,
      parameters.journalPruning.toInternal,
      indexedStringStore,
      protocolVersion,
      timeouts,
      loggerFactory,
    )
  private val client = SequencerClientDiscriminator.fromIndexedDomainId(domainId)
  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    client,
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
    pureCryptoApi,
    timeouts,
    futureSupervisor,
    loggerFactory,
  )

  val parameterStore: DbDomainParameterStore =
    new DbDomainParameterStore(domainId.item, storage, timeouts, loggerFactory)
  val sequencerCounterTrackerStore =
    new DbSequencerCounterTrackerStore(client, storage, timeouts, loggerFactory)
  // TODO(i5660): Use the db-based send tracker store
  val sendTrackerStore = new InMemorySendTrackerStore()

  val submissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage,
      domainId,
      parameters.journalPruning.toInternal,
      timeouts,
      loggerFactory,
    )

  override val topologyStore =
    new DbTopologyStoreX(
      storage,
      DomainStore(domainId.item),
      timeouts,
      loggerFactory,
    )

  override val domainOutboxQueue = new DomainOutboxQueue(loggerFactory)

  override val topologyManager = new DomainTopologyManagerX(
    clock = clock,
    crypto = crypto,
    store = topologyStore,
    outboxQueue = domainOutboxQueue,
    enableTopologyTransactionValidation,
    timeouts = timeouts,
    futureSupervisor = futureSupervisor,
    loggerFactory = loggerFactory,
  )

  override def close(): Unit = {
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
    )(logger)
  }

  override def isMemory: Boolean = false
}
