// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.{
  SyncDomainPersistentState,
  SyncDomainPersistentStateOld,
  SyncDomainPersistentStateX,
}
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.memory.{
  InMemorySendTrackerStore,
  InMemorySequencedEventStore,
  InMemorySequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.memory.{InMemoryTopologyStore, InMemoryTopologyStoreX}
import com.digitalasset.canton.topology.{DomainOutboxQueue, DomainTopologyManagerX}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

abstract class InMemorySyncDomainPersistentStateCommon(
    override val domainId: IndexedDomain,
    override val pureCryptoApi: CryptoPureApi,
    override val enableAdditionalConsistencyChecks: Boolean,
    val loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState {

  val eventLog = new InMemorySingleDimensionEventLog(DomainEventLogId(domainId), loggerFactory)
  val contractStore = new InMemoryContractStore(loggerFactory)
  val activeContractStore = new InMemoryActiveContractStore(protocolVersion, loggerFactory)
  val contractKeyJournal = new InMemoryContractKeyJournal(loggerFactory)
  val transferStore = new InMemoryTransferStore(TargetDomainId(domainId.item), loggerFactory)
  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val acsCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
  val parameterStore = new InMemoryDomainParameterStore()
  val sequencerCounterTrackerStore =
    new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
  val sendTrackerStore = new InMemorySendTrackerStore()
  val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)

  override def isMemory(): Boolean = true

  override def close(): Unit = ()
}

class InMemorySyncDomainPersistentStateOld(
    domainId: IndexedDomain,
    val protocolVersion: ProtocolVersion,
    pureCryptoApi: CryptoPureApi,
    enableAdditionalConsistencyChecks: Boolean,
    loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends InMemorySyncDomainPersistentStateCommon(
      domainId,
      pureCryptoApi,
      enableAdditionalConsistencyChecks,
      loggerFactory,
      timeouts,
    )
    with SyncDomainPersistentStateOld {

  val topologyStore =
    new InMemoryTopologyStore(DomainStore(domainId.item), loggerFactory, timeouts, futureSupervisor)

}

class InMemorySyncDomainPersistentStateX(
    clock: Clock,
    crypto: Crypto,
    domainId: IndexedDomain,
    val protocolVersion: ProtocolVersion,
    enableAdditionalConsistencyChecks: Boolean,
    enableTopologyTransactionValidation: Boolean,
    loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends InMemorySyncDomainPersistentStateCommon(
      domainId,
      crypto.pureCrypto,
      enableAdditionalConsistencyChecks,
      loggerFactory,
      timeouts,
    )
    with SyncDomainPersistentStateX {

  override val topologyStore =
    new InMemoryTopologyStoreX(
      DomainStore(domainId.item),
      loggerFactory,
      timeouts,
    )

  override val domainOutboxQueue = new DomainOutboxQueue(loggerFactory)
  override val topologyManager = new DomainTopologyManagerX(
    clock,
    crypto,
    topologyStore,
    domainOutboxQueue,
    enableTopologyTransactionValidation,
    timeouts,
    futureSupervisor,
    loggerFactory,
  )

}
