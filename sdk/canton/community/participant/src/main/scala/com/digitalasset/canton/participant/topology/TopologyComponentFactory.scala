// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{PackageDependencyResolverUS, TopologyStore}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class TopologyComponentFactory(
    synchronizerId: SynchronizerId,
    protocolVersion: ProtocolVersion,
    crypto: SynchronizerCrypto,
    clock: Clock,
    ips: IdentityProvidingServiceClient,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    participantId: ParticipantId,
    unsafeOnlinePartyReplication: Option[UnsafeOnlinePartyReplicationConfig],
    exitOnFatalFailures: Boolean,
    topologyStore: TopologyStore[SynchronizerStore],
    loggerFactory: NamedLoggerFactory,
) {
  // TODO(#25483) synchronizerId of this class should be physical
  val psid = PhysicalSynchronizerId(synchronizerId, protocolVersion)

  def createTopologyProcessorFactory(
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      topologyClient: SynchronizerTopologyClientWithInit,
      recordOrderPublisher: RecordOrderPublisher,
      sequencedEventStore: SequencedEventStore,
      ledgerApiStore: LedgerApiStore,
  ): TopologyTransactionProcessor.Factory = new TopologyTransactionProcessor.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[TopologyTransactionProcessor] = {

      val participantTerminateProcessing = new ParticipantTopologyTerminateProcessing(
        psid,
        protocolVersion,
        recordOrderPublisher,
        topologyStore,
        recordOrderPublisher.initTimestamp,
        participantId,
        unsafeOnlinePartyReplication.exists(_.pauseSynchronizerIndexingDuringPartyReplication),
        loggerFactory,
      )
      val terminateTopologyProcessingFUS =
        for {
          topologyEventPublishedOnInitialRecordTime <- FutureUnlessShutdown.outcomeF(
            ledgerApiStore.topologyEventOffsetPublishedOnRecordTime(
              synchronizerId,
              recordOrderPublisher.initTimestamp,
            )
          )
          _ <- participantTerminateProcessing.scheduleMissingTopologyEventsAtInitialization(
            topologyEventPublishedOnInitialRecordTime =
              topologyEventPublishedOnInitialRecordTime.isDefined,
            traceContextForSequencedEvent = sequencedEventStore.traceContext(_),
            parallelism = batching.parallelism,
          )
        } yield participantTerminateProcessing

      terminateTopologyProcessingFUS.map { terminateTopologyProcessing =>
        val processor = new TopologyTransactionProcessor(
          synchronizerId,
          crypto.pureCrypto,
          topologyStore,
          acsCommitmentScheduleEffectiveTime,
          terminateTopologyProcessing,
          futureSupervisor,
          exitOnFatalFailures = exitOnFatalFailures,
          timeouts,
          loggerFactory,
        )
        // subscribe party notifier to topology processor
        processor.subscribe(partyNotifier.attachToTopologyProcessor())
        processor.subscribe(missingKeysAlerter.attachToTopologyProcessor())
        processor.subscribe(topologyClient)
        processor
      }
    }
  }

  def createInitialTopologySnapshotValidator(implicit
      executionContext: ExecutionContext
  ): InitialTopologySnapshotValidator =
    new InitialTopologySnapshotValidator(
      protocolVersion,
      crypto.pureCrypto,
      topologyStore,
      timeouts,
      loggerFactory,
    )

  def createTopologyClient(
      packageDependencyResolver: PackageDependencyResolverUS
  )(implicit executionContext: ExecutionContext): SynchronizerTopologyClientWithInit =
    new StoreBasedSynchronizerTopologyClient(
      clock,
      psid,
      topologyStore,
      packageDependencyResolver,
      new IdentityProvidingServiceClient(),
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  def createCachingTopologyClient(
      packageDependencyResolver: PackageDependencyResolverUS
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    CachingSynchronizerTopologyClient.create(
      clock,
      psid,
      topologyStore,
      packageDependencyResolver,
      ips,
      caching,
      batching,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )()

  def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencyResolver: PackageDependencyResolverUS,
      preferCaching: Boolean,
  )(implicit executionContext: ExecutionContext): TopologySnapshot = {
    val snapshot = new StoreBasedTopologySnapshot(
      asOf,
      topologyStore,
      packageDependencyResolver,
      loggerFactory,
    )
    if (preferCaching) {
      new CachingTopologySnapshot(snapshot, caching, batching, loggerFactory, futureSupervisor)
    } else
      snapshot
  }

  def createHeadTopologySnapshot()(implicit
      executionContext: ExecutionContext
  ): TopologySnapshot =
    createTopologySnapshot(
      CantonTimestamp.MaxValue,
      StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
      preferCaching = false,
    )
}
