// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  ProcessingTimeout,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.CacheMetrics
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing
import com.digitalasset.canton.participant.sync.LogicalSynchronizerUpgradeCallback
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{PackageDependencyResolver, TopologyStore}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

class TopologyComponentFactory(
    psid: PhysicalSynchronizerId,
    crypto: SynchronizerCrypto,
    clock: Clock,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    topology: TopologyConfig,
    participantId: ParticipantId,
    unsafeOnlinePartyReplication: Option[UnsafeOnlinePartyReplicationConfig],
    exitOnFatalFailures: Boolean,
    topologyStore: TopologyStore[SynchronizerStore],
    topologyCacheMetrics: CacheMetrics,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext) {

  private val topologyStateCache = Option.when(topology.useNewProcessor)(
    new TopologyStateWriteThroughCache(
      topologyStore,
      batching.topologyCacheAggregator,
      cacheEvictionThreshold = topology.topologyStateCacheEvictionThreshold,
      maxCacheSize = topology.maxTopologyStateCacheItems,
      enableConsistencyChecks = topology.enableTopologyStateCacheConsistencyChecks,
      topologyCacheMetrics,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )
  )

  def createTopologyProcessorFactory(
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      sequencerConnectionSuccessorListener: SequencerConnectionSuccessorListener,
      topologyClient: SynchronizerTopologyClientWithInit,
      recordOrderPublisher: RecordOrderPublisher,
      lsuCallback: LogicalSynchronizerUpgradeCallback,
      sequencedEventStore: SequencedEventStore,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      ledgerApiStore: LedgerApiStore,
  ): TopologyTransactionProcessor.Factory = new TopologyTransactionProcessor.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[TopologyTransactionProcessor] = {

      val participantTerminateProcessing = new ParticipantTopologyTerminateProcessing(
        recordOrderPublisher,
        topologyStore,
        initialRecordTime = recordOrderPublisher.initTimestamp,
        participantId,
        pauseSynchronizerIndexingDuringPartyReplication = unsafeOnlinePartyReplication.nonEmpty,
        synchronizerPredecessor = synchronizerPredecessor,
        lsuCallback,
        loggerFactory,
      )
      val terminateTopologyProcessingFUS =
        for {
          topologyEventPublishedOnInitialRecordTime <- FutureUnlessShutdown.outcomeF(
            ledgerApiStore.topologyEventOffsetPublishedOnRecordTime(
              psid.logical,
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
          crypto.pureCrypto,
          topologyStore,
          topologyStateCache,
          topology,
          crypto.staticSynchronizerParameters,
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
        processor.subscribe(sequencerConnectionSuccessorListener)
        processor.subscribe(topologyClient)
        processor
      }
    }
  }

  def createInitialTopologySnapshotValidator()(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): InitialTopologySnapshotValidator =
    new InitialTopologySnapshotValidator(
      crypto.pureCrypto,
      topologyStore,
      batching.topologyCacheAggregator,
      topology,
      Some(crypto.staticSynchronizerParameters),
      timeouts,
      loggerFactory = loggerFactory,
    )

  def createTopologyClient(
      packageDependencyResolver: PackageDependencyResolver,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    if (topology.useNewClient)
      WriteThroughCacheSynchronizerTopologyClient.create(
        clock,
        crypto.staticSynchronizerParameters,
        topologyStore,
        topologyStateCache.getOrElse(
          throw new IllegalStateException("Cannot use new topology client without new processor")
        ),
        synchronizerUpgradeTime = synchronizerPredecessor.map(_.upgradeTime),
        packageDependencyResolver,
        caching,
        topology,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )()
    else
      CachingSynchronizerTopologyClient.create(
        clock,
        crypto.staticSynchronizerParameters,
        topologyStore,
        synchronizerPredecessor,
        packageDependencyResolver,
        caching,
        batching,
        topology,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )()

  def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencyResolver: PackageDependencyResolver,
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
