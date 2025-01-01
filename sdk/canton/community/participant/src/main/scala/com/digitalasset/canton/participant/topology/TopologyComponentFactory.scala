// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Crypto, DomainCryptoPureApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.protocol.{
  ParticipantTopologyTerminateProcessing,
  ParticipantTopologyTerminateProcessingTicker,
}
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{PackageDependencyResolverUS, TopologyStore}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class TopologyComponentFactory(
    synchronizerId: SynchronizerId,
    protocolVersion: ProtocolVersion,
    crypto: Crypto,
    clock: Clock,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    participantId: ParticipantId,
    unsafeEnableOnlinePartyReplication: Boolean,
    exitOnFatalFailures: Boolean,
    topologyStore: TopologyStore[DomainStore],
    loggerFactory: NamedLoggerFactory,
) {

  def createTopologyProcessorFactory(
      staticDomainParameters: StaticDomainParameters,
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      topologyClient: DomainTopologyClientWithInit,
      recordOrderPublisher: RecordOrderPublisher,
      sequencedEventStore: SequencedEventStore,
      ledgerApiStore: LedgerApiStore,
      experimentalEnableTopologyEvents: Boolean,
  ): TopologyTransactionProcessor.Factory = new TopologyTransactionProcessor.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[TopologyTransactionProcessor] = {

      val terminateTopologyProcessingFUS =
        if (experimentalEnableTopologyEvents) {
          val participantTerminateProcessing = new ParticipantTopologyTerminateProcessing(
            synchronizerId,
            protocolVersion,
            recordOrderPublisher,
            topologyStore,
            recordOrderPublisher.initTimestamp,
            participantId,
            unsafeEnableOnlinePartyReplication = unsafeEnableOnlinePartyReplication,
            loggerFactory,
          )
          for {
            topologyEventPublishedOnInitialRecordTime <- FutureUnlessShutdown.outcomeF(
              ledgerApiStore.topologyEventPublishedOnRecordTime(
                synchronizerId,
                recordOrderPublisher.initTimestamp,
              )
            )
            _ <- participantTerminateProcessing.scheduleMissingTopologyEventsAtInitialization(
              topologyEventPublishedOnInitialRecordTime = topologyEventPublishedOnInitialRecordTime,
              traceContextForSequencedEvent = sequencedEventStore.traceContext(_),
              parallelism = batching.parallelism.value,
            )
          } yield participantTerminateProcessing
        } else {
          FutureUnlessShutdown.pure(
            new ParticipantTopologyTerminateProcessingTicker(loggerFactory)
          )
        }

      terminateTopologyProcessingFUS.map { terminateTopologyProcessing =>
        val processor = new TopologyTransactionProcessor(
          synchronizerId,
          new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto),
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

  def createInitialTopologySnapshotValidator(staticDomainParameters: StaticDomainParameters)(
      implicit executionContext: ExecutionContext
  ): InitialTopologySnapshotValidator =
    new InitialTopologySnapshotValidator(
      synchronizerId,
      new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto),
      topologyStore,
      timeouts,
      loggerFactory,
    )

  def createTopologyClient(
      packageDependencyResolver: PackageDependencyResolverUS
  )(implicit executionContext: ExecutionContext): DomainTopologyClientWithInit =
    new StoreBasedDomainTopologyClient(
      clock,
      synchronizerId,
      topologyStore,
      packageDependencyResolver,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  def createCachingTopologyClient(
      packageDependencyResolver: PackageDependencyResolverUS
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[DomainTopologyClientWithInit] = CachingDomainTopologyClient.create(
    clock,
    synchronizerId,
    topologyStore,
    packageDependencyResolver,
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
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      preferCaching = false,
    )
}
