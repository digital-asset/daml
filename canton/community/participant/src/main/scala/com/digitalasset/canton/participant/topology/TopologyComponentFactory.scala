// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  ProcessingTimeout,
  TopologyXConfig,
}
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessingTickerX
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.traffic.{
  TrafficStateController,
  TrafficStateTopUpSubscription,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{
  DomainTopologyTransactionMessageValidator,
  EffectiveTime,
  TopologyTransactionProcessor,
  TopologyTransactionProcessorCommon,
  TopologyTransactionProcessorX,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreX}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait TopologyComponentFactory {

  def createTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit executionContext: ExecutionContext): DomainTopologyClientWithInit

  def createCachingTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainTopologyClientWithInit]

  def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      preferCaching: Boolean,
  )(implicit executionContext: ExecutionContext): TopologySnapshot

  def createHeadTopologySnapshot()(implicit
      executionContext: ExecutionContext
  ): TopologySnapshot =
    createTopologySnapshot(
      CantonTimestamp.MaxValue,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      preferCaching = false,
    )

  def createTopologyProcessorFactory(
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      topologyClient: DomainTopologyClientWithInit,
      // this is the client above, wrapped with some crypto methods, but only the base client is accessible, so we
      // need to pass both.
      // TODO(#15208) remove me with 3.0
      syncCrypto: DomainSyncCryptoClient,
      trafficStateController: TrafficStateController,
      recordOrderPublisher: RecordOrderPublisher,
      protocolVersion: ProtocolVersion,
  ): TopologyTransactionProcessorCommon.Factory

}

class TopologyComponentFactoryOld(
    participantId: ParticipantId,
    domainId: DomainId,
    clock: Clock,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    topologyStore: TopologyStore[DomainStore],
    loggerFactory: NamedLoggerFactory,
) extends TopologyComponentFactory {

  override def createTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit executionContext: ExecutionContext): DomainTopologyClientWithInit = {
    new StoreBasedDomainTopologyClient(
      clock,
      domainId = domainId,
      protocolVersion = protocolVersion,
      store = topologyStore,
      initKeys = Map(),
      packageDependencies = packageDependencies,
      timeouts,
      futureSupervisor,
      loggerFactory,
      useStateTxs = false,
    )
  }

  override def createCachingTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainTopologyClientWithInit] =
    CachingDomainTopologyClient.create(
      clock,
      domainId,
      protocolVersion,
      topologyStore,
      Map(),
      packageDependencies,
      caching,
      batching,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  override def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      preferCaching: Boolean,
  )(implicit executionContext: ExecutionContext): TopologySnapshot = {
    val snapshot = new StoreBasedTopologySnapshot(
      asOf,
      topologyStore,
      Map(),
      useStateTxs = true,
      packageDependencies,
      loggerFactory,
    )
    if (preferCaching) {
      new CachingTopologySnapshot(snapshot, caching, batching, loggerFactory)
    } else
      snapshot
  }

  override def createTopologyProcessorFactory(
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      topologyClient: DomainTopologyClientWithInit,
      syncCrypto: DomainSyncCryptoClient,
      trafficStateController: TrafficStateController,
      recordOrderPublisher: RecordOrderPublisher,
      protocolVersion: ProtocolVersion,
  ): TopologyTransactionProcessorCommon.Factory =
    new TopologyTransactionProcessorCommon.Factory {
      override def create(
          acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
      )(implicit executionContext: ExecutionContext): TopologyTransactionProcessorCommon = {

        val processor = new TopologyTransactionProcessor(
          domainId,
          DomainTopologyTransactionMessageValidator
            .create(
              syncCrypto,
              participantId,
              protocolVersion,
              timeouts,
              futureSupervisor,
              loggerFactory,
            ),
          syncCrypto.pureCrypto,
          topologyStore,
          acsCommitmentScheduleEffectiveTime,
          futureSupervisor,
          timeouts,
          loggerFactory,
        )
        // subscribe party notifier to topology processor
        processor.subscribe(partyNotifier.attachToTopologyProcessorOld())
        processor.subscribe(missingKeysAlerter.attachToTopologyProcessorOld())
        // TODO(#14048) this is an ugly hack, but I don't know where we could create the individual components
        //              and have the types align :(
        topologyClient match {
          case old: DomainTopologyClientWithInitOld =>
            processor.subscribe(old)
          case _ =>
            throw new IllegalStateException("passed wrong type. coding bug")
        }
        processor
      }
    }

}

class TopologyComponentFactoryX(
    domainId: DomainId,
    crypto: Crypto,
    clock: Clock,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    topologyXConfig: TopologyXConfig,
    topologyStore: TopologyStoreX[DomainStore],
    loggerFactory: NamedLoggerFactory,
) extends TopologyComponentFactory {

  override def createTopologyProcessorFactory(
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      topologyClient: DomainTopologyClientWithInit,
      syncCrypto: DomainSyncCryptoClient,
      trafficStateController: TrafficStateController,
      recordOrderPublisher: RecordOrderPublisher,
      protocolVersion: ProtocolVersion,
  ): TopologyTransactionProcessorCommon.Factory = new TopologyTransactionProcessorCommon.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit executionContext: ExecutionContext): TopologyTransactionProcessorCommon = {

      val terminateTopologyProcessing = new ParticipantTopologyTerminateProcessingTickerX(
        recordOrderPublisher,
        loggerFactory,
      )

      val processor = new TopologyTransactionProcessorX(
        domainId,
        crypto,
        topologyStore,
        acsCommitmentScheduleEffectiveTime,
        terminateTopologyProcessing,
        topologyXConfig.enableTopologyTransactionValidation,
        futureSupervisor,
        timeouts,
        loggerFactory,
      )
      // subscribe party notifier to topology processor
      processor.subscribe(partyNotifier.attachToTopologyProcessorX())
      processor.subscribe(missingKeysAlerter.attachToTopologyProcessorX())
      // TODO(#14048) this is an ugly hack, but I don't know where we could create the individual components
      //              and have the types align :(
      topologyClient match {
        case x: DomainTopologyClientWithInitX =>
          processor.subscribe(x)
        case _ =>
          throw new IllegalStateException("passed wrong type. coding bug")
      }
      processor.subscribe(new TrafficStateTopUpSubscription(trafficStateController, loggerFactory))
      processor
    }
  }

  override def createTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit executionContext: ExecutionContext): DomainTopologyClientWithInit =
    new StoreBasedDomainTopologyClientX(
      clock,
      domainId,
      protocolVersion,
      topologyStore,
      packageDependencies,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  override def createCachingTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainTopologyClientWithInit] = CachingDomainTopologyClient.createX(
    clock,
    domainId,
    protocolVersion,
    topologyStore,
    packageDependencies,
    caching,
    batching,
    timeouts,
    futureSupervisor,
    loggerFactory,
  )

  override def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      preferCaching: Boolean,
  )(implicit executionContext: ExecutionContext): TopologySnapshot = {
    val snapshot = new StoreBasedTopologySnapshotX(
      asOf,
      topologyStore,
      packageDependencies,
      loggerFactory,
    )
    if (preferCaching) {
      new CachingTopologySnapshot(snapshot, caching, batching, loggerFactory)
    } else
      snapshot
  }

}
