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
  TopologyConfig,
}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessingTicker
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.participant.traffic.TrafficStateController
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, TopologyTransactionProcessor}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class TopologyComponentFactory(
    domainId: DomainId,
    crypto: Crypto,
    clock: Clock,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    topologyConfig: TopologyConfig,
    topologyStore: TopologyStore[DomainStore],
    loggerFactory: NamedLoggerFactory,
) {

  def createTopologyProcessorFactory(
      partyNotifier: LedgerServerPartyNotifier,
      missingKeysAlerter: MissingKeysAlerter,
      topologyClient: DomainTopologyClientWithInit,
      trafficStateController: TrafficStateController,
      recordOrderPublisher: RecordOrderPublisher,
  ): TopologyTransactionProcessor.Factory = new TopologyTransactionProcessor.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit executionContext: ExecutionContext): TopologyTransactionProcessor = {

      val terminateTopologyProcessing = new ParticipantTopologyTerminateProcessingTicker(
        recordOrderPublisher,
        loggerFactory,
      )

      val processor = new TopologyTransactionProcessor(
        domainId,
        crypto.pureCrypto,
        topologyStore,
        acsCommitmentScheduleEffectiveTime,
        terminateTopologyProcessing,
        futureSupervisor,
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

  def createTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit executionContext: ExecutionContext): DomainTopologyClientWithInit =
    new StoreBasedDomainTopologyClient(
      clock,
      domainId,
      protocolVersion,
      topologyStore,
      packageDependencies,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  def createCachingTopologyClient(
      protocolVersion: ProtocolVersion,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DomainTopologyClientWithInit] = CachingDomainTopologyClient.create(
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

  def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      preferCaching: Boolean,
  )(implicit executionContext: ExecutionContext): TopologySnapshot = {
    val snapshot = new StoreBasedTopologySnapshot(
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

  def createHeadTopologySnapshot()(implicit
      executionContext: ExecutionContext
  ): TopologySnapshot =
    createTopologySnapshot(
      CantonTimestamp.MaxValue,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      preferCaching = false,
    )
}
