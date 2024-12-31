// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.driver

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.block.BlockSequencerStateManager
import com.digitalasset.canton.domain.block.data.SequencerBlockStore
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.block.{
  BlockSequencer,
  BlockSequencerFactory,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.sequencer.{
  BlockSequencerConfig,
  SequencerHealthConfig,
  SequencerSnapshot,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

class BftSequencerFactory(
    config: BftBlockOrderer.Config,
    blockSequencerConfig: BlockSequencerConfig,
    health: Option[SequencerHealthConfig],
    storage: Storage,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    nodeParameters: CantonNodeParameters,
    metrics: SequencerMetrics,
    override val loggerFactory: NamedLoggerFactory,
    testingInterceptor: Option[TestingInterceptor],
)(implicit ec: ExecutionContext)
    extends BlockSequencerFactory(
      health,
      blockSequencerConfig,
      storage,
      protocolVersion,
      sequencerId,
      nodeParameters,
      loggerFactory,
      testingInterceptor,
      metrics,
    ) {

  import BftSequencerFactory.*

  override protected final lazy val name: String = ShortName

  override protected final lazy val orderingTimeFixMode: OrderingTimeFixMode =
    OrderingTimeFixMode.ValidateOnly

  override protected final def createBlockSequencer(
      name: String,
      synchronizerId: SynchronizerId,
      cryptoApi: DomainSyncCryptoClient,
      stateManager: BlockSequencerStateManager,
      store: SequencerBlockStore,
      balanceStore: TrafficPurchasedStore,
      storage: Storage,
      futureSupervisor: FutureSupervisor,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      driverClock: Clock,
      protocolVersion: ProtocolVersion,
      rateLimitManager: SequencerRateLimitManager,
      orderingTimeFixMode: OrderingTimeFixMode,
      initialBlockHeight: Option[Long],
      sequencerSnapshot: Option[SequencerSnapshot],
      domainLoggerFactory: NamedLoggerFactory,
      runtimeReady: FutureUnlessShutdown[Unit],
  )(implicit
      ec: ExecutionContext,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockSequencer = {
    val initialHeight = initialBlockHeight.getOrElse(0L)
    new BlockSequencer(
      new BftBlockOrderer(
        config,
        storage,
        sequencerId,
        protocolVersion,
        driverClock,
        new CantonOrderingTopologyProvider(cryptoApi, loggerFactory),
        nodeParameters,
        initialHeight,
        orderingTimeFixMode,
        sequencerSnapshot.flatMap(_.additional),
        metrics.bftOrdering,
        domainLoggerFactory,
      ),
      name,
      synchronizerId,
      cryptoApi,
      sequencerId,
      stateManager,
      store,
      sequencerStore,
      blockSequencerConfig,
      balanceStore,
      storage,
      futureSupervisor,
      health,
      clock,
      protocolVersion,
      rateLimitManager,
      orderingTimeFixMode,
      nodeParameters.processingTimeouts,
      nodeParameters.loggingConfig.eventDetails,
      nodeParameters.loggingConfig.api.printer,
      metrics,
      domainLoggerFactory,
      exitOnFatalFailures = nodeParameters.exitOnFatalFailures,
      runtimeReady = runtimeReady,
    )
  }
}

object BftSequencerFactory extends LazyLogging {

  val ShortName: String = "BFT"
}
