// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{CommunityStorageSetup, Storage, StorageSetup}
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager
import com.digitalasset.canton.synchronizer.block.data.SequencerBlockStore
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.block.{BlockSequencer, BlockSequencerFactory}
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.synchronizer.sequencer.{
  AuthenticationServices,
  BlockSequencerConfig,
  SequencerHealthConfig,
  SequencerSnapshot,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficPurchasedStore
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
    storageSetup: StorageSetup = CommunityStorageSetup,
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
      cryptoApi: SynchronizerCryptoClient,
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
      authenticationServices: Option[AuthenticationServices],
      synchronizerLoggerFactory: NamedLoggerFactory,
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
        synchronizerId,
        sequencerId,
        protocolVersion,
        driverClock,
        new CantonOrderingTopologyProvider(cryptoApi, loggerFactory),
        authenticationServices,
        nodeParameters,
        initialHeight,
        orderingTimeFixMode,
        sequencerSnapshot.flatMap(_.additional),
        metrics.bftOrdering,
        synchronizerLoggerFactory,
        storageSetup,
        nodeParameters.loggingConfig.queryCost,
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
      synchronizerLoggerFactory,
      exitOnFatalFailures = nodeParameters.exitOnFatalFailures,
      runtimeReady = runtimeReady,
    )
  }
}

object BftSequencerFactory extends LazyLogging {

  val ShortName: String = "BFT"
}
