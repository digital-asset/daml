// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerSyncCryptoClient
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator
import com.digitalasset.canton.synchronizer.block.data.SequencerBlockStore
import com.digitalasset.canton.synchronizer.block.{
  BlockSequencerStateManager,
  UninitializedBlockHeight,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.synchronizer.sequencing.sequencer.traffic.{
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.{
  TrafficConsumedStore,
  TrafficPurchasedStore,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.{
  EnterpriseSequencerRateLimitManager,
  TrafficPurchasedManager,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

import BlockSequencerFactory.OrderingTimeFixMode

abstract class BlockSequencerFactory(
    health: Option[SequencerHealthConfig],
    blockSequencerConfig: BlockSequencerConfig,
    storage: Storage,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    nodeParameters: CantonNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
    testingInterceptor: Option[TestingInterceptor],
    metrics: SequencerMetrics,
)(implicit ec: ExecutionContext)
    extends DatabaseSequencerFactory(
      blockSequencerConfig.toDatabaseSequencerConfig,
      storage,
      nodeParameters.cachingConfigs,
      nodeParameters.processingTimeouts,
      protocolVersion,
      sequencerId,
      blockSequencerMode = true,
    )
    with NamedLogging {

  private val store = SequencerBlockStore(
    storage,
    protocolVersion,
    sequencerStore,
    nodeParameters.processingTimeouts,
    loggerFactory,
  )

  private val trafficPurchasedStore = TrafficPurchasedStore(
    storage,
    nodeParameters.processingTimeouts,
    loggerFactory,
    nodeParameters.batchingConfig.aggregator,
  )

  private val trafficConsumedStore = TrafficConsumedStore(
    storage,
    nodeParameters.processingTimeouts,
    loggerFactory,
  )

  protected val name: String

  protected val orderingTimeFixMode: OrderingTimeFixMode

  protected def createBlockSequencer(
      name: String,
      synchronizerId: SynchronizerId,
      cryptoApi: SynchronizerSyncCryptoClient,
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
      executionContext: ExecutionContext,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockSequencer

  override final def initialize(
      snapshot: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(s"Storing sequencers initial state: $snapshot")
    for {
      _ <- super[DatabaseSequencerFactory].initialize(
        snapshot,
        sequencerId,
      ) // Members are stored in the DBS
      _ <- EitherT
        .right(
          store.setInitialState(snapshot, snapshot.initialTopologyEffectiveTimestamp)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- EitherT
        .right(
          snapshot.snapshot.trafficPurchased.parTraverse_(trafficPurchasedStore.store)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- EitherT
        .right(trafficConsumedStore.store(snapshot.snapshot.trafficConsumed))
        .mapK(FutureUnlessShutdown.outcomeK)
      _ = logger.debug(
        s"from snapshot: ticking traffic purchased entry manager with ${snapshot.latestSequencerEventTimestamp}"
      )
      _ <- EitherT
        .right(
          snapshot.latestSequencerEventTimestamp
            .traverse(ts => trafficPurchasedStore.setInitialTimestamp(ts))
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield ()
  }

  @VisibleForTesting
  protected def makeRateLimitManager(
      trafficPurchasedManager: TrafficPurchasedManager,
      domainSyncCryptoApi: SynchronizerSyncCryptoClient,
      protocolVersion: ProtocolVersion,
      trafficConfig: SequencerTrafficConfig,
  ): SequencerRateLimitManager =
    new EnterpriseSequencerRateLimitManager(
      trafficPurchasedManager,
      trafficConsumedStore,
      loggerFactory,
      nodeParameters.processingTimeouts,
      metrics,
      domainSyncCryptoApi,
      protocolVersion,
      trafficConfig,
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )

  override final def create(
      synchronizerId: SynchronizerId,
      sequencerId: SequencerId,
      clock: Clock,
      driverClock: Clock,
      domainSyncCryptoApi: SynchronizerSyncCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
      runtimeReady: FutureUnlessShutdown[Unit],
      sequencerSnapshot: Option[SequencerSnapshot] = None,
  )(implicit
      traceContext: TraceContext,
      tracer: trace.Tracer,
      actorMaterializer: Materializer,
  ): FutureUnlessShutdown[Sequencer] = {
    val initialBlockHeight = {
      val last = nodeParameters.processingTimeouts.unbounded
        .awaitUS(s"Reading the $name store head to get the initial block height")(
          store.readHead
        )
        .map(
          _.latestBlock.height
        )
      last.map {
        case result if result == UninitializedBlockHeight => None
        case result => Some(result + 1)
      }
    }

    initialBlockHeight match {
      case UnlessShutdown.Outcome(result) =>
        logger.info(
          s"Creating $name sequencer at block height $result"
        )
      case UnlessShutdown.AbortedDueToShutdown =>
        logger.info(
          s"$name sequencer creation stopped because of shutdown"
        )
    }

    val balanceManager = new TrafficPurchasedManager(
      trafficPurchasedStore,
      trafficConfig,
      futureSupervisor,
      metrics,
      nodeParameters.processingTimeouts,
      loggerFactory,
    )

    val rateLimitManager = makeRateLimitManager(
      balanceManager,
      domainSyncCryptoApi,
      protocolVersion,
      trafficConfig,
    )

    val domainLoggerFactory = loggerFactory.append("synchronizerId", synchronizerId.toString)

    for {
      initialBlockHeight <- FutureUnlessShutdown(Future.successful(initialBlockHeight))
      _ <- FutureUnlessShutdown.outcomeF(balanceManager.initialize)
      stateManager <- FutureUnlessShutdown.lift(
        BlockSequencerStateManager.create(
          synchronizerId,
          store,
          trafficConsumedStore,
          nodeParameters.enableAdditionalConsistencyChecks,
          nodeParameters.processingTimeouts,
          domainLoggerFactory,
        )
      )
    } yield {
      val sequencer = createBlockSequencer(
        name,
        synchronizerId,
        domainSyncCryptoApi,
        stateManager,
        store,
        trafficPurchasedStore,
        storage,
        futureSupervisor,
        health,
        clock,
        driverClock,
        protocolVersion,
        rateLimitManager,
        orderingTimeFixMode,
        initialBlockHeight,
        sequencerSnapshot,
        domainLoggerFactory,
        runtimeReady,
      )
      testingInterceptor
        .map(_(clock)(sequencer)(ec))
        .getOrElse(sequencer)
    }
  }

  override def onClosed(): Unit =
    LifeCycle.close(store)(logger)
}

object BlockSequencerFactory {

  /** Whether a sequencer implementation requires `BlockUpdateGenerator` to adjust ordering timestamps
    * to ensure they are strictly increasing or just validate that they are.
    */
  sealed trait OrderingTimeFixMode

  object OrderingTimeFixMode {

    /** Ordering timestamps are not necessarily unique or increasing.
      * Clients should adjust timestamps to enforce that.
      */
    final case object MakeStrictlyIncreasing extends OrderingTimeFixMode

    /** Ordering timestamps are strictly monotonically increasing. */
    final case object ValidateOnly extends OrderingTimeFixMode
  }
}
