// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.block.data.{BlockEphemeralState, SequencerBlockStore}
import com.digitalasset.canton.domain.block.{BlockSequencerStateManager, UninitializedBlockHeight}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{
  DatabaseSequencerFactory,
  Sequencer,
  SequencerHealthConfig,
  SequencerInitialState,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficBalanceStore
import com.digitalasset.canton.domain.sequencing.traffic.{
  EnterpriseSequencerRateLimitManager,
  TrafficBalanceManager,
}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.EventCostCalculator
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

abstract class BlockSequencerFactory(
    health: Option[SequencerHealthConfig],
    storage: Storage,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    nodeParameters: CantonNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
    testingInterceptor: Option[TestingInterceptor],
    metrics: SequencerMetrics,
)(implicit ec: ExecutionContext)
    extends DatabaseSequencerFactory(storage, nodeParameters.processingTimeouts, protocolVersion)
    with NamedLogging {

  private val store = SequencerBlockStore(
    storage,
    protocolVersion,
    nodeParameters.processingTimeouts,
    // Block sequencer invariant checks will fail in unified sequencer mode
    checkedInvariant = Option.when(
      nodeParameters.enableAdditionalConsistencyChecks && !nodeParameters.useUnifiedSequencer
    )(sequencerId),
    loggerFactory,
  )

  private val balanceStore = TrafficBalanceStore(
    storage,
    nodeParameters.processingTimeouts,
    loggerFactory,
    nodeParameters.batchingConfig.aggregator,
  )

  protected val name: String

  protected val orderingTimeFixMode: OrderingTimeFixMode

  protected def createBlockSequencer(
      name: String,
      domainId: DomainId,
      cryptoApi: DomainSyncCryptoClient,
      stateManager: BlockSequencerStateManager,
      store: SequencerBlockStore,
      balanceStore: TrafficBalanceStore,
      storage: Storage,
      futureSupervisor: FutureSupervisor,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      driverClock: Clock,
      protocolVersion: ProtocolVersion,
      rateLimitManager: SequencerRateLimitManager,
      orderingTimeFixMode: OrderingTimeFixMode,
      initialBlockHeight: Option[Long],
      domainLoggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockSequencer

  override final def initialize(
      snapshot: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Unit] = {
    val initialBlockState = BlockEphemeralState.fromSequencerInitialState(snapshot)
    logger.debug(s"Storing sequencers initial state: $initialBlockState")
    for {
      _ <- {
        if (nodeParameters.useUnifiedSequencer) {
          super.initialize(snapshot, sequencerId)
        } else {
          EitherT.pure[Future, String](())
        }
      }
      _ <- EitherT.right(
        store.setInitialState(initialBlockState, snapshot.initialTopologyEffectiveTimestamp)
      )
      _ <- EitherT.right(snapshot.snapshot.trafficBalances.parTraverse_(balanceStore.store))
      _ = logger.debug(
        s"from snapshot: ticking traffic balance manager with ${snapshot.latestSequencerEventTimestamp}"
      )
      _ <- EitherT.right(
        snapshot.latestSequencerEventTimestamp
          .traverse(ts => balanceStore.setInitialTimestamp(ts))
      )
    } yield ()
  }

  @VisibleForTesting
  protected def makeRateLimitManager(
      trafficBalanceManager: TrafficBalanceManager,
      futureSupervisor: FutureSupervisor,
  ): SequencerRateLimitManager = {
    new EnterpriseSequencerRateLimitManager(
      trafficBalanceManager,
      loggerFactory,
      futureSupervisor,
      nodeParameters.processingTimeouts,
      metrics,
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )
  }

  override final def create(
      domainId: DomainId,
      sequencerId: SequencerId,
      clock: Clock,
      driverClock: Clock,
      domainSyncCryptoApi: DomainSyncCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      tracer: trace.Tracer,
      actorMaterializer: Materializer,
  ): Future[Sequencer] = {
    val initialBlockHeight = {
      val last = nodeParameters.processingTimeouts.unbounded
        .await(s"Reading the $name store head to get the initial block height")(
          store.readHead
        )
        .latestBlock
        .height
      if (last == UninitializedBlockHeight) {
        None
      } else {
        Some(last + 1)
      }
    }
    logger.info(
      s"Creating $name sequencer at block height $initialBlockHeight"
    )

    val balanceManager = new TrafficBalanceManager(
      balanceStore,
      clock,
      trafficConfig,
      futureSupervisor,
      metrics,
      nodeParameters.processingTimeouts,
      loggerFactory,
    )

    //  Start auto pruning of traffic balances
    FutureUtil.doNotAwaitUnlessShutdown(
      balanceManager.startAutoPruning,
      "Auto pruning of traffic balances",
    )

    val rateLimitManager = makeRateLimitManager(
      balanceManager,
      futureSupervisor,
    )

    val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)

    val stateManagerF = BlockSequencerStateManager(
      protocolVersion,
      domainId,
      sequencerId,
      store,
      nodeParameters.enableAdditionalConsistencyChecks,
      nodeParameters.processingTimeouts,
      domainLoggerFactory,
      rateLimitManager,
      nodeParameters.useUnifiedSequencer,
    )

    for {
      _ <- balanceManager.initialize
      stateManager <- stateManagerF
    } yield {
      val sequencer = createBlockSequencer(
        name,
        domainId,
        domainSyncCryptoApi,
        stateManager,
        store,
        balanceStore,
        storage,
        futureSupervisor,
        health,
        clock,
        driverClock,
        protocolVersion,
        rateLimitManager,
        orderingTimeFixMode,
        initialBlockHeight,
        domainLoggerFactory,
      )
      testingInterceptor
        .map(_(clock)(sequencer)(ec))
        .getOrElse(sequencer)
    }
  }

  override def onClosed(): Unit = {
    Lifecycle.close(store)(logger)
  }
}

object BlockSequencerFactory {

  /** Whether a sequencer implementation requires `BlockUpdateGenerator` to adjust ordering timestamps
    * to ensure they are strictly increasing or just validate that they are.
    */
  sealed trait OrderingTimeFixMode

  object OrderingTimeFixMode {

    final case object MakeStrictlyIncreasing extends OrderingTimeFixMode

    final case object ValidateOnly extends OrderingTimeFixMode
  }
}
