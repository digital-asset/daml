// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.{
  AvailabilityModule,
  AvailabilityModuleConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  EpochStoreReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  PreIssConsensusModule,
  SegmentModuleRefFactoryImpl,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool.{
  MempoolModule,
  MempoolModuleConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.{
  BftP2PNetworkIn,
  BftP2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.OrderingModuleSystemInitializer.ModuleFactories
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.{
  AvailabilityModuleDependencies,
  ConsensusModuleDependencies,
  P2PNetworkOutModuleDependencies,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  ClientP2PNetworkManager,
  Env,
  ModuleSystem,
  OrderingModuleSystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.Random

/** A module system initializer for the concrete Canton BFT ordering system.
  */
private[bftordering] class BftOrderingModuleSystemInitializer[E <: Env[E]](
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    config: BftBlockOrderer.Config,
    sequencerSubscriptionInitialBlockNumber: BlockNumber,
    epochLength: EpochLength,
    stores: BftOrderingStores[E],
    orderingTopologyProvider: OrderingTopologyProvider[E],
    blockSubscription: BlockSubscription,
    sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
    clock: Clock,
    random: Random,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
    requestInspector: RequestInspector =
      OutputModule.DefaultRequestInspector, // Only set by simulation tests
)(implicit
    mc: MetricsContext
) extends SystemInitializer[E, BftOrderingServiceReceiveRequest, Mempool.Message]
    with NamedLogging {

  override def initialize(
      moduleSystem: ModuleSystem[E],
      networkManager: ClientP2PNetworkManager[E, BftOrderingServiceReceiveRequest],
  ): SystemInitializationResult[BftOrderingServiceReceiveRequest, Mempool.Message] = {
    implicit val c: BftBlockOrderer.Config = config
    val bootstrapTopologyInfo = fetchBootstrapTopologyInfo(moduleSystem)

    val thisPeerFirstKnownAt =
      sequencerSnapshotAdditionalInfo.flatMap(_.peerActiveAt.get(bootstrapTopologyInfo.thisPeer))
    val firstBlockNumberInOnboardingEpoch = thisPeerFirstKnownAt.flatMap(_.firstBlockNumberInEpoch)
    val previousBftTimeForOnboarding = thisPeerFirstKnownAt.flatMap(_.previousBftTime)
    val onboardingEpochCouldAlterOrderingTopology =
      thisPeerFirstKnownAt
        .flatMap(_.epochCouldAlterOrderingTopology)
        .exists(pendingChanges => pendingChanges)
    val outputModuleStartupState =
      OutputModule.StartupState(
        initialHeightToProvide =
          firstBlockNumberInOnboardingEpoch.getOrElse(sequencerSubscriptionInitialBlockNumber),
        previousBftTimeForOnboarding,
        onboardingEpochCouldAlterOrderingTopology,
        bootstrapTopologyInfo.currentCryptoProvider,
        bootstrapTopologyInfo.currentTopology,
      )
    new OrderingModuleSystemInitializer[E](
      ModuleFactories(
        mempool = { availabilityRef =>
          val cfg = MempoolModuleConfig(
            config.maxMempoolQueueSize,
            config.maxRequestPayloadBytes,
            config.maxRequestsInBatch,
            config.minRequestsInBatch,
            config.maxBatchCreationInterval,
          )
          new MempoolModule(
            cfg,
            metrics,
            availabilityRef,
            loggerFactory,
            timeouts,
          )
        },
        p2pNetworkIn = (availabilityRef, consensusRef) =>
          new BftP2PNetworkIn(
            metrics,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
          ),
        p2pNetworkOut =
          (networkManager, p2pNetworkInRef, mempoolRef, availabilityRef, consensusRef, outputRef) =>
            {
              val dependencies = P2PNetworkOutModuleDependencies(
                networkManager,
                p2pNetworkInRef,
                mempoolRef,
                availabilityRef,
                consensusRef,
                outputRef,
              )
              new BftP2PNetworkOut(
                bootstrapTopologyInfo.thisPeer,
                stores.p2pEndpointsStore,
                metrics,
                dependencies,
                loggerFactory,
                timeouts,
              )
            },
        availability = (mempoolRef, networkOutRef, consensusRef, outputRef) => {
          val cfg = AvailabilityModuleConfig(
            config.maxRequestsInBatch,
            config.maxBatchesPerBlockProposal,
            config.outputFetchTimeout,
          )
          val dependencies = AvailabilityModuleDependencies[E](
            mempoolRef,
            networkOutRef,
            consensusRef,
            outputRef,
          )
          new AvailabilityModule[E](
            bootstrapTopologyInfo.currentMembership,
            bootstrapTopologyInfo.currentCryptoProvider,
            stores.availabilityStore,
            cfg,
            clock,
            random,
            metrics,
            dependencies,
            loggerFactory,
            timeouts,
          )
        },
        consensus = (p2pNetworkOutRef, availabilityRef, outputRef) => {
          val dependencies = ConsensusModuleDependencies(
            availabilityRef,
            outputRef,
            p2pNetworkOutRef,
          )

          val segmentModuleRefFactory = new SegmentModuleRefFactoryImpl(
            storePbftMessages = true,
            stores.epochStore,
            dependencies,
            clock,
            loggerFactory,
            timeouts,
          )

          new PreIssConsensusModule(
            bootstrapTopologyInfo,
            epochLength,
            stores.epochStore,
            sequencerSnapshotAdditionalInfo,
            clock,
            metrics,
            segmentModuleRefFactory,
            dependencies,
            loggerFactory,
            timeouts,
          )
        },
        output = (availabilityRef, consensusRef) =>
          new OutputModule(
            outputModuleStartupState,
            orderingTopologyProvider,
            stores.outputStore,
            stores.epochStoreReader,
            blockSubscription,
            metrics,
            protocolVersion,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
            requestInspector,
          ),
      )
    ).initialize(moduleSystem, networkManager)
  }

  private def fetchBootstrapTopologyInfo(moduleSystem: ModuleSystem[E]): OrderingTopologyInfo[E] = {
    import TraceContext.Implicits.Empty.*

    val (initialTopologyQueryTimestamp, previousTopologyQueryTimestamp, onboarding) =
      getInitialAndPreviousTopologyQueryTimestamps(moduleSystem)

    val (initialTopology, initialCryptoProvider) =
      getOrderingTopologyAt(moduleSystem, initialTopologyQueryTimestamp, "initial")

    val (previousTopology, previousCryptoProvider) =
      getOrderingTopologyAt(moduleSystem, previousTopologyQueryTimestamp, "previous")

    OrderingTopologyInfo(
      sequencerId,
      // Use the previous topology (not containing this peer) as current topology when onboarding.
      //  This prevents relying on newly onboarded peers for state transfer.
      currentTopology = if (onboarding) previousTopology else initialTopology,
      initialCryptoProvider,
      previousTopology,
      previousCryptoProvider,
    )
  }

  private def getInitialAndPreviousTopologyQueryTimestamps(
      moduleSystem: ModuleSystem[E]
  )(implicit traceContext: TraceContext) =
    sequencerSnapshotAdditionalInfo match {
      case Some(additionalInfo) =>
        // We assume that, if a sequencer snapshot has been provided, then we're onboarding; in that case, we use
        //  topology information from the sequencer snapshot, else we fetch the latest topology from the DB.
        val thisPeerActiveAt = additionalInfo.peerActiveAt.getOrElse(
          sequencerId,
          failBootstrap("Peer activation information is required when onboarding but it's empty"),
        )
        val initialTopologyQueryTimestamp = thisPeerActiveAt.timestamp
        val previousTopologyQueryTimestamp =
          thisPeerActiveAt.epochTopologyQueryTimestamp.getOrElse(
            failBootstrap(
              "Start epoch topology query timestamp is required when onboarding but it's empty"
            )
          )
        (initialTopologyQueryTimestamp, previousTopologyQueryTimestamp, true)

      case _ =>
        // Regular (i.e., non-onboarding) start
        val initialTopologyQueryTimestamp = {
          val latestEpoch = fetchLatestEpoch(moduleSystem, includeInProgress = true)
          latestEpoch.info.topologyActivationTime
        }
        // TODO(#24262) if restarted just after completing an epoch, the previous and initial topology might be the same
        val previousTopologyQueryTimestamp = {
          val latestCompletedEpoch = fetchLatestEpoch(moduleSystem, includeInProgress = false)
          latestCompletedEpoch.info.topologyActivationTime
        }
        (initialTopologyQueryTimestamp, previousTopologyQueryTimestamp, false)
    }

  private def getOrderingTopologyAt(
      moduleSystem: ModuleSystem[E],
      topologyQueryTimestamp: TopologyActivationTime,
      topologyDesignation: String,
  )(implicit traceContext: TraceContext) =
    awaitFuture(
      moduleSystem,
      orderingTopologyProvider.getOrderingTopologyAt(topologyQueryTimestamp),
      s"fetch $topologyDesignation ordering topology for bootstrap",
    ).getOrElse(failBootstrap(s"Failed to fetch $topologyDesignation ordering topology"))

  private def fetchLatestEpoch(moduleSystem: ModuleSystem[E], includeInProgress: Boolean)(implicit
      traceContext: TraceContext
  ) =
    awaitFuture(
      moduleSystem,
      stores.epochStore.latestEpoch(includeInProgress),
      s"fetch latest${if (includeInProgress) " in-progress " else " "}epoch",
    )

  private def failBootstrap(msg: String)(implicit traceContext: TraceContext) = {
    logger.error(msg)
    sys.error(msg)
  }

  private def awaitFuture[X](
      moduleSystem: ModuleSystem[E],
      future: E#FutureUnlessShutdownT[X],
      description: String,
  )(implicit traceContext: TraceContext): X = {
    logger.debug(description)
    moduleSystem.rootActorContext.blockingAwait(
      future,
      timeouts.default.asFiniteApproximation,
    )
  }
}

object BftOrderingModuleSystemInitializer {

  final case class BftOrderingStores[E <: Env[E]](
      p2pEndpointsStore: P2PEndpointsStore[E],
      availabilityStore: AvailabilityStore[E],
      epochStore: EpochStore[E],
      epochStoreReader: EpochStoreReader[E],
      outputStore: OutputMetadataStore[E],
  )
}
