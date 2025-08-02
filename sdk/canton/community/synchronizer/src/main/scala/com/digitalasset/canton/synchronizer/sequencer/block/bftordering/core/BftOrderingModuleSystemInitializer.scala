// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.{
  BftOrderingStores,
  BootstrapTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.DelegationCryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  BlacklistLeaderSelectionPolicyState,
  LeaderSelectionInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.{
  EpochChecker,
  OutputModule,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.{
  P2PNetworkInModule,
  P2PNetworkOutModule,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.PruningModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.OrderingModuleSystemInitializer.ModuleFactories
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
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
  Env,
  ModuleSystem,
  OrderingModuleSystemInitializer,
  P2PConnectionEventListener,
  P2PNetworkRefFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveRequest
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.Random

/** A module system initializer for the concrete Canton BFT ordering system.
  */
private[bftordering] class BftOrderingModuleSystemInitializer[
    E <: Env[E],
    P2PNetworkRefFactoryT <: P2PNetworkRefFactory[E, BftOrderingServiceReceiveRequest],
](
    node: BftNodeId,
    config: BftBlockOrdererConfig,
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
    epochChecker: EpochChecker = EpochChecker.DefaultEpochChecker, // Only set by simulation tests
)(implicit synchronizerProtocolVersion: ProtocolVersion, mc: MetricsContext)
    extends SystemInitializer[
      E,
      P2PNetworkRefFactoryT,
      BftOrderingServiceReceiveRequest,
      Mempool.Message,
    ]
    with NamedLogging {

  override def initialize(
      moduleSystem: ModuleSystem[E],
      createP2PNetworkRefFactory: P2PConnectionEventListener => P2PNetworkRefFactoryT,
  ): SystemInitializationResult[
    E,
    P2PNetworkRefFactoryT,
    BftOrderingServiceReceiveRequest,
    Mempool.Message,
  ] = {
    implicit val c: BftBlockOrdererConfig = config
    val leaderSelectionPolicyFactory = LeaderSelectionInitializer.create[E](
      node,
      config,
      synchronizerProtocolVersion,
      stores.outputStore,
      timeouts,
      msg => implicit context => failBootstrap(msg),
      metrics,
      loggerFactory,
    )
    val (initialEpoch, bootstrapTopologyInfo, blacklistLeaderSelectionState) =
      fetchBootstrapTopologyInfo(moduleSystem, leaderSelectionPolicyFactory)

    val thisNodeFirstKnownAt =
      sequencerSnapshotAdditionalInfo.flatMap(_.nodeActiveAt.get(bootstrapTopologyInfo.thisNode))
    val firstBlockNumberInOnboardingEpoch =
      thisNodeFirstKnownAt.flatMap(_.firstBlockNumberInStartEpoch)
    val previousBftTimeForOnboarding = thisNodeFirstKnownAt.flatMap(_.previousBftTime)

    val initialLowerBound = thisNodeFirstKnownAt.flatMap { data =>
      for {
        epoch <- data.startEpochNumber
        blockNumber <- data.firstBlockNumberInStartEpoch
      } yield (epoch, blockNumber)
    }

    val onboardingEpochCouldAlterOrderingTopology =
      thisNodeFirstKnownAt
        .flatMap(_.startEpochCouldAlterOrderingTopology)
        .exists(pendingChanges => pendingChanges)
    val outputModuleStartupState =
      OutputModule.StartupState(
        bootstrapTopologyInfo.thisNode,
        initialHeightToProvide =
          firstBlockNumberInOnboardingEpoch.getOrElse(sequencerSubscriptionInitialBlockNumber),
        initialEpochWeHaveLeaderSelectionStateFor = initialEpoch,
        previousBftTimeForOnboarding,
        onboardingEpochCouldAlterOrderingTopology,
        bootstrapTopologyInfo.currentCryptoProvider,
        bootstrapTopologyInfo.currentTopology,
        initialLowerBound,
        leaderSelectionPolicyFactory.leaderSelectionPolicy(
          blacklistLeaderSelectionState,
          bootstrapTopologyInfo.currentTopology,
        ),
      )
    new OrderingModuleSystemInitializer[E, P2PNetworkRefFactoryT](
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
          new P2PNetworkInModule(
            metrics,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
          ),
        p2pNetworkOut = (
            p2pNetworkInRef,
            mempoolRef,
            availabilityRef,
            consensusRef,
            outputRef,
            pruningRef,
        ) => {
          val dependencies = P2PNetworkOutModuleDependencies(
            createP2PNetworkRefFactory,
            p2pNetworkInRef,
            mempoolRef,
            availabilityRef,
            consensusRef,
            outputRef,
            pruningRef,
          )
          val p2PNetworkOutModule = new P2PNetworkOutModule(
            bootstrapTopologyInfo.thisNode,
            stores.p2pEndpointsStore,
            metrics,
            dependencies,
            loggerFactory,
            timeouts,
          )
          (p2PNetworkOutModule, p2PNetworkOutModule.p2pNetworkRefFactory)
        },
        availability = (mempoolRef, networkOutRef, consensusRef, outputRef) => {
          val dependencies = AvailabilityModuleDependencies[E](
            mempoolRef,
            networkOutRef,
            consensusRef,
            outputRef,
          )
          new AvailabilityModule[E](
            bootstrapTopologyInfo.currentMembership,
            initialEpoch,
            bootstrapTopologyInfo.currentCryptoProvider,
            stores.availabilityStore,
            clock,
            random,
            metrics,
            dependencies,
            loggerFactory,
            timeouts,
          )()
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
            loggerFactory,
            timeouts,
            metrics,
          )

          new PreIssConsensusModule(
            bootstrapTopologyInfo,
            epochLength,
            stores.epochStore,
            sequencerSnapshotAdditionalInfo,
            clock,
            metrics,
            segmentModuleRefFactory,
            random,
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
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
            requestInspector,
            epochChecker,
          ),
        pruning = () =>
          new PruningModule(
            stores,
            clock,
            loggerFactory,
            timeouts,
          ),
      )
    ).initialize(moduleSystem, createP2PNetworkRefFactory)
  }

  private def fetchBootstrapTopologyInfo(
      moduleSystem: ModuleSystem[E],
      leaderSelectionPolicyFactory: LeaderSelectionInitializer[E],
  ): (EpochNumber, OrderingTopologyInfo[E], BlacklistLeaderSelectionPolicyState) = {
    import TraceContext.Implicits.Empty.*

    val BootstrapTopologyInfo(
      initialEpochNumber,
      initialTopologyQueryTimestamp,
      previousTopologyQueryTimestamp,
      maybeOnboardingTopologyQueryTimestamp,
    ) =
      getInitialAndPreviousTopologyQueryTimestamps(moduleSystem)

    val (initialTopology, initialCryptoProvider) =
      getOrderingTopologyAt(moduleSystem, initialTopologyQueryTimestamp, "initial")

    val leaderSelectionPolicyState = leaderSelectionPolicyFactory.stateForInitial(
      moduleSystem,
      sequencerSnapshotAdditionalInfo,
      initialEpochNumber,
    )
    val initialLeaders =
      leaderSelectionPolicyFactory.leaderFromState(
        leaderSelectionPolicyState,
        initialTopology,
      )

    val (previousTopology, previousCryptoProvider) =
      getOrderingTopologyAt(moduleSystem, previousTopologyQueryTimestamp, "previous")

    // the previousLeaders is not really used unless we are doing onboarding, in that case we should use previousTopology
    // to compute the "currentLeaders"
    val previousLeaders = leaderSelectionPolicyFactory.leaderFromState(
      leaderSelectionPolicyState,
      previousTopology,
    )

    val maybeOnboardingTopologyAndCryptoProvider = maybeOnboardingTopologyQueryTimestamp
      .map(onboardingTopologyQueryTimestamp =>
        getOrderingTopologyAt(moduleSystem, onboardingTopologyQueryTimestamp, "onboarding")
      )

    (
      initialEpochNumber,
      OrderingTopologyInfo(
        node,
        // Use the previous topology (not containing this node) as current topology when onboarding.
        //  This prevents relying on newly onboarded nodes for state transfer.
        currentTopology = initialTopology,
        currentCryptoProvider =
          maybeOnboardingTopologyAndCryptoProvider.fold(initialCryptoProvider) {
            case (_, onboardingCryptoProvider) =>
              DelegationCryptoProvider(
                // Note that, when onboarding, the signing crypto provider corresponds to the onboarding node activation
                //  timestamp (so that its signing key is present), the verification will use the one at the start of epoch.
                signer = onboardingCryptoProvider,
                verifier = initialCryptoProvider,
              )
          },
        currentLeaders = initialLeaders,
        previousTopology, // for canonical commit set verification
        previousCryptoProvider, // for canonical commit set verification
        previousLeaders,
      ),
      leaderSelectionPolicyState,
    )
  }

  private def getInitialAndPreviousTopologyQueryTimestamps(
      moduleSystem: ModuleSystem[E]
  )(implicit traceContext: TraceContext) =
    sequencerSnapshotAdditionalInfo match {
      case Some(additionalInfo) =>
        // We assume that, if a sequencer snapshot has been provided, then we're onboarding; in that case, we use
        //  topology information from the sequencer snapshot, else we fetch the latest topology from the DB.
        val thisNodeActiveAt = additionalInfo.nodeActiveAt.getOrElse(
          node,
          failBootstrap("Activation information is required when onboarding but it's empty"),
        )
        val epochNumber = thisNodeActiveAt.startEpochNumber.getOrElse(
          failBootstrap("Start epoch information is required when onboarding but it's empty")
        )
        val initialTopologyQueryTimestamp =
          thisNodeActiveAt.startEpochTopologyQueryTimestamp.getOrElse(
            failBootstrap(
              "Start epoch topology query timestamp is required when onboarding but it's empty"
            )
          )
        val previousTopologyQueryTimestamp =
          thisNodeActiveAt.previousEpochTopologyQueryTimestamp.getOrElse {
            // If the start epoch is immediately after the genesis epoch
            initialTopologyQueryTimestamp
          }
        val onboardingTopologyQueryTimestamp = thisNodeActiveAt.timestamp
        BootstrapTopologyInfo(
          epochNumber,
          initialTopologyQueryTimestamp,
          previousTopologyQueryTimestamp,
          Some(onboardingTopologyQueryTimestamp),
        )

      case _ =>
        // Regular (i.e., non-onboarding) start
        val initialTopologyEpochInfo = {
          val latestEpoch = fetchLatestEpoch(moduleSystem, includeInProgress = true)
          latestEpoch.info
        }
        val initialTopologyQueryTimestamp = initialTopologyEpochInfo.topologyActivationTime
        // TODO(#24262) if restarted just after completing an epoch, the previous and initial topology might be the same
        val previousTopologyQueryTimestamp = {
          val latestCompletedEpoch = fetchLatestEpoch(moduleSystem, includeInProgress = false)
          latestCompletedEpoch.info.topologyActivationTime
        }
        BootstrapTopologyInfo(
          initialTopologyEpochInfo.number,
          initialTopologyQueryTimestamp,
          previousTopologyQueryTimestamp,
        )
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
      pruningSchedulerStore: BftOrdererPruningSchedulerStore[E],
  )

  /** In case of onboarding, the topology query timestamps look as follows:
    * {{{
    * ───|────────────|─────────────────────|──────────────────────────|──────────> time
    *   Previous     Initial topology ts   Onboarding topology ts     (Topology ts, where
    *   topology ts  (start epoch)         (node active in topology)  node is active in consensus)
    * }}}
    *
    * @param initialEpochNumber
    *   A start epoch number.
    * @param initialTopologyQueryTimestamp
    *   A timestamp to get an initial topology (and a crypto provider) for signing and validation.
    * @param previousTopologyQueryTimestamp
    *   A timestamp to get a topology (and a crypto provider) for canonical commit set validation at
    *   the first epoch boundary.
    * @param onboardingTopologyQueryTimestamp
    *   An optional timestamp to get a topology (and a crypto provider) for signing state transfer
    *   requests for onboarding.
    */
  final case class BootstrapTopologyInfo(
      initialEpochNumber: EpochNumber,
      initialTopologyQueryTimestamp: TopologyActivationTime,
      previousTopologyQueryTimestamp: TopologyActivationTime,
      onboardingTopologyQueryTimestamp: Option[TopologyActivationTime] = None,
  )
}
